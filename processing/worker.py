"""
processing/worker.py
--------------------
DB worker for the Coriolis Data Processing & NLP Layer (Step 2).

Workflow
--------
1. Query ``social_activity`` rows where ``status = 'pending'``.
2. For each row, call ``process_post()`` from nlp_processor.
3. Insert a ``ProcessedActivity`` row with the enriched data.
4. Mark the source row as ``status = 'processed'``.
5. On error: mark ``status = 'error'`` and log; never crash the whole batch.

Usage
-----
Run as a one-shot script (schedule via cron or systemd timer):

    # From the project root:
    python -m processing.worker

    # Optional: limit to N rows per run
    python -m processing.worker --limit 100

Environment
-----------
Reads ``DATABASE_URL`` from ``.env`` (via python-dotenv).
"""

from __future__ import annotations

import argparse
import logging
import time
import uuid
from datetime import datetime, timezone

from dotenv import load_dotenv

load_dotenv()

from database.models import (  # noqa: E402
    ProcessedActivity,
    SessionLocal,
    SocialActivity,
    create_tables,
)
from processing.nlp_processor import load_models, process_post  # noqa: E402

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
logger = logging.getLogger("coriolis.worker")

# ---------------------------------------------------------------------------
# Storage Limit: 1,000-post rolling FIFO buffer
# ---------------------------------------------------------------------------
STORAGE_LIMIT = 1_000

def prune_processed_activity(db) -> None:
    """Enforce a hard cap of STORAGE_LIMIT rows in processed_activity."""
    from sqlalchemy import text
    total: int = db.query(ProcessedActivity).count()
    if total <= STORAGE_LIMIT:
        return

    try:
        db.execute(
            text(
                """
                DELETE FROM processed_activity
                WHERE id NOT IN (
                    SELECT id FROM processed_activity
                    ORDER BY processed_at DESC
                    LIMIT :limit
                )
                """
            ),
            {"limit": STORAGE_LIMIT},
        )

        pruned = total - STORAGE_LIMIT
        print(
            f"[MAINTENANCE] Storage limit reached. "
            f"Pruned {pruned} old posts to maintain {STORAGE_LIMIT:,} post buffer."
        )
        logger.info(
            "[MAINTENANCE] Pruned %d old posts from processed_activity (limit=%d).",
            pruned,
            STORAGE_LIMIT,
        )
    except Exception as exc:
        logger.error(f"Failed to execute storage limit prune: {exc}")


# ---------------------------------------------------------------------------
# Core worker logic
# ---------------------------------------------------------------------------
def run_worker(limit: int = 500, batch_size: int = 50, poll_interval: int = 10) -> None:
    """Process pending social_activity rows and write to processed_activity in an infinite loop.

    Parameters
    ----------
    limit:
        Maximum number of rows to process in a single un-interrupted batch sequence.
    batch_size:
        Number of rows fetched per DB query.
    poll_interval:
        Seconds to sleep when no pending rows are found.
    """
    stats = {"processed": 0, "skipped": 0, "errors": 0}

    logger.info("Worker starting — loading NLP models…")
    load_models()
    logger.info("Models loaded. Checking DB tables…")
    create_tables()

    # Initial Prune: clear any backlog from previous runs
    try:
        db_init = SessionLocal()
        prune_processed_activity(db_init)
        db_init.commit()
        db_init.close()
    except Exception as exc:
        logger.warning(f"Initial storage limit prune failed: {exc}")

    logger.info("Starting infinite polling loop (interval=%ds).", poll_interval)
    
    while True:
        logger.info("Checking for new posts...")
        db = SessionLocal()
        try:
            total_fetched = 0

            while total_fetched < limit:
                fetch_n = min(batch_size, limit - total_fetched)

                pending: list[SocialActivity] = (
                    db.query(SocialActivity)
                    .filter(SocialActivity.status == "pending")
                    .order_by(SocialActivity.timestamp.asc())
                    .limit(fetch_n)
                    .with_for_update(skip_locked=True)
                    .all()
                )

                if not pending:
                    # No more rows; break inner batching loop
                    break

                # Aggressive logging as requested
                print(f"DEBUG: Worker found {len(pending)} raw items. Attempting to process...")
                logger.info("DEBUG: Found %d raw rows in the database", len(pending))
                total_fetched += len(pending)
                logger.info(
                    "Processing batch of %d rows (total fetched so far: %d)…",
                    len(pending),
                    total_fetched,
                )

                batch_processed = 0
                for row in pending:
                    try:
                        # Geocoding Retry Logic: Catch GeocoderQueryError
                        from geopy.exc import GeocoderQueryError
                        
                        try:
                            result = process_post(
                                {
                                    "text": row.text or "",
                                    "raw_location": row.raw_location,
                                    "source": row.source,
                                    "timestamp": (
                                        row.timestamp.isoformat() if row.timestamp else None
                                    ),
                                }
                            )
                        except GeocoderQueryError as gqe:
                            logger.warning("Geocoding Query Error: %s. Waiting 2s extra.", gqe)
                            time.sleep(2)
                            # Return minimal result to allow fallback
                            result = {
                                "latitude": 0.0, 
                                "longitude": 0.0, 
                                "extracted_locations": [], 
                                "geocoded_location": "Rate-Limited Fallback"
                            }

                        # Rate Limit: 1.1s delay after every geocoding attempt (inside process_post)
                        time.sleep(1.1)

                        # Fallback / Preserve Coordinates: use row.latitude if geocoding didn't find anything
                        final_lat = result.get("latitude")
                        final_lon = result.get("longitude")

                        # Priority for GDELT: Use pre-existing database coordinates if available
                        if row.source.lower() == "gdelt" and row.latitude is not None and row.longitude is not None:
                            final_lat = row.latitude
                            final_lon = row.longitude
                        elif (final_lat is None or final_lat == 0.0) and row.latitude is not None:
                            final_lat = row.latitude
                            final_lon = row.longitude
                        
                        if final_lat is None:
                            final_lat = 0.0
                            final_lon = 0.0

                        if not result.get("geocoded_location") or result.get("geocoded_location") == "Unknown (Fallback)":
                            if row.source.lower() == "gdelt" and row.raw_location:
                                result["geocoded_location"] = row.raw_location
                            elif row.raw_location:
                                result["geocoded_location"] = row.raw_location
                            else:
                                result["geocoded_location"] = "Unknown (Fallback)"

                        processed_row = ProcessedActivity(
                            id=str(uuid.uuid4()),
                            source_id=row.id,
                            topic=result.get("topic") or row.topic,
                            source_text=row.text or "",
                            extracted_locations=result["extracted_locations"],
                            geocoded_location=result["geocoded_location"],
                            latitude=final_lat,
                            longitude=final_lon,
                            sentiment_score=result["sentiment_score"],
                            sentiment_label=result["sentiment_label"],
                            processed_at=datetime.now(timezone.utc),
                        )
                        db.add(processed_row)

                        # Update source row status
                        row.status = "processed"
                        # Back-fill lat/lon on source row if we geocoded it
                        if result["latitude"] and row.latitude is None:
                            row.latitude = result["latitude"]
                            row.longitude = result["longitude"]

                        db.flush()  # push to transaction; commit happens per sub-batch
                        stats["processed"] += 1
                        batch_processed += 1

                        logger.debug(
                            "✓ source_id=%s | locations=%s | %s → (%.4f, %.4f) | %s",
                            row.id,
                            result["extracted_locations"],
                            result["geocoded_location"],
                            result["latitude"] or 0,
                            result["longitude"] or 0,
                            result["sentiment_label"],
                        )
                        
                        # COMMIT IMMEDIATELY after each successful (or fallback) geocode
                        db.commit()

                    except Exception as exc:  # noqa: BLE001
                        logger.error("Error processing row %s: %s", row.id, exc)
                        row.status = "error"
                        db.flush()
                        db.commit() # Commit the error status
                        stats["errors"] += 1

                    # Throttling to prevent CPU/Memory spikes in cloud
                    time.sleep(2)

                if batch_processed > 0:
                    print(f"[WORKER] Successfully processed {batch_processed} items")
                    logger.info("SUCCESS: Processed %d posts", batch_processed)
                    
                    # Enforce the 1,000-row storage cap after every successful commit
                    prune_processed_activity(db)
                    db.commit()

        except Exception as exc:  # noqa: BLE001
            logger.critical("Worker error during polling cycle: %s", exc)
            db.rollback()
        finally:
            db.close()

        # Sleep before the next polling cycle
        time.sleep(3)
        time.sleep(poll_interval)


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------
def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Coriolis NLP worker — processes pending social_activity rows."
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=500,
        metavar="N",
        help="Maximum number of rows to process in this run (default: 500).",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=50,
        metavar="N",
        help="Number of rows fetched per DB query (default: 50).",
    )
    parser.add_argument(
        "--poll-interval",
        type=int,
        default=10,
        metavar="S",
        help="Seconds to sleep when no pending rows are found (default: 10).",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = _parse_args(argv)
    try:
        run_worker(
            limit=args.limit,
            batch_size=args.batch_size,
            poll_interval=args.poll_interval,
        )
    except KeyboardInterrupt:
        logger.info("Worker stopped by user.")


if __name__ == "__main__":
    main()
