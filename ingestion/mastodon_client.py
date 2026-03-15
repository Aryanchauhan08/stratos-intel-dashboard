"""
ingestion/mastodon_client.py
----------------------------
REST polling client for the Mastodon public federated timeline.

Replacing the WebSocket stream (which suffers from keep-alive decoding bugs)
with a robust polling loop on /api/v1/timelines/public.

Two modes are available:
  1. stream_public()      — Long-lived loop fetching the timeline every 10s.
  2. fetch_public_posts() — One-shot REST poll.

Environment variables (loaded from .env):
  MASTODON_API_BASE_URL  - e.g. https://mastodon.social
  MASTODON_ACCESS_TOKEN  - highly recommended to avoid severe rate-limiting
"""

from __future__ import annotations

import logging
import os
import time
from html.parser import HTMLParser
from typing import Callable, Optional

import requests
from dotenv import load_dotenv
from mastodon import Mastodon

from database.models import SocialActivity, SessionLocal

load_dotenv()

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# HTML stripping utility
# ---------------------------------------------------------------------------

class _HTMLStripper(HTMLParser):
    """Lightweight HTML → plain-text converter."""

    def __init__(self) -> None:
        super().__init__()
        self._parts: list[str] = []

    def handle_data(self, data: str) -> None:
        self._parts.append(data)

    def get_text(self) -> str:
        return " ".join(self._parts).strip()


def strip_html(html: str) -> str:
    """Return *html* with all tags removed."""
    parser = _HTMLStripper()
    parser.feed(html or "")
    return parser.get_text()


# ---------------------------------------------------------------------------
# Record Builder
# ---------------------------------------------------------------------------

def build_mastodon_record(status: dict, queried_topic: Optional[str] = None) -> dict:
    """Convert a raw Mastodon status dict into a SocialActivity dict."""
    account = status.get("account", {})

    # Location: try account fields first, then account note
    raw_location: Optional[str] = None
    for field in account.get("fields", []):
        name = (field.get("name") or "").lower()
        if "location" in name or "loc" in name or "place" in name:
            raw_location = strip_html(field.get("value", ""))
            break

    # Keywords from tags
    keywords: list[str] = [
        tag.get("name", "").lower() for tag in status.get("tags", []) if tag.get("name")
    ]
    if queried_topic and queried_topic.lower() not in keywords:
        keywords.append(queried_topic.lower())

    text = strip_html(status.get("content", ""))
    timestamp = (
        status["created_at"].isoformat()
        if hasattr(status.get("created_at"), "isoformat")
        else str(status.get("created_at"))
    )
    status_id = str(status.get("id", ""))

    return {
        "source": "mastodon",
        "topic": queried_topic,
        "text": text,
        "timestamp": timestamp,
        "raw_location": "Placeholder (Mastodon)", # The instruction implies this change
        "latitude": None,
        "longitude": None,
        "keywords": keywords,
        "external_id": status_id,
        # Extra context (not in DB schema but handy for debugging)
        "_meta": {
            "status_id": status_id,
            "account_id": str(account.get("id")), # The instruction implies this change
            "username": account.get("username"),   # The instruction implies this change
            "url": status.get("url", ""),
            "language": status.get("language", ""),
        },
    }


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def build_mastodon_client(
    api_base_url: Optional[str] = None,
    access_token: Optional[str] = None,
) -> Mastodon:
    """
    Create and return a configured ``Mastodon`` client.
    """
    base_url = api_base_url or os.getenv(
        "MASTODON_API_BASE_URL", "https://mastodon.social"
    )
    token = access_token or os.getenv("MASTODON_ACCESS_TOKEN") or None

    kwargs: dict = {"api_base_url": base_url, "request_timeout": 30}
    if token:
        kwargs["access_token"] = token

    client = Mastodon(**kwargs)
    logger.info("Mastodon client created for %s (auth=%s)", base_url, bool(token))
    return client


def stream_public(
    on_activity: Optional[Callable[[dict], None]] = None,
    max_posts: Optional[int] = None,
    api_base_url: Optional[str] = None,
    access_token: Optional[str] = None,
    poll_interval: int = 10,
) -> None:
    """
    Continuously poll the public federated timeline.
    Simulates streaming by fetching the latest 40 posts every `poll_interval` seconds.

    This call **blocks** the current thread.  For integration into
    FastAPI use ``asyncio.to_thread(stream_public, ...)``.

    Parameters
    ----------
    on_activity : callable, optional
        Invoked for every newly received post with a SocialActivity-shaped dict.
    max_posts : int, optional
        Stop streaming after this many posts (handy for smoke tests).
    api_base_url : str, optional
        Mastodon instance URL.  Falls back to ``MASTODON_API_BASE_URL``.
    access_token : str, optional
        Bearer token.  Falls back to ``MASTODON_ACCESS_TOKEN``.
    poll_interval : int, optional
        Seconds to wait between polls (default 10s).
    """
    client = build_mastodon_client(api_base_url, access_token)
    on_activity = on_activity or (lambda record: None)
    
    logger.info("Starting Mastodon REST polling (interval=%ds)…", poll_interval)
    
    import hashlib
    import sqlite3
    
    cache_db_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "mastodon_hash_cache.db")
    conn = sqlite3.connect(cache_db_path)
    conn.execute("CREATE TABLE IF NOT EXISTS seen_hashes (hash TEXT PRIMARY KEY, added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)")
    conn.commit()

    posts_yielded = 0
    
    import itertools
    topics = ['news', 'tech', 'travel', 'climate', 'sports', 'ai', 'weather', 'politics', 'economy', 'finance', 'banking', 'datascience', 'machinelearning', 'gaming', 'food']
    topic_cycle = itertools.cycle(topics)

    while True:
        try:
            current_topic = next(topic_cycle)
            # Fetch up to 20 latest posts from the current hashtag
            statuses = client.timeline_hashtag(current_topic, limit=20)
            
            new_count = 0
            # Reverse statuses so oldest in the batch are processed first
            for status in reversed(statuses):
                raw_text = strip_html(status.get("content", ""))
                url = status.get("url", "")
                
                # Content properties for robust deduplication
                content_to_hash = raw_text if raw_text.strip() else url
                normalized_content = content_to_hash.lower().strip()
                
                if not normalized_content:
                    continue
                    
                hash_hex = hashlib.md5(normalized_content.encode("utf-8")).hexdigest()
                
                # Check persistent cache
                cursor = conn.execute("SELECT 1 FROM seen_hashes WHERE hash = ?", (hash_hex,))
                if cursor.fetchone() is not None:
                    continue
                
                # Mark as seen
                conn.execute("INSERT INTO seen_hashes (hash) VALUES (?)", (hash_hex,))
                conn.commit()
                
                new_count += 1
                
                try:
                    record = build_mastodon_record(status, queried_topic=current_topic)
                    on_activity(record)
                    posts_yielded += 1
                    
                    if max_posts and posts_yielded >= max_posts:
                        logger.info("Stream ended cleanly (max_posts %d reached).", max_posts)
                        return
                except Exception as exc:
                    logger.warning("Failed to process status %s: %s", status.get("id", "UNKNOWN"), exc)
            
            if new_count > 0:
                logger.info("SUCCESS: Processed %d posts (topic: %s)", new_count, current_topic)
            
            # Prune SQLite cache to prevent infinite growth (keep latest 10,000 hashes)
            conn.execute("""
                DELETE FROM seen_hashes 
                WHERE hash NOT IN (
                    SELECT hash FROM seen_hashes 
                    ORDER BY added_at DESC 
                    LIMIT 10000
                )
            """)
            conn.commit()

        except Exception as exc:
            logger.warning("Mastodon polling error: %s — will retry in %ds", exc, poll_interval)

        time.sleep(poll_interval)


def fetch_public_posts(
    limit: int = 20,
    api_base_url: Optional[str] = None,
) -> list[dict]:
    """
    One-shot REST fetch of the public federated timeline.
    """
    base_url = api_base_url or os.getenv(
        "MASTODON_API_BASE_URL", "https://mastodon.social"
    )
    url = f"{base_url.rstrip('/')}/api/v1/timelines/public"
    params = {"limit": min(limit, 40)}

    logger.info("Fetching %d posts from %s …", limit, url)
    resp = requests.get(url, params=params, timeout=30)
    resp.raise_for_status()

    statuses: list[dict] = resp.json()
    logger.info("Received %d statuses.", len(statuses))

    records: list[dict] = []
    for status in statuses[:limit]:
        try:
            records.append(build_mastodon_record(status))
        except Exception as exc:  # pragma: no cover
            logger.warning("Skipping malformed status: %s", exc)
    return records


# ---------------------------------------------------------------------------
# Quick smoke test
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    import sys
    from pathlib import Path

    # Ensure project root is in path so we can import 'database.models'
    project_root = str(Path(__file__).resolve().parent.parent)
    if project_root not in sys.path:
        sys.path.insert(0, project_root)

    from database.models import SocialActivity, SessionLocal
    from datetime import datetime

    logging.basicConfig(level=logging.INFO)

    def _save_to_db(record: dict) -> None:
        db = SessionLocal()
        try:
            timestamp_str = record.get("timestamp")
            dt = datetime.fromisoformat(timestamp_str.replace("Z", "+00:00")) if timestamp_str else None
            
            # Deduplication check
            from sqlalchemy import exists
            if db.query(exists().where(SocialActivity.external_id == record["external_id"])).scalar():
                logger.info("[mastodon smoke] Duplicate skipped: %s", record["external_id"])
                return

            activity = SocialActivity(
                source=record["source"],
                topic=record.get("topic"),
                text=record["text"],
                timestamp=dt,
                raw_location=record.get("raw_location"),
                latitude=record.get("latitude"),
                longitude=record.get("longitude"),
                keywords=record.get("keywords") or [],
                external_id=record.get("external_id"),
                status="pending"
            )
            db.add(activity)
            db.commit()
        except Exception as exc:
            db.rollback()
            print(f"CRITICAL DB SAVE ERROR (Mastodon Smoke Test): {exc}")
            logger.error("Failed to save record to DB: %s", exc)
        finally:
            db.close()

    print("Connecting to Mastodon #news timeline — polling every 10s.\n")
    
    while True:
        try:
            stream_public(on_activity=_save_to_db, max_posts=None, poll_interval=10)
        except KeyboardInterrupt:
            print("\nStopped by user.")
            break
        except Exception as exc:
            logger.error("Polling loop crashed unexpectedly: %s", exc)
        
        # Fallback sleep to prevent hot-looping if stream_public exits immediately
        time.sleep(10)
