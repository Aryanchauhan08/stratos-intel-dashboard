"""
main.py
-------
FastAPI application entry point for the Global Social Media Activity Map
data ingestion service (Coriolis).

Startup
-------
Run with:
    uvicorn main:app --reload --host 0.0.0.0 --port 8000

Endpoints
---------
GET  /health        — liveness probe
POST /ingest/gdelt  — manual GDELT GKG pull (returns sample rows)
"""

from __future__ import annotations

import asyncio
import logging
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any
import threading

import os
import uvicorn
from fastapi import FastAPI, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

from database.models import create_tables, SessionLocal, SocialActivity
from ingestion.gdelt_client import run_gdelt_ingestion_loop, fetch_latest_gkg, gkg_row_to_activity
from ingestion.mastodon_client import stream_public
from ingestion.rss_client import run_rss_ingestion_loop
from processing.worker import run_worker

from api.main import api_router

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Background tasks
# ---------------------------------------------------------------------------
_mastodon_thread: threading.Thread | None = None
_rss_thread: threading.Thread | None = None
_gdelt_thread: threading.Thread | None = None
_worker_thread: threading.Thread | None = None


def _run_mastodon_stream() -> None:
    """Run the Mastodon public stream."""
    def _store(record: dict) -> None:
        db = SessionLocal()
        try:
            from datetime import datetime
            timestamp_str = record.get("timestamp")
            dt = datetime.fromisoformat(timestamp_str.replace("Z", "+00:00")) if timestamp_str else None

            activity = SocialActivity(
                source=record["source"],
                topic=record.get("topic"),
                text=record["text"],
                timestamp=dt,
                raw_location=record.get("raw_location"),
                latitude=record.get("latitude"),
                longitude=record.get("longitude"),
                keywords=record.get("keywords") or [],
                status="pending"
            )
            db.add(activity)
            db.commit()
            logger.info("[mastodon] Saved: %s", record.get("text", "")[:80])
        except Exception as e:
            db.rollback()
            print(f"CRITICAL DB SAVE ERROR (Mastodon): {e}")
            logger.error(f"Critical SQL error during Mastodon save: {e}")
        finally:
            db.close()

    stream_public(_store)


# ---------------------------------------------------------------------------
# Lifespan
# ---------------------------------------------------------------------------
@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Application startup/shutdown.
    """
    global _mastodon_thread, _rss_thread, _gdelt_thread, _worker_thread

    logger.info("Coriolis starting up — creating DB tables if absent…")
    try:
        create_tables()
        logger.info("DB tables ready.")
    except Exception as exc:
        logger.warning("Could not create DB tables: %s", exc)

    # Launch background tasks as threads
    _mastodon_thread = threading.Thread(target=_run_mastodon_stream, daemon=True)
    _mastodon_thread.start()
    
    _rss_thread = threading.Thread(target=run_rss_ingestion_loop, kwargs={"poll_interval": 300}, daemon=True)
    _rss_thread.start()
    
    _gdelt_thread = threading.Thread(target=run_gdelt_ingestion_loop, kwargs={"poll_interval": 900}, daemon=True)
    _gdelt_thread.start()
    
    _worker_thread = threading.Thread(target=run_worker, kwargs={"poll_interval": 10}, daemon=True)
    _worker_thread.start()

    logger.info("All background threads (Mastodon, RSS, GDELT, Worker) launched.")

    yield

    # Shutdown: daemon threads will be killed by the OS, but we can attempt graceful shutdown where possible.
    # We do not block to join these infinite loops.
    logger.info("Coriolis shutting down.")


# ---------------------------------------------------------------------------
# App
# ---------------------------------------------------------------------------
app = FastAPI(
    title="Coriolis — Global Social Media Activity Map",
    description=(
        "Data ingestion API that aggregates real-time social signals from "
        "Mastodon and geopolitical events from GDELT GKG."
    ),
    version="0.1.0",
    lifespan=lifespan,
)

# ---------------------------------------------------------------------------
# CORS
# ---------------------------------------------------------------------------
_raw_origins = os.getenv("ALLOWED_ORIGINS", "*")
allowed_origins: list[str] = (
    ["*"] if _raw_origins == "*" else [o.strip() for o in _raw_origins.split(",")]
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=allowed_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

app.include_router(api_router)

_FRONTEND = Path(__file__).parent / "frontend"


@app.get("/", include_in_schema=False)
async def root() -> FileResponse:
    """Serve the Coriolis frontend SPA."""
    return FileResponse(str(_FRONTEND / "index.html"))


@app.get("/health", tags=["ops"])
async def health() -> dict[str, str]:
    """Liveness probe — always returns 200 OK."""
    return {"status": "ok", "service": "coriolis"}


@app.post("/ingest/gdelt", tags=["ingestion"])
async def ingest_gdelt(
    background_tasks: BackgroundTasks,
    max_rows: int = 50,
) -> JSONResponse:
    """
    Manually trigger a GDELT GKG pull.

    Returns the first *max_rows* records converted to SocialActivity format.
    In production this endpoint would persist to PostgreSQL; here it
    returns the data directly for inspection.
    """
    def _pull() -> list[dict[str, Any]]:
        df = fetch_latest_gkg(max_rows=max_rows)
        return [gkg_row_to_activity(row) for _, row in df.iterrows()]

    records = await asyncio.to_thread(_pull)
    return JSONResponse(content={"count": len(records), "records": records})


# ---------------------------------------------------------------------------
# Static files — frontend SPA (must come AFTER all API routes)
# ---------------------------------------------------------------------------
if _FRONTEND.exists():
    app.mount("/static", StaticFiles(directory=str(_FRONTEND)), name="static")


# ---------------------------------------------------------------------------
# Dev entry point
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=False)
