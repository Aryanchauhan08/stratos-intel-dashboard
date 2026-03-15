"""
api/main.py
-----------
FastAPI application for the Coriolis Global Social Media Activity Map.

Endpoints
---------
GET /                       →  redirect to /docs (health-check)
GET /api/v1/activity        →  GeoJSON FeatureCollection of processed posts
GET /api/v1/stats           →  summary statistics

Run with:
    uvicorn api.main:app --host 0.0.0.0 --port 8000 --reload
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone
from typing import List, Optional

from fastapi import APIRouter, Depends, Query
from sqlalchemy import func, text
from sqlalchemy.orm import Session

from database.models import ProcessedActivity, SocialActivity, get_db
from api.schemas import (
    ActivityProperties,
    FeatureCollection,
    GeoJsonFeature,
    GeoJsonGeometry,
    StatsResponse,
)

# ---------------------------------------------------------------------------
# Router setup
# ---------------------------------------------------------------------------

api_router = APIRouter(tags=["API v1"])


def normalize_source(val: Optional[str]) -> Optional[str]:
    """
    Map frontend source labels to internal database source names.
    - 'GDELT_GKG' -> 'gdelt'
    - 'RSS_FEED'  -> 'rss'
    - 'MASTODON'  -> 'mastodon'
    """
    if not val:
        return None

    v = val.lower()
    if v in ["gdelt", "gdelt_gkg"]:
        return "gdelt"
    if v in ["rss", "rss_feed"]:
        return "rss"
    return v


@api_router.get("/debug/source_counts")
def debug_source_counts(db: Session = Depends(get_db)):
    """
    Temporary debug endpoint to inspect database counts for each source.
    """
    social_counts = (
        db.query(SocialActivity.source, func.count())
        .group_by(SocialActivity.source)
        .all()
    )

    processed_counts = (
        db.query(ProcessedActivity.source, func.count())
        .group_by(ProcessedActivity.source)
        .all()
    )

    # Convert results to a more readable format (dict)
    return {
        "social_activity": {source: count for source, count in social_counts},
        "processed_activity": {source: count for source, count in processed_counts},
    }


# ---------------------------------------------------------------------------
# GET /api/v1/activity
# ---------------------------------------------------------------------------

@api_router.get(
    "/api/v1/activity",
    response_model=FeatureCollection,
    summary="Get processed social-media activity as GeoJSON",
    tags=["Activity"],
    responses={
        200: {
            "description": (
                "A GeoJSON FeatureCollection. Each Feature represents one "
                "processed social-media post or news article with coordinates, "
                "sentiment, and text."
            )
        }
    },
)
def get_activity(
    source: Optional[str] = Query(
        default=None,
        description=(
            "Filter by data source. "
            "Known values: **mastodon**, **gdelt**. Case-insensitive."
        ),
        example="mastodon",
    ),
    min_sentiment: Optional[float] = Query(
        default=None,
        ge=-1.0,
        le=1.0,
        description=(
            "Return only records whose VADER compound score is ≥ this value. "
            "Range: −1.0 (most negative) to +1.0 (most positive)."
        ),
        example=0.05,
    ),
    search: Optional[str] = Query(
        default=None,
        min_length=2,
        description=(
            "Case-insensitive full-text filter applied to the post body. "
            "Returns records whose text contains this substring."
        ),
        examples=["climate"],
    ),
    topic: Optional[str] = Query(
        default=None,
        description="Filter by a specific topic/hashtag (e.g., news, tech).",
        examples=["news"],
    ),
    time_range: Optional[str] = Query(
        default=None,
        description="Filter by time range: '1h' (last hour), '24h' (last 24 hours).",
        examples=["24h"],
    ),
    limit: int = Query(
        default=500,
        ge=1,
        le=1000,
        description="Maximum number of features to return (default 500, max 1000).",
    ),
    db: Session = Depends(get_db),
) -> FeatureCollection:
    """
    Query the **processed_activity** table and return a GeoJSON FeatureCollection.

    - Records without valid coordinates (latitude / longitude) are **excluded**.
    - Use `source`, `min_sentiment`, and `keyword` to narrow results.
    - Results are ordered by `processed_at` descending (newest first).
    """
    # Base query — join ProcessedActivity with SocialActivity to get `source`
    query = (
        db.query(ProcessedActivity, SocialActivity.source)
        .join(SocialActivity, ProcessedActivity.source_id == SocialActivity.id)
        .filter(
            ProcessedActivity.latitude.isnot(None),
            ProcessedActivity.longitude.isnot(None),
            ProcessedActivity.latitude != 0.0,
            ProcessedActivity.longitude != 0.0,
        )
    )

    # Optional filters
    source_db = normalize_source(source)
    if source_db:
        query = query.filter(SocialActivity.source == source_db)

    if min_sentiment is not None:
        query = query.filter(ProcessedActivity.sentiment_score >= min_sentiment)

    if topic is not None:
        query = query.filter(ProcessedActivity.topic.ilike(topic))

    if search is not None:
        query = query.filter(ProcessedActivity.source_text.ilike(f"%{search}%"))

    if time_range in ('1h', '24h'):
        now_utc = datetime.now(timezone.utc)
        if time_range == '1h':
            cutoff = now_utc - timedelta(hours=1)
        else:
            cutoff = now_utc - timedelta(hours=24)
        query = query.filter(ProcessedActivity.processed_at >= cutoff)

    rows = (
        query.order_by(ProcessedActivity.processed_at.desc())
        .limit(limit)
        .all()
    )

    features = []
    for processed, src in rows:
        # Map back to display names for frontend matching
        display_source = src
        if src.lower() == "gdelt":
            display_source = "GDELT_GKG"
        elif src.lower() == "rss":
            display_source = "RSS_FEED"

        # Coordinate order: GeoJSON requires [longitude, latitude]
        coords = [processed.longitude, processed.latitude]

        feature = GeoJsonFeature(
            geometry=GeoJsonGeometry(coordinates=coords),
            properties=ActivityProperties(
                id=str(processed.id),
                source=display_source,
                topic=processed.topic,
                text=processed.source_text,
                sentiment_score=processed.sentiment_score,
                sentiment_label=processed.sentiment_label,
                geocoded_location=processed.geocoded_location,
                extracted_locations=processed.extracted_locations or [],
                timestamp=None,   # available via join if needed
                processed_at=processed.processed_at,
            ),
        )
        features.append(feature)

    return FeatureCollection(features=features, count=len(features))


# ---------------------------------------------------------------------------
# GET /api/v1/stats
# ---------------------------------------------------------------------------

@api_router.get(
    "/api/v1/stats",
    response_model=StatsResponse,
    summary="Get summary statistics for processed activity data",
    tags=["Statistics"],
    responses={
        200: {
            "description": (
                "Aggregated counts and averages over the processed_activity table."
            )
        }
    },
)
def get_stats(
    topic: Optional[str] = Query(default=None, description="Filter stats by topic/hashtag", examples=["news"]),
    search: Optional[str] = Query(default=None, description="Keyword search string"),
    time_range: Optional[str] = Query(default=None, description="Time filter ('1h', '24h', 'all')"),
    db: Session = Depends(get_db)
) -> StatsResponse:
    """
    Return high-level summary statistics:

    - **total_records** — total rows in `processed_activity`
    - **records_with_coords** — rows with valid latitude + longitude
    - **by_source** — count of records per data source (mastodon, gdelt, …)
    - **avg_sentiment_by_source** — average VADER score per source
    - **by_sentiment_label** — count per sentiment label (Positive / Neutral / Negative)
    - **top_geocoded_locations** — top 10 most-frequent geocoded place names
    """
    # Helper to apply common filters
    def apply_filters(q):
        if topic:
            q = q.filter(ProcessedActivity.topic.ilike(topic))
        if search:
            q = q.filter(ProcessedActivity.source_text.ilike(f"%{search}%"))
        if time_range in ('1h', '24h'):
            now_utc = datetime.now(timezone.utc)
            cutoff = now_utc - timedelta(hours=1) if time_range == '1h' else now_utc - timedelta(hours=24)
            q = q.filter(ProcessedActivity.processed_at >= cutoff)
        return q

    # Total records
    q_total = db.query(func.count(ProcessedActivity.id))
    q_total = apply_filters(q_total)
    total_records: int = q_total.scalar() or 0

    # Records with coordinates
    q_coords = (
        db.query(func.count(ProcessedActivity.id))
        .filter(
            ProcessedActivity.latitude.isnot(None),
            ProcessedActivity.longitude.isnot(None),
        )
    )
    q_coords = apply_filters(q_coords)
    records_with_coords: int = q_coords.scalar() or 0

    # Counts by source
    q_src = (
        db.query(SocialActivity.source, func.count(ProcessedActivity.id))
        .join(ProcessedActivity, ProcessedActivity.source_id == SocialActivity.id)
    )
    q_src = apply_filters(q_src)
    source_rows = q_src.group_by(SocialActivity.source).all()
    by_source = {row[0]: row[1] for row in source_rows}

    # Average sentiment by source
    q_avg = (
        db.query(SocialActivity.source, func.avg(ProcessedActivity.sentiment_score))
        .join(ProcessedActivity, ProcessedActivity.source_id == SocialActivity.id)
    )
    q_avg = apply_filters(q_avg)
    avg_rows = q_avg.group_by(SocialActivity.source).all()
    avg_sentiment_by_source = {}
    if avg_rows:
        for row in avg_rows:
            if row[0] is not None:
                val = row[1]
                avg_sentiment_by_source[row[0]] = round(float(val), 4) if val is not None else 0.0

    # Counts by sentiment label
    q_label = (
        db.query(ProcessedActivity.sentiment_label, func.count(ProcessedActivity.id))
        .filter(ProcessedActivity.sentiment_label.isnot(None))
    )
    q_label = apply_filters(q_label)
    label_rows = q_label.group_by(ProcessedActivity.sentiment_label).all()
    by_sentiment_label = {row[0]: row[1] for row in label_rows if row[0] is not None}

    # Counts by topic
    q_topic = (
        db.query(ProcessedActivity.topic, func.count(ProcessedActivity.id))
        .filter(ProcessedActivity.topic.isnot(None))
    )
    q_topic = apply_filters(q_topic)
    topic_rows = q_topic.group_by(ProcessedActivity.topic).all()
    by_topic = {row[0]: row[1] for row in topic_rows if row[0] is not None}

    # Top 10 geocoded locations
    q_loc = (
        db.query(
            ProcessedActivity.geocoded_location,
            func.count(ProcessedActivity.id).label("count"),
        )
        .filter(ProcessedActivity.geocoded_location.isnot(None))
    )
    q_loc = apply_filters(q_loc)
    location_rows = q_loc.group_by(ProcessedActivity.geocoded_location).order_by(func.count(ProcessedActivity.id).desc()).limit(10).all()
    top_geocoded_locations = [
        {"location": row[0], "count": row[1]} for row in location_rows
    ]

    return StatsResponse(
        total_records=total_records,
        records_with_coords=records_with_coords,
        by_source=by_source,
        avg_sentiment_by_source=avg_sentiment_by_source,
        by_sentiment_label=by_sentiment_label,
        by_topic=by_topic,
        top_geocoded_locations=top_geocoded_locations,
    )
