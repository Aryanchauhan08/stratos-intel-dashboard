"""
database/models.py
------------------
SQLAlchemy ORM models for the Global Social Media Activity Map.
"""

import uuid
from datetime import datetime, timezone

from sqlalchemy import (
    Column,
    DateTime,
    Float,
    ForeignKey,
    JSON,
    String,
    Text,
    create_engine,
)
from sqlalchemy.orm import relationship
from sqlalchemy.orm import declarative_base, sessionmaker

from dotenv import load_dotenv
import os

load_dotenv()

# Force an absolute path for the SQLite database to avoid "ghost database" issues
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
ROOT_DIR = os.path.abspath(os.path.join(CURRENT_DIR, ".."))
DB_PATH = os.path.join(ROOT_DIR, "stratos_production.db")

DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    DATABASE_URL = f"sqlite:///{DB_PATH}"

# ---------------------------------------------------------------------------
# Base & engine
# ---------------------------------------------------------------------------
Base = declarative_base()

engine = create_engine(
    DATABASE_URL, 
    pool_pre_ping=True, 
    connect_args={"check_same_thread": False} if DATABASE_URL.startswith("sqlite") else {}
)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


# ---------------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------------
class SocialActivity(Base):
    """
    Unified schema for social media + news activity events.

    Columns
    -------
    id            UUID primary key, auto-generated.
    source        Name of the data source, e.g. 'mastodon' or 'gdelt'.
    text          Full text content of the post / article summary.
    timestamp     UTC datetime of the original post / publication.
    raw_location  Raw location string as reported by the source (nullable).
    latitude      Parsed latitude  (nullable).
    longitude     Parsed longitude (nullable).
    keywords      JSON array of extracted keywords / themes.
    """

    __tablename__ = "social_activity"

    id = Column(
        String(36),
        primary_key=True,
        default=lambda: str(uuid.uuid4()),
        nullable=False,
        comment="Auto-generated UUID primary key",
    )
    source = Column(
        String(64),
        nullable=False,
        index=True,
        comment="Data source identifier (mastodon | gdelt | …)",
    )
    topic = Column(
        String(64),
        nullable=True,
        index=True,
        comment="The specific topic/hashtag polled",
    )
    text = Column(
        Text,
        nullable=False,
        comment="Full text / content of the record",
    )
    timestamp = Column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(timezone.utc),
        index=True,
        comment="UTC timestamp of the original post",
    )
    raw_location = Column(
        String(512),
        nullable=True,
        comment="Raw location string from the source",
    )
    latitude = Column(
        Float,
        nullable=True,
        comment="Parsed latitude coordinate",
    )
    longitude = Column(
        Float,
        nullable=True,
        comment="Parsed longitude coordinate",
    )
    keywords = Column(
        JSON,
        nullable=True,
        default=list,
        comment="JSON array of extracted keywords / themes",
    )
    status = Column(
        String(32),
        nullable=False,
        default="pending",
        index=True,
        comment="Processing status: pending | processed | error",
    )

    def __repr__(self) -> str:  # pragma: no cover
        return (
            f"<SocialActivity id={self.id} source={self.source!r} "
            f"timestamp={self.timestamp}>"
        )

    def to_dict(self) -> dict:
        """Return a JSON-serialisable dictionary of this record."""
        return {
            "id": str(self.id),
            "source": self.source,
            "topic": self.topic,
            "text": self.text,
            "timestamp": self.timestamp.isoformat() if self.timestamp else None,
            "raw_location": self.raw_location,
            "latitude": self.latitude,
            "longitude": self.longitude,
            "keywords": self.keywords or [],
        }


class ProcessedActivity(Base):
    """
    NLP-enriched version of a SocialActivity row.

    Written by processing/worker.py after running spaCy NER,
    Nominatim geocoding, and VADER sentiment analysis.

    Columns
    -------
    id                  UUID PK, auto-generated.
    source_id           UUID FK → social_activity.id.
    topic               String, the topic carried over from social_activity.
    source_text         Original post text (denormalised for convenience).
    extracted_locations JSON list of GPE/LOC entity strings from NER.
    geocoded_location   The first entity that resolved to coordinates.
    latitude            Geocoded latitude  (nullable).
    longitude           Geocoded longitude (nullable).
    sentiment_score     VADER compound score in [-1, +1].
    sentiment_label     'Positive' | 'Neutral' | 'Negative'.
    processed_at        UTC datetime this row was created.
    """

    __tablename__ = "processed_activity"

    id = Column(
        String(36),
        primary_key=True,
        default=lambda: str(uuid.uuid4()),
        nullable=False,
        comment="Auto-generated UUID PK",
    )
    source_id = Column(
        String(36),
        ForeignKey("social_activity.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
        comment="FK → social_activity.id",
    )
    topic = Column(
        String(64),
        nullable=True,
        index=True,
        comment="The specific topic/hashtag polled, copied from source",
    )
    source_text = Column(
        Text,
        nullable=False,
        comment="Denormalised original post text",
    )
    extracted_locations = Column(
        JSON,
        nullable=True,
        default=list,
        comment="GPE/LOC entity strings found by spaCy NER",
    )
    geocoded_location = Column(
        String(512),
        nullable=True,
        comment="First place name that resolved to coordinates",
    )
    latitude = Column(
        Float,
        nullable=True,
        comment="Geocoded latitude",
    )
    longitude = Column(
        Float,
        nullable=True,
        comment="Geocoded longitude",
    )
    sentiment_score = Column(
        Float,
        nullable=True,
        comment="VADER compound score [-1, +1]",
    )
    sentiment_label = Column(
        String(16),
        nullable=True,
        comment="Positive | Neutral | Negative",
    )
    processed_at = Column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(timezone.utc),
        index=True,
        comment="UTC datetime this record was created by the worker",
    )

    # relationship (lazy-loaded; optional convenience)
    source = relationship("SocialActivity", foreign_keys=[source_id])

    def __repr__(self) -> str:  # pragma: no cover
        return (
            f"<ProcessedActivity id={self.id} "
            f"source_id={self.source_id} label={self.sentiment_label!r}>"
        )

    def to_dict(self) -> dict:
        """Return a JSON-serialisable dictionary."""
        return {
            "id": str(self.id),
            "source_id": str(self.source_id),
            "topic": self.topic,
            "source_text": self.source_text,
            "extracted_locations": self.extracted_locations or [],
            "geocoded_location": self.geocoded_location,
            "latitude": self.latitude,
            "longitude": self.longitude,
            "sentiment_score": self.sentiment_score,
            "sentiment_label": self.sentiment_label,
            "processed_at": self.processed_at.isoformat() if self.processed_at else None,
        }


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def get_db():
    """FastAPI dependency — yields a DB session and closes it afterwards."""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def create_tables() -> None:
    """Create all tables if they don't already exist."""
    Base.metadata.create_all(bind=engine)
