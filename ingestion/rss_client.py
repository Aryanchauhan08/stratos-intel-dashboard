"""
ingestion/rss_client.py
-------------------------
Polls major news RSS feeds every 5 minutes and saves new articles
to the Coriolis PostgreSQL database with source='rss' and topic='news'.
"""

import sys
import time
import logging
from pathlib import Path
import feedparser

# Ensure we can import from the parent Coriolis directory
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.append(str(PROJECT_ROOT))
from sqlalchemy import exists

from database.models import SessionLocal, SocialActivity
from html.parser import HTMLParser

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
logger = logging.getLogger(__name__)

# List of major global news RSS feeds
RSS_FEEDS = [
    "http://feeds.bbci.co.uk/news/world/rss.xml",
    "https://rss.nytimes.com/services/xml/rss/nyt/World.xml",
    "https://www.aljazeera.com/xml/rss/all.xml",
    "https://feeds.npr.org/1004/rss.xml",
]

def fetch_rss_feeds() -> int:
    """
    Fetch articles from all RSS feeds, deduplicate against the database,
    and save new articles to social_activity.
    
    Returns:
        The number of new articles inserted.
    """
    import hashlib
    new_articles_count = 0
    skipped_count = 0
    db = SessionLocal()
    
    try:
        for feed_url in RSS_FEEDS:
            logger.info(f"Parsing feed: {feed_url}")
            parsed_feed = feedparser.parse(feed_url)
            
            for entry in parsed_feed.entries:
                title = getattr(entry, "title", "").strip()
                summary = getattr(entry, "summary", "").strip()
                link = getattr(entry, "link", "").strip()
                guid = getattr(entry, "id", "").strip() # RSS guid is often in 'id'
                
                if not title:
                    continue
                
                # Deduplication logic: Use link/guid if available, fallback to hash
                external_id = guid or link
                if not external_id:
                    # Fallback to hash of (title + timestamp + source)
                    # Note: entry.published or entry.updated might be present
                    ts = getattr(entry, "published", getattr(entry, "updated", ""))
                    hash_str = f"{title}|{ts}|rss"
                    external_id = hashlib.sha256(hash_str.encode()).hexdigest()

                # Optimized deduplication check using external_id
                if db.query(exists().where(SocialActivity.external_id == external_id)).scalar():
                    skipped_count += 1
                    continue
                
                # Combine title and summary for the text field
                clean_title = strip_html(title)
                clean_summary = strip_html(summary)
                combined_text = f"{clean_title}. {clean_summary}"
                if link:
                    combined_text += f"\n\nSource: {link}"
                
                # Create the database record
                activity = SocialActivity(
                    source="rss",
                    topic="news", # Dynamically assign topic
                    text=combined_text,
                    external_id=external_id,
                    status="pending"
                )
                
                db.add(activity)
                new_articles_count += 1
                
        if new_articles_count > 0:
            try:
                db.commit()
                logger.info(f"SUCCESS: Processed {new_articles_count} new RSS articles (skipped {skipped_count} duplicates)")
            except Exception as e:
                db.rollback()
                print(f"CRITICAL DB SAVE ERROR (RSS): {e}")
                logger.error(f"Critical SQL error during RSS commit: {e}")
        else:
            logger.info(f"No new RSS articles found this cycle (skipped {skipped_count} duplicates).")
            
    except Exception as e:
        logger.error(f"Error while fetching RSS feeds: {e}")
        db.rollback()
    finally:
        db.close()
        
    return new_articles_count


def run_rss_ingestion_loop(poll_interval: int = 300):
    """
    Run an infinite loop that polls RSS feeds every `poll_interval` seconds.
    """
    logger.info("Starting RSS ingestion loop...")
    while True:
        logger.info("Fetching latest RSS news...")
        fetch_rss_feeds()
        
        logger.info(f"RSS polling complete. Sleeping for {poll_interval} seconds.")
        time.sleep(poll_interval)


if __name__ == "__main__":
    # Start the continuous polling loop (5 minutes = 300 seconds)
    run_rss_ingestion_loop(poll_interval=300)
