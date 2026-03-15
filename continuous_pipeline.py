import time
import hashlib
import sqlite3
import schedule
from datetime import datetime
from geopy.geocoders import Nominatim
from geopy.exc import GeocoderTimedOut, GeocoderUnavailable

# ==========================================
# 1. Geocoder Setup (Nominatim)
# ==========================================
try:
    geolocator = Nominatim(user_agent="unified_fetching_pipeline")
except Exception as e:
    print(f"Failed to initialize geolocator: {e}")
    geolocator = None

# ==========================================
# 2. Database Initialization
# ==========================================
DB_PATH = "pipeline_cache.db"

def init_db():
    """Create the SQLite database and articles table if it doesn't exist."""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS articles (
            text_hash TEXT PRIMARY KEY,
            source TEXT,
            content TEXT,
            location_name TEXT,
            latitude REAL,
            longitude REAL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    # Index on created_at for fast cleanup
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_created_at ON articles(created_at)")
    conn.commit()
    conn.close()

# ==========================================
# 3. Geocoding Helper
# ==========================================
def get_coordinates(location_name):
    """Fallback precise geocoding utilizing Geopy with rate-limit protection."""
    if not geolocator or not location_name:
        return None, None
    try:
        # Nominatim strictly requires 1 request per second max
        time.sleep(1.2)
        location = geolocator.geocode(location_name, timeout=10)
        if location:
            return location.latitude, location.longitude
    except (GeocoderTimedOut, GeocoderUnavailable) as e:
        print(f"Geocoding timeout/unavailable for {location_name}: {e}")
    except Exception as e:
        print(f"Unexpected geocoding error for {location_name}: {e}")
    return None, None

# ==========================================
# 4. Processing & Content-Based Deduplication
# ==========================================
def process_and_insert(source_name, raw_text, location_name=None):
    """Standardizes text, hashes it, skips duplicates, geocodes, and inserts."""
    if not raw_text:
        return

    # Normalize and Hash
    normalized_text = raw_text.lower().strip()
    text_hash = hashlib.md5(normalized_text.encode('utf-8')).hexdigest()

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    # Deduplication Check
    cursor.execute("SELECT 1 FROM articles WHERE text_hash = ?", (text_hash,))
    if cursor.fetchone() is not None:
        conn.close()
        return  # Hash exists, skip processing

    # Geocoding Fallback (Triggered for GDELT or when explicit location handles exist)
    lat, lon = None, None
    if source_name == "GDELT" and location_name:
        lat, lon = get_coordinates(location_name)

    # Insert into Database
    # UTC Timestamp for consistent rolling window deletion
    timestamp = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
    try:
        cursor.execute("""
            INSERT INTO articles (text_hash, source, content, location_name, latitude, longitude, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (text_hash, source_name, raw_text, location_name, lat, lon, timestamp))
        conn.commit()
        print(f"Inserted new post from {source_name}: {text_hash}")
    except sqlite3.IntegrityError:
        pass # Gracefully handle race conditions
    finally:
        conn.close()

# ==========================================
# 5. Database Auto-Cleanup (Rolling Window)
# ==========================================
def cleanup_database():
    """Restricts the SQLite database to the 5,000 most recent records."""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # Keep the newest 5000 rows, delete the rest across all sources
    cursor.execute("""
        DELETE FROM articles 
        WHERE text_hash NOT IN (
            SELECT text_hash 
            FROM articles 
            ORDER BY created_at DESC 
            LIMIT 5000
        )
    """)
    deleted_count = cursor.rowcount
    conn.commit()
    conn.close()
    
    if deleted_count > 0:
        print(f"Cleanup: Removed {deleted_count} old records to maintain 5,000 limit.")

# ==========================================
# 6. API Fetching Stubs (Fetch up to 100 items each)
# ==========================================
def fetch_mastodon():
    print("-> Fetching Mastodon...")
    # TODO: Replace with real mastodon API call (limit=100)
    mock_posts = ["Breaking news: AI models improving", "A completely different tech post"]
    for post in mock_posts:
        process_and_insert("Mastodon", post)

def fetch_gdelt():
    print("-> Fetching GDELT...")
    # TODO: Replace with real GDELT parsing (limit=100)
    mock_gdelt_data = [
        {"text": "UK Parliament debates tech regulations", "location": "London, United Kingdom"},
        {"text": "Global markets rally significantly", "location": "New York, USA"}
    ]
    for item in mock_gdelt_data:
        process_and_insert("GDELT", item["text"], item["location"])

def fetch_source_three():
    print("-> Fetching Source 3 (e.g. RSS/Twitter/NewsAPI)...")
    # TODO: Replace with real 3rd API call (limit=100)
    mock_items = ["Tech blog uploaded a new tutorial about SQLite."]
    for item in mock_items:
        process_and_insert("Source 3", item)

# ==========================================
# 7. Main Pipeline Loop
# ==========================================
def run_pipeline_cycle():
    """Runs the full fetch routine and performs cleanup."""
    print(f"\n--- Starting Fetch Cycle at {datetime.now()} ---")
    
    fetch_mastodon()
    fetch_gdelt()
    fetch_source_three()
    
    cleanup_database()
    print("--- Cycle Complete ---\n")

def main():
    init_db()
    print("Database Initialized. Starting continuous polling every 60 seconds...")
    
    # Run a cycle immediately on startup
    run_pipeline_cycle()
    
    # Lock the schedule to 60 seconds
    schedule.every(60).seconds.do(run_pipeline_cycle)
    
    # Keep the script alive and listening to the schedule
    while True:
        schedule.run_pending()
        time.sleep(1)

if __name__ == "__main__":
    main()
