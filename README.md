# 🌍 Stratos Intel Dashboard (Coriolis)

> **A real-time global intelligence dashboard** that aggregates social signals and geopolitical news from across the internet, enriches them with AI-powered NLP, and plots them live on an interactive world map.

---

## 📌 What Is This Project?

**Stratos Intel Dashboard** (internally codenamed *Coriolis*) is a full-stack data intelligence platform that answers one question:

> *"What is the world talking about right now, and where?"*

It continuously pulls posts from **Mastodon** (decentralised social media), articles from **major global RSS news feeds**, and geopolitical events from the **GDELT Global Knowledge Graph** — all in real time. Every piece of content is then processed through an NLP pipeline that:

- Extracts named locations using **spaCy NER** (Named Entity Recognition)
- Geocodes those locations to **latitude/longitude** using the **Nominatim** geocoder
- Scores the emotional tone using **VADER Sentiment Analysis**

The enriched, geo-tagged data is served through a **FastAPI REST API** as **GeoJSON** and displayed on a live **interactive world map** in the browser frontend.

---

## 🏗️ System Architecture

```
┌────────────────────────────────────────────────────────────────┐
│                        DATA SOURCES                            │
│  Mastodon API  ──┐                                             │
│  RSS Feeds     ──┼──► social_activity table (raw, "pending")   │
│  GDELT GKG     ──┘                                             │
└────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌────────────────────────────────────────────────────────────────┐
│                     NLP WORKER (processing/worker.py)          │
│  spaCy NER → Nominatim Geocoder → VADER Sentiment              │
│  → writes to processed_activity table                          │
│  → enforces 1,000-row FIFO rolling storage cap                 │
└────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌────────────────────────────────────────────────────────────────┐
│                  FastAPI REST API (api/main.py)                 │
│  GET /api/v1/activity  →  GeoJSON FeatureCollection            │
│  GET /api/v1/stats     →  Aggregated statistics                │
└────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌────────────────────────────────────────────────────────────────┐
│              Browser Frontend (frontend/)                      │
│  Interactive world map · Filters · Live updates                │
└────────────────────────────────────────────────────────────────┘
```

All four stages run **concurrently as async background tasks** inside a single `uvicorn` / FastAPI process launched from `main.py`.

---

## 📁 Project Structure

```
stratos-intel-dashboard/
│
├── main.py                    # FastAPI app entry point + background task launcher
│
├── database/
│   ├── models.py              # SQLAlchemy ORM models (SocialActivity, ProcessedActivity)
│   └── __init__.py
│
├── ingestion/
│   ├── mastodon_client.py     # Polls Mastodon hashtag timelines (40 posts/topic)
│   ├── rss_client.py          # Polls major global RSS news feeds every 5 min
│   ├── gdelt_client.py        # Downloads GDELT GKG every 15 min
│   └── __init__.py
│
├── processing/
│   ├── worker.py              # NLP worker: NER + geocoding + sentiment + pruning
│   ├── nlp_processor.py       # spaCy, Nominatim, VADER logic
│   └── __init__.py
│
├── api/
│   ├── main.py                # API router: /api/v1/activity, /api/v1/stats
│   ├── schemas.py             # Pydantic response models (GeoJSON, StatsResponse)
│   └── __init__.py
│
├── frontend/
│   ├── index.html             # Single-page application shell
│   ├── app.js                 # Map rendering, API calls, filters, live updates
│   └── style.css              # Dashboard styling
│
├── requirements.txt           # Python dependencies
├── reset_db.py                # Utility: wipe and recreate the database
└── .env                       # Environment variables (not committed)
```

---

## 🔌 Data Sources — Why We Use Them

### 1. 🐘 Mastodon (Social Media)
**File:** `ingestion/mastodon_client.py`

**What it is:** Mastodon is a decentralised, open-source social network (part of the Fediverse). Unlike Twitter/X, its API is completely free and public with no rate-limit walls.

**Why we use it:**
- Free, open API — no API key required for public timelines
- Real-time human sentiment on current events (news, tech, climate, politics, etc.)
- Supports hashtag-based polling, giving us topic-level control

**How it works:**
- Polls 15 different hashtag timelines in a rotating cycle: `news`, `tech`, `travel`, `climate`, `sports`, `ai`, `weather`, `politics`, `economy`, `finance`, `banking`, `datascience`, `machinelearning`, `gaming`, `food`
- Fetches **up to 40 posts per topic per cycle** (the Mastodon API hard cap per request)
- Strips HTML from post content
- Extracts location from user profile fields
- Deduplicates using a fast in-memory `seen_ids` set
- Saves new posts to `social_activity` with `status = "pending"`

**Poll interval:** Every **10 seconds** per topic

---

### 2. 📰 RSS News Feeds (Global News)
**File:** `ingestion/rss_client.py`

**What it is:** RSS (Really Simple Syndication) is a standard XML format that major news organisations publish to distribute their headlines and article summaries.

**Why we use it:**
- Provides structured, high-quality editorial news content
- Zero authentication required
- Covers major global perspectives (BBC, NYT, Al Jazeera, NPR)

**Feeds we pull from:**

| Source | Feed URL |
|---|---|
| BBC World News | `feeds.bbci.co.uk/news/world/rss.xml` |
| New York Times World | `rss.nytimes.com/services/xml/rss/nyt/World.xml` |
| Al Jazeera | `aljazeera.com/xml/rss/all.xml` |
| NPR News | `feeds.npr.org/1004/rss.xml` |

**How it works:**
- Uses `feedparser` to download and parse all 4 feeds
- Combines article title + summary into a single text field
- Appends the source article URL for traceability
- Deduplicates against the database using a case-insensitive title match
- Saves new articles to `social_activity` with `status = "pending"`

**Poll interval:** Every **5 minutes (300 seconds)**

---

### 3. 🌐 GDELT Global Knowledge Graph (Geopolitical Events)
**File:** `ingestion/gdelt_client.py`

**What it is:** [GDELT](https://www.gdeltproject.org/) (Global Database of Events, Language, and Tone) is one of the world's largest open data platforms for news media. It monitors news in 100+ languages across 65 languages and publishes updates every **15 minutes**.

**Why we use it:**
- Provides pre-parsed geolocation data directly in the feed (lat/lon extracted from article content)
- Covers geopolitical themes with CAMEO event codes
- Free, no authentication required, extremely high volume

**What we pull:**
- The `lastupdate.txt` index file to discover the current GKG ZIP URL
- The latest CSV file is downloaded in-memory (no disk writes)
- Extracts: record ID, publication date, source name, article URL, CAMEO themes, location blocks, tone score
- Parses location blocks (`type#fullname#countrycode#adm1#lat#lon#featureId`) to extract coordinates directly

**Poll interval:** Every **15 minutes (900 seconds)**

---

## ⚙️ NLP Processing Pipeline
**File:** `processing/worker.py`, `processing/nlp_processor.py`

Every raw item saved by the ingestion scripts enters the `social_activity` table with `status = "pending"`. The worker picks these up in batches and runs the following pipeline:

### Step 1 — Named Entity Recognition (spaCy)
Uses the `en_core_web_sm` spaCy model to identify **GPE** (geopolitical entities) and **LOC** (locations) in the post text.

Example: `"Flooding reported in Mumbai and coastal Karnataka"` → `["Mumbai", "Karnataka"]`

### Step 2 — Geocoding (Nominatim / OpenStreetMap)
Takes the extracted location names and queries the **Nominatim** geocoder (powered by OpenStreetMap) to get latitude and longitude.

- Respects Nominatim's 1 request/second rate limit with a 1.1s delay between calls
- Falls back to `(0.0, 0.0)` if no location can be resolved
- Results are cached in `geocoding_cache.json` to avoid redundant API calls

### Step 3 — Sentiment Analysis (VADER)
Uses the **VADER** (Valence Aware Dictionary and sEntiment Reasoner) model to score the emotional tone of the text:

| Score | Label |
|---|---|
| ≥ +0.05 | Positive |
| ≤ −0.05 | Negative |
| Between | Neutral |

### Step 4 — Storage Limit Enforcement (FIFO Prune)
After every successful batch commit, the worker enforces a **hard cap of 1,000 rows** in `processed_activity`:

```sql
DELETE FROM processed_activity
WHERE id NOT IN (
    SELECT id FROM processed_activity
    ORDER BY processed_at DESC
    LIMIT 1000
)
```

When rows are pruned, a maintenance log is printed:
```
[MAINTENANCE] Storage limit reached. Pruned 42 old posts to maintain 1,000 post buffer.
```

This keeps the database lean and fast without any manual cleanup.

---

## 🗃️ Database Schema

### `social_activity` — Raw ingested data

| Column | Type | Description |
|---|---|---|
| `id` | UUID PK | Auto-generated unique ID |
| `source` | String | `mastodon`, `rss`, or `gdelt` |
| `topic` | String | Hashtag or feed topic |
| `text` | Text | Full post/article content |
| `timestamp` | DateTime | Original publication time (UTC) |
| `raw_location` | String | Location string from source |
| `latitude` | Float | Source-provided lat (nullable) |
| `longitude` | Float | Source-provided lon (nullable) |
| `keywords` | JSON | Array of hashtags/CAMEO themes |
| `status` | String | `pending` → `processed` or `error` |

### `processed_activity` — NLP-enriched data

| Column | Type | Description |
|---|---|---|
| `id` | UUID PK | Auto-generated unique ID |
| `source_id` | UUID FK | Links to `social_activity.id` |
| `topic` | String | Topic carried over from source |
| `source_text` | Text | Original post text (denormalised) |
| `extracted_locations` | JSON | List of NER-identified place names |
| `geocoded_location` | String | First place name that resolved to coords |
| `latitude` | Float | Geocoded latitude |
| `longitude` | Float | Geocoded longitude |
| `sentiment_score` | Float | VADER compound score (−1 to +1) |
| `sentiment_label` | String | `Positive`, `Neutral`, or `Negative` |
| `processed_at` | DateTime | When the worker processed this row *(indexed)* |

> **Index on `processed_at`:** Added for fast FIFO prune queries — the `ORDER BY processed_at DESC` in the DELETE subquery runs in milliseconds even at large table sizes.

---

## 🔗 API Endpoints

Base URL: `http://localhost:8000`

### `GET /api/v1/activity`
Returns all processed posts as a **GeoJSON FeatureCollection** for map rendering.

**Query Parameters:**

| Parameter | Type | Description |
|---|---|---|
| `source` | string | Filter by `mastodon`, `rss`, or `gdelt` |
| `topic` | string | Filter by topic/hashtag (e.g., `news`, `tech`) |
| `search` | string | Full-text search within post body |
| `min_sentiment` | float | Minimum VADER score (−1.0 to +1.0) |
| `time_range` | string | `1h` or `24h` |
| `limit` | int | Max results (default 500, max 2000) |

**Example Response:**
```json
{
  "type": "FeatureCollection",
  "count": 142,
  "features": [
    {
      "type": "Feature",
      "geometry": { "type": "Point", "coordinates": [72.8777, 19.0760] },
      "properties": {
        "source": "mastodon",
        "topic": "climate",
        "text": "Flooding reported in Mumbai...",
        "sentiment_score": -0.62,
        "sentiment_label": "Negative",
        "geocoded_location": "Mumbai"
      }
    }
  ]
}
```

### `GET /api/v1/stats`
Returns aggregated statistics over the processed data.

**Returns:**
- `total_records` — total rows in `processed_activity`
- `records_with_coords` — rows with valid lat/lon
- `by_source` — count per data source
- `avg_sentiment_by_source` — average VADER score per source
- `by_sentiment_label` — count per sentiment label
- `by_topic` — count per topic
- `top_geocoded_locations` — top 10 most mentioned places

### `GET /health`
Liveness probe — always returns `{"status": "ok"}`.

### `POST /ingest/gdelt`
Manual trigger for a GDELT GKG pull. Returns raw records for inspection.

### `GET /docs`
Interactive Swagger UI for exploring all API endpoints.

---

## 🖥️ Frontend

**Files:** `frontend/index.html`, `frontend/app.js`, `frontend/style.css`

The frontend is a vanilla HTML/CSS/JavaScript single-page application served directly by FastAPI.

**Features:**
- **Interactive world map** powered by Leaflet.js with OpenStreetMap tiles
- **Colour-coded markers** by sentiment (green = positive, red = negative, grey = neutral)
- **Filter panel** — filter by source, topic, sentiment score, time range, and keyword search
- **Live auto-refresh** — polls `/api/v1/activity` every 30 seconds for new posts
- **Stats sidebar** — shows total posts, sentiment breakdown, and top locations
- **Click-to-inspect** — click any map marker to see the full post text and metadata

---

## 🚀 Getting Started

### Prerequisites
- Python 3.10+
- SQLite (default) or PostgreSQL

### 1. Clone the repository
```bash
git clone https://github.com/nikhil1770/stratos-intel-dashboard.git
cd stratos-intel-dashboard
```

### 2. Install dependencies
```bash
pip install -r requirements.txt
python -m spacy download en_core_web_sm
```

### 3. Configure environment
Create a `.env` file in the project root:
```env
# Optional: Mastodon API token (increases rate limits)
MASTODON_ACCESS_TOKEN=your_token_here
MASTODON_API_BASE_URL=https://mastodon.social

# Optional: Use PostgreSQL instead of SQLite
# DATABASE_URL=postgresql://user:password@localhost:5432/stratos

# Optional: Restrict CORS origins
# ALLOWED_ORIGINS=https://yourdomain.com
```

> **Note:** Without a `.env` file, the app uses a local SQLite database (`stratos_production.db`) which is perfect for development.

### 4. Run the application
```bash
uvicorn main:app --host 0.0.0.0 --port 8000 --reload
```

The app will:
1. Create all database tables automatically
2. Start the Mastodon poller (every 10s)
3. Start the RSS poller (every 5 min)
4. Start the GDELT poller (every 15 min)
5. Start the NLP worker (continuous)
6. Serve the frontend at `http://localhost:8000`
7. Serve the API at `http://localhost:8000/api/v1/`
8. Serve Swagger docs at `http://localhost:8000/docs`

### 5. (Optional) Reset the database
```bash
python reset_db.py
```

---

## 📦 Dependencies

| Package | Version | Purpose |
|---|---|---|
| `fastapi` | 0.135.1 | REST API framework |
| `uvicorn` | 0.41.0 | ASGI server |
| `sqlalchemy` | 2.0.34 | ORM and database access |
| `pydantic` | 2.12.5 | Request/response validation |
| `mastodon.py` | 2.1.4 | Mastodon API client |
| `feedparser` | 6.0.12 | RSS feed parsing |
| `requests` | 2.32.5 | HTTP client for GDELT |
| `pandas` | 3.0.1 | GDELT CSV parsing |
| `geopy` | 2.4.1 | Nominatim geocoding |
| `vaderSentiment` | 3.3.2 | Sentiment analysis |
| `python-dotenv` | 1.2.2 | Environment variable loading |
| `psycopg2-binary` | latest | PostgreSQL adapter |

> spaCy (`en_core_web_sm`) is downloaded separately via `python -m spacy download en_core_web_sm`.

---

## 🔧 Key Design Decisions

| Decision | Why |
|---|---|
| **SQLite by default** | Zero-config local dev; swap to PostgreSQL for production via `DATABASE_URL` env var |
| **1,000-row FIFO cap** | Keeps the DB lean and the API fast; oldest posts auto-pruned, newest always available |
| **Index on `processed_at`** | Makes the prune DELETE query instant regardless of table size |
| **Async background tasks** | All 4 services (3 ingesters + 1 worker) run concurrently without blocking the API |
| **GeoJSON API response** | Directly consumable by Leaflet.js and any standard mapping library |
| **VADER over transformer models** | Runs CPU-only, no GPU required, <1ms per post, good enough for short social text |
| **Nominatim geocoder** | Free, open-source, no API key needed; backs OpenStreetMap |
| **deduplication in RSS** | Prevents the same article appearing multiple times across polling cycles |

---

## 📊 Data Flow Summary

```
[Mastodon API]  ─► poll every 10s   ─► 40 posts/topic ─┐
[RSS Feeds]     ─► poll every 5min  ─► all entries     ─┼─► social_activity (pending)
[GDELT GKG]     ─► poll every 15min ─► 100-200 rows    ─┘
                                                          │
                                                          ▼
                                        NLP Worker picks up pending rows
                                          │
                                          ├─► spaCy NER  → location names
                                          ├─► Nominatim  → lat/lon
                                          └─► VADER      → sentiment score
                                                          │
                                                          ▼
                                        processed_activity (max 1,000 rows)
                                                          │
                                                          ▼
                                        GET /api/v1/activity → GeoJSON
                                                          │
                                                          ▼
                                        🗺️  Live World Map (browser)
```

---

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch: `git checkout -b feature/my-feature`
3. Commit your changes: `git commit -m 'feat: add my feature'`
4. Push to the branch: `git push origin feature/my-feature`
5. Open a Pull Request

---

## 📄 License

This project is open source. See [LICENSE](LICENSE) for details.

---

*Built with ❤️ using FastAPI, spaCy, VADER, Nominatim, Mastodon, GDELT, and OpenStreetMap.*
