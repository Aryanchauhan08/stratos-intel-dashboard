"""
processing/nlp_processor.py
---------------------------
Core NLP module for the Coriolis Data Processing & NLP Layer (Step 2).

Components
----------
GeoCache          — Persistent JSON-backed cache for geocoding results.
extract_locations — spaCy NER to find GPE/LOC entities in post text.
analyze_sentiment — VADER compound score + Positive/Neutral/Negative label.
geocode           — Nominatim geocoder with cache + rate-limit compliance.
process_post      — Orchestrates all three steps; returns a structured dict.

Usage
-----
    from processing.nlp_processor import process_post, load_models

    load_models()   # call once at startup
    result = process_post({"text": "Protests in Berlin and Paris today."})
    # {
    #   "extracted_locations": ["Berlin", "Paris"],
    #   "geocoded_location": "Berlin",
    #   "latitude": 52.5200,
    #   "longitude": 13.4050,
    #   "sentiment_score": -0.34,
    #   "sentiment_label": "Negative",
    # }
"""

from __future__ import annotations

import json
import logging
import os
import re
import time
from pathlib import Path
from threading import Lock
from typing import Any

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Lazy model handles (populated by load_models())
# ---------------------------------------------------------------------------
_nlp = None          # spaCy language model
_vader = None        # VADER SentimentIntensityAnalyzer

# Project root is one level up from this file
_PROJECT_ROOT = Path(__file__).resolve().parent.parent
_CACHE_FILE = _PROJECT_ROOT / "geocoding_cache.json"


# ---------------------------------------------------------------------------
# Model loader
# ---------------------------------------------------------------------------
def load_models() -> None:
    """Load spaCy and VADER models into module-level singletons.

    Call once at application startup (or before the first call to
    process_post).  Subsequent calls are no-ops.
    """
    global _nlp, _vader

    if _nlp is None:
        import spacy  # noqa: PLC0415
        try:
            _nlp = spacy.load("en_core_web_sm")
            logger.info("spaCy model 'en_core_web_sm' loaded.")
        except Exception as e:
            logger.error(f"CRITICAL: Failed to load SpaCy model: {e}. Switching to lightweight fallback.")
            _nlp = None # Explicitly None to trigger fallback

    if _vader is None:
        from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer  # noqa: PLC0415
        _vader = SentimentIntensityAnalyzer()
        logger.info("VADER SentimentIntensityAnalyzer loaded.")


# ---------------------------------------------------------------------------
# Text cleaning helper
# ---------------------------------------------------------------------------
_URL_RE = re.compile(r"https?://\S+")
_HASHTAG_RE = re.compile(r"#\s*(\w+)")  # keep the word, drop the '#'
_EMOJI_RE = re.compile(
    "["
    "\U0001F600-\U0001F64F"
    "\U0001F300-\U0001F5FF"
    "\U0001F680-\U0001F6FF"
    "\U0001F1E0-\U0001F1FF"
    "\U00002702-\U000027B0"
    "\U000024C2-\U0001F251"
    "]+",
    flags=re.UNICODE,
)


def _clean(text: str) -> str:
    """Remove URLs, emojis, and normalise whitespace for NLP."""
    text = _URL_RE.sub(" ", text)
    text = _EMOJI_RE.sub(" ", text)
    text = _HASHTAG_RE.sub(r"\1", text)
    return " ".join(text.split())


# ---------------------------------------------------------------------------
# Fallback Location Extractor (Regex + Country List)
# ---------------------------------------------------------------------------
_COMMON_COUNTRIES = [
    "Afghanistan", "Albania", "Algeria", "Andorra", "Angola", "Antigua and Barbuda", "Argentina", "Armenia", "Australia", "Austria", "Azerbaijan",
    "Bahamas", "Bahrain", "Bangladesh", "Barbados", "Belarus", "Belgium", "Belize", "Benin", "Bhutan", "Bolivia", "Bosnia and Herzegovina", "Botswana", "Brazil", "Brunei", "Bulgaria", "Burkina Faso", "Burundi",
    "Cabo Verde", "Cambodia", "Cameroon", "Canada", "Central African Republic", "Chad", "Chile", "China", "Colombia", "Comoros", "Congo", "Costa Rica", "Croatia", "Cuba", "Cyprus", "Czech Republic",
    "Denmark", "Djibouti", "Dominica", "Dominican Republic",
    "Ecuador", "Egypt", "El Salvador", "Equatorial Guinea", "Eritrea", "Estonia", "Eswatini", "Ethiopia",
    "Fiji", "Finland", "France",
    "Gabon", "Gambia", "Georgia", "Germany", "Ghana", "Greece", "Grenada", "Guatemala", "Guinea", "Guinea-Bissau", "Guyana",
    "Haiti", "Honduras", "Hungary",
    "Iceland", "India", "Indonesia", "Iran", "Iraq", "Ireland", "Israel", "Italy",
    "Jamaica", "Japan", "Jordan",
    "Kazakhstan", "Kenya", "Kiribati", "Korea, North", "Korea, South", "Kosovo", "Kuwait", "Kyrgyzstan",
    "Laos", "Latvia", "Lebanon", "Lesotho", "Liberia", "Libya", "Liechtenstein", "Lithuania", "Luxembourg",
    "Madagascar", "Malawi", "Malaysia", "Maldives", "Mali", "Malta", "Marshall Islands", "Mauritania", "Mauritius", "Mexico", "Micronesia", "Moldova", "Monaco", "Mongolia", "Montenegro", "Morocco", "Mozambique", "Myanmar",
    "Namibia", "Nauru", "Nepal", "Netherlands", "New Zealand", "Nicaragua", "Niger", "Nigeria", "North Macedonia", "Norway",
    "Oman",
    "Pakistan", "Palau", "Palestine", "Panama", "Papua New Guinea", "Paraguay", "Peru", "Philippines", "Poland", "Portugal",
    "Qatar",
    "Romania", "Russia", "Rwanda",
    "Saint Kitts and Nevis", "Saint Lucia", "Saint Vincent and the Grenadines", "Samoa", "San Marino", "Sao Tome and Principe", "Saudi Arabia", "Senegal", "Serbia", "Seychelles", "Sierra Leone", "Singapore", "Slovakia", "Slovenia", "Solomon Islands", "Somalia", "South Africa", "South Sudan", "Spain", "Sri Lanka", "Sudan", "Suriname", "Sweden", "Switzerland", "Syria",
    "Taiwan", "Tajikistan", "Tanzania", "Thailand", "Timor-Leste", "Togo", "Tonga", "Trinidad and Tobago", "Tunisia", "Turkey", "Turkmenistan", "Tuvalu",
    "Uganda", "Ukraine", "United Arab Emirates", "United Kingdom", "United States", "USA", "UK", "Uruguay", "Uzbekistan",
    "Vanuatu", "Vatican City", "Venezuela", "Vietnam",
    "Yemen",
    "Zambia", "Zimbabwe"
]

def extract_locations_fallback(text: str) -> list[str]:
    """Lightweight regex-based country extractor for when SpaCy is unavailable."""
    text_clean = _clean(text)
    locations = []
    for country in _COMMON_COUNTRIES:
        # Use word boundaries to avoid partial matches
        if re.search(r'\b' + re.escape(country) + r'\b', text_clean, re.IGNORECASE):
            locations.append(country)
    return locations

# ---------------------------------------------------------------------------
# Named Entity Recognition — location extraction
# ---------------------------------------------------------------------------
def extract_locations(text: str) -> list[str]:
    """Return unique GPE / LOC entity strings found in *text*."""
    if _nlp is None:
        logger.debug("NER: SpaCy not loaded. Using fallback country extractor.")
        return extract_locations_fallback(text)

    doc = _nlp(_clean(text))
    seen: set[str] = set()
    locations: list[str] = []
    for ent in doc.ents:
        if ent.label_ in {"GPE", "LOC"}:
            name = ent.text.strip()
            if name and name not in seen:
                seen.add(name)
                locations.append(name)
    
    # If SpaCy found nothing, still try the fallback as a safety net
    if not locations:
        locations = extract_locations_fallback(text)
        
    return locations


# ---------------------------------------------------------------------------
# Sentiment analysis — VADER
# ---------------------------------------------------------------------------
def analyze_sentiment(text: str) -> dict[str, Any]:
    """Run VADER sentiment analysis on *text*.

    Returns
    -------
    dict with keys:
        score (float)  — VADER compound score in [-1, +1].
        label (str)    — "Positive" | "Neutral" | "Negative".
        details (dict) — full VADER scores {neg, neu, pos, compound}.
    """
    if _vader is None:
        raise RuntimeError("Models not loaded. Call load_models() first.")

    scores = _vader.polarity_scores(_clean(text))
    compound = scores["compound"]

    if compound >= 0.05:
        label = "Positive"
    elif compound <= -0.05:
        label = "Negative"
    else:
        label = "Neutral"

    return {"score": round(compound, 4), "label": label, "details": scores}


# ---------------------------------------------------------------------------
# Geocoding cache
# ---------------------------------------------------------------------------
class GeoCache:
    """Thread-safe, JSON-backed local cache for geocoding lookups.

    The cache file lives at *path* (default: ``<project_root>/geocoding_cache.json``).
    Each entry maps a place name (lower-cased) to ``{"lat": float, "lon": float}``.
    """

    def __init__(self, path: Path = _CACHE_FILE) -> None:
        self._path = path
        self._lock = Lock()
        self._data: dict[str, dict] = self._load()

    # ------------------------------------------------------------------
    def _load(self) -> dict:
        if self._path.exists():
            try:
                with open(self._path, "r", encoding="utf-8") as f:
                    return json.load(f)
            except (json.JSONDecodeError, OSError) as exc:
                logger.warning("Could not read geo cache (%s) — starting fresh.", exc)
        return {}

    def _save(self) -> None:
        """Atomic write via a temp file."""
        tmp = self._path.with_suffix(".tmp")
        try:
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump(self._data, f, indent=2)
            os.replace(tmp, self._path)
        except OSError as exc:
            logger.warning("Could not write geo cache: %s", exc)

    # ------------------------------------------------------------------
    def get(self, place: str) -> dict | None:
        """Return cached ``{"lat": ..., "lon": ...}`` for *place*, or None."""
        with self._lock:
            return self._data.get(place.lower())

    def set(self, place: str, lat: float, lon: float) -> None:
        """Store geocoding result and persist to disk."""
        with self._lock:
            self._data[place.lower()] = {"lat": lat, "lon": lon}
            self._save()

    def __len__(self) -> int:
        with self._lock:
            return len(self._data)


# Module-level singleton cache instance
_geo_cache = GeoCache()


# ---------------------------------------------------------------------------
# Geocoding — Nominatim (OpenStreetMap)
# ---------------------------------------------------------------------------
# Nominatim usage policy: max 1 request/second
_NOMINATIM_DELAY = 1.1  # seconds between calls
_last_request_time: float = 0.0
_geocoder = None


def _get_geocoder():
    """Lazy-initialise the Nominatim geocoder."""
    global _geocoder
    if _geocoder is None:
        from geopy.geocoders import Nominatim  # noqa: PLC0415
        _geocoder = Nominatim(user_agent="coriolis_student_project_v1")
    return _geocoder


def geocode(place: str) -> dict | None:
    """Geocode *place* to ``{"lat": float, "lon": float}`` or None.

    Checks the local cache first. On a cache miss, calls Nominatim and
    respects the 1 req/s rate limit.

    Parameters
    ----------
    place:
        City/country name extracted by NER.

    Returns
    -------
    dict or None
    """
    global _last_request_time

    if not place or not place.strip():
        return None

    # Cache hit
    cached = _geo_cache.get(place)
    if cached:
        logger.debug("Geocode cache HIT for %r", place)
        return cached

    # Rate-limit compliance
    elapsed = time.monotonic() - _last_request_time
    if elapsed < _NOMINATIM_DELAY:
        time.sleep(_NOMINATIM_DELAY - elapsed)

    geocoder = _get_geocoder()
    from geopy.exc import GeocoderUnavailable
    
    try:
        # Mandatory 1.5s delay to strictly comply with nominatim 1 req/s
        time.sleep(1.5)
        
        _last_request_time = time.monotonic()
        location = geocoder.geocode(place, timeout=10)
        if location:
            result = {"lat": round(location.latitude, 6), "lon": round(location.longitude, 6)}
            _geo_cache.set(place, result["lat"], result["lon"])
            logger.debug("Geocoded %r → %s", place, result)
            return result
        else:
            logger.debug("Nominatim returned no result for %r", place)
            return None
    except GeocoderUnavailable:
        print('Rate limited, sleeping 5s...')
        time.sleep(5)
        return None
    except Exception as exc:  # noqa: BLE001
        logger.warning("Geocoding error for %r: %s", place, exc)
        return None


# ---------------------------------------------------------------------------
# Topic Classification
# ---------------------------------------------------------------------------
_TOPIC_KEYWORDS = {
    "Food": ["food", "diet", "cooking", "restaurant", "meal", "recipe", "nutrition", "hunger", "agriculture"],
    "Tech": ["tech", "technology", "software", "hardware", "apple", "google", "microsoft", "gadget", "cyber", "cybersecurity", "startup"],
    "AI": ["ai", "artificial intelligence", "openai", "chatgpt", "llm", "generative ai", "neural network"],
    "Finance": ["finance", "market", "stock", "trading", "economy", "bank", "inflation", "investment", "crypto", "bitcoin"],
    "Machine Learning": ["machine learning", "ml", "deep learning", "algorithm", "data science"],
    "Climate": ["climate", "weather", "warming", "environment", "emissions"],
    "Politics": ["politics", "election", "government", "president", "minister", "parliament"],
}

import re

def classify_topic(text: str, default_topic: str | None = None, source: str | None = None) -> str:
    """Classify text into a known topic category using keyword matching and explicit hashtags."""
    text_lower = text.lower()
    
    # 1. Explicit Hashtag Grabbing First
    hashtags = re.findall(r'#([a-zA-Z0-9_]+)', text_lower)
    for tag in hashtags:
        for topic, keywords in _TOPIC_KEYWORDS.items():
            if tag in keywords or tag == topic.lower():
                return topic

    # 2. Keyword matching
    max_matches = 0
    best_topic = None
    for topic, keywords in _TOPIC_KEYWORDS.items():
        matches = sum(1 for kw in keywords if kw in text_lower)
        if matches > max_matches:
            max_matches = matches
            best_topic = topic
            
    if best_topic and max_matches > 0:
        return best_topic
        
    # 3. Strict Fallback Rule
    if isinstance(source, str) and source.lower() == 'mastodon':
        return "Social"
    return "Politics"

# ---------------------------------------------------------------------------
# Main orchestrator
# ---------------------------------------------------------------------------
def process_post(post: dict) -> dict:
    """Process a single post dict and return enriched results.

    Expected input keys (as produced by the ingestion layer):
        text          (str, required)
        raw_location  (str | None)   — pre-existing raw location hint
        source        (str | None)
        timestamp     (str | None)

    Returns
    -------
    dict with keys:
        extracted_locations  list[str]
        geocoded_location    str | None   (first successfully geocoded place)
        latitude             float | None
        longitude            float | None
        sentiment_score      float
        sentiment_label      str
        sentiment_details    dict
    """
    text: str = post.get("text", "") or ""

    # --- 1. NER ---
    locations = extract_locations(text)

    # Also check raw_location from source as a bonus hint
    raw_loc: str | None = post.get("raw_location")
    if raw_loc and raw_loc.strip():
        # Geocode the raw location hint even if NER found nothing
        if raw_loc.strip() not in locations:
            locations.insert(0, raw_loc.strip())

    # --- 2. Geocoding (try each extracted location in order) ---
    geocoded_place: str | None = None
    lat: float | None = None
    lon: float | None = None

    for place in locations:
        result = geocode(place)
        if result:
            geocoded_place = place
            lat = result["lat"]
            lon = result["lon"]
            break  # use the first successfully geocoded entity

    # --- 3. Sentiment ---
    sentiment = analyze_sentiment(text)

    # --- 4. Topic Classification ---
    topic = classify_topic(text, post.get("topic"), post.get("source"))

    return {
        "topic": topic,
        "extracted_locations": locations,
        "geocoded_location": geocoded_place,
        "latitude": lat,
        "longitude": lon,
        "sentiment_score": sentiment["score"],
        "sentiment_label": sentiment["label"],
        "sentiment_details": sentiment["details"],
    }


# ---------------------------------------------------------------------------
# CLI Entry Point
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    import sys
    from pathlib import Path

    # Ensure the parent directory (project root) is in the Python path
    # so we can import 'processing.worker' and 'database.models'
    project_root = str(Path(__file__).resolve().parent.parent)
    if project_root not in sys.path:
        sys.path.insert(0, project_root)

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
    )
    from processing.worker import run_worker
    
    logger.info("Starting NLP Processor directly (5s polling interval)…")
    try:
        run_worker(poll_interval=5)
    except KeyboardInterrupt:
        logger.info("NLP Processor stopped by user.")
