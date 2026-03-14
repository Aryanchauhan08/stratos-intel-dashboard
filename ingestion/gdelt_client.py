"""
ingestion/gdelt_client.py
--------------------------
Fetch the latest 15-minute GDELT Global Knowledge Graph (GKG) v2.1 update.

GDELT publishes a fresh index every 15 minutes at:
  http://data.gdeltproject.org/gdeltv2/lastupdate.txt

The third line of that file contains the GKG download URL of the form:
  <bytes> <md5> http://data.gdeltproject.org/gdeltv2/{YYYYMMDDHHMMSS}.gkg.csv.zip

This module:
  1. Fetches lastupdate.txt to discover the current GKG file URL.
  2. Downloads the ZIP in-memory.
  3. Extracts the CSV and loads it with pandas.
  4. Returns a cleaned DataFrame with the most useful columns.

No authentication is required. Respecting the 15-minute cadence avoids
unnecessary server load.

Verified against live lastupdate.txt on 2026-03-06:
  URL format confirmed as http://data.gdeltproject.org/gdeltv2/{TS}.gkg.csv.zip
"""

from __future__ import annotations

import io
import logging
import zipfile
import time
from datetime import datetime
from typing import Optional

import pandas as pd
import requests

from database.models import SocialActivity, SessionLocal

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
LASTUPDATE_URL = "http://data.gdeltproject.org/gdeltv2/lastupdate.txt"

# GKG 2.1 column definitions (partial — the columns we care about)
# Full spec: http://data.gdeltproject.org/documentation/GDELT-Global_Knowledge_Graph_Codebook-V2.1.pdf
GKG_COLUMNS = [
    "GKGRECORDID",       #  0 — unique record ID
    "DATE",              #  1 — YYYYMMDDHHMMSS publication date
    "SourceCollectionIdentifier",  # 2
    "SourceCommonName",  #  3 — human-readable source name
    "DocumentIdentifier",#  4 — URL of the source article
    "V1Counts",          #  5
    "V21Counts",         #  6
    "V1Themes",          #  7 — semicolon-separated CAMEO themes
    "V2EnhancedThemes",  #  8
    "V1Locations",       #  9 — semicolon-separated location blocks
    "V2EnhancedLocations",# 10
    "V1Persons",         # 11
    "V2EnhancedPersons", # 12
    "V1Organizations",   # 13
    "V2EnhancedOrganizations",# 14
    "V15Tone",           # 15 — legacy tone field
    "V21EnhancedDates",  # 16
    "V2GCAM",            # 17
    "V21SharingImage",   # 18
    "V21RelatedImages",  # 19
    "V21SocialImageEmbeds",  # 20
    "V21SocialVideoEmbeds",  # 21
    "V21Quotations",     # 22
    "V21AllNames",       # 23
    "V21Amounts",        # 24
    "V21TranslationInfo",# 25
    "V2ExtrasXML",       # 26
]

# Focused subset we expose downstream
USEFUL_COLUMNS = [
    "GKGRECORDID",
    "DATE",
    "SourceCommonName",
    "DocumentIdentifier",
    "V1Themes",
    "V1Locations",
    "V15Tone",
]


# ---------------------------------------------------------------------------
# URL discovery
# ---------------------------------------------------------------------------

def get_latest_gkg_url() -> str:
    """
    Fetch ``lastupdate.txt`` and return the URL of the current GKG file.

    Raises
    ------
    RuntimeError
        If the URL cannot be discovered (network error or unexpected format).
    """
    logger.info("Fetching GDELT last-update index: %s", LASTUPDATE_URL)
    try:
        resp = requests.get(LASTUPDATE_URL, timeout=30)
        resp.raise_for_status()
    except requests.RequestException as exc:
        raise RuntimeError(f"Failed to fetch lastupdate.txt: {exc}") from exc

    lines = resp.text.strip().splitlines()
    if len(lines) < 3:
        raise RuntimeError(
            f"Unexpected lastupdate.txt format — got {len(lines)} line(s):\n{resp.text}"
        )

    # Third line → "<bytes> <md5> <url>"
    gkg_line = lines[2]
    parts = gkg_line.split()
    if len(parts) < 3:
        raise RuntimeError(f"Cannot parse GKG line: {gkg_line!r}")

    url = parts[2]
    logger.info("Latest GKG file: %s", url)
    return url


# ---------------------------------------------------------------------------
# Download & parse
# ---------------------------------------------------------------------------

def _download_and_unzip(url: str) -> bytes:
    """Download a ZIP from *url* and return the raw bytes of the first member."""
    logger.info("Downloading GKG ZIP: %s", url)
    resp = requests.get(url, timeout=120, stream=True)
    resp.raise_for_status()

    raw = io.BytesIO(resp.content)
    with zipfile.ZipFile(raw) as zf:
        names = zf.namelist()
        logger.info("ZIP members: %s", names)
        return zf.read(names[0])


def _parse_gkg_csv(data: bytes, max_rows: Optional[int] = None) -> pd.DataFrame:
    """
    Parse raw GKG CSV bytes into a DataFrame.

    GDELT GKG files are tab-separated with no header row.
    """
    buf = io.BytesIO(data)
    kwargs: dict = {
        "sep": "\t",
        "header": None,
        "names": GKG_COLUMNS,
        "on_bad_lines": "skip",
        "low_memory": False,
    }
    if max_rows:
        kwargs["nrows"] = max_rows

    df = pd.read_csv(buf, **kwargs)
    logger.info("Parsed GKG CSV — shape: %s", df.shape)

    # Keep only the useful columns that actually exist in this file
    existing = [c for c in USEFUL_COLUMNS if c in df.columns]
    return df[existing].copy()


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def fetch_latest_gkg(
    max_rows: Optional[int] = 200,
    gkg_url: Optional[str] = None,
) -> pd.DataFrame:
    """
    Download and return the latest GDELT GKG 15-minute update as a DataFrame.

    Parameters
    ----------
    max_rows : int, optional
        Limit the number of rows read from the CSV.  ``None`` reads all rows.
        Defaults to 200 to keep startup time reasonable.
    gkg_url : str, optional
        Override the auto-discovered GKG URL (useful for testing with a
        specific historical file).

    Returns
    -------
    pd.DataFrame
        Columns: GKGRECORDID, DATE, SourceCommonName, DocumentIdentifier,
        V1Themes, V1Locations, V15Tone.
    """
    url = gkg_url or get_latest_gkg_url()
    raw_csv = _download_and_unzip(url)
    df = _parse_gkg_csv(raw_csv, max_rows=max_rows)
    return df


def gkg_row_to_activity(row: pd.Series) -> dict:
    """
    Convert a single GKG DataFrame row into a SocialActivity-shaped dict.

    Location parsing: GDELT V1Locations format is semicolon-separated
    blocks of ``type#fullname#countrycode#adm1#lat#lon#featureId``.
    We extract the lat/lon from the first location block when present.
    """
    lat: Optional[float] = None
    lon: Optional[float] = None
    raw_location: Optional[str] = None

    loc_field = str(row.get("V1Locations", "") or "")
    if loc_field:
        first_block = loc_field.split(";")[0]
        parts = first_block.split("#")
        if len(parts) >= 7:
            raw_location = parts[1] if parts[1] else None
            try:
                lat = float(parts[4]) if parts[4] else None
                lon = float(parts[5]) if parts[5] else None
            except (ValueError, IndexError):
                pass

    themes_raw = str(row.get("V1Themes", "") or "")
    keywords = [t.strip() for t in themes_raw.split(";") if t.strip()]

    date_str = str(row.get("DATE", ""))
    timestamp: Optional[str] = None
    if len(date_str) == 14:
        # YYYYMMDDHHMMSS → ISO 8601
        timestamp = (
            f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}"
            f"T{date_str[8:10]}:{date_str[10:12]}:{date_str[12:14]}Z"
        )

    return {
        "source": "gdelt",
        "text": str(row.get("DocumentIdentifier", "")),
        "timestamp": timestamp,
        "raw_location": raw_location,
        "latitude": lat,
        "longitude": lon,
        "keywords": keywords,
        "_meta": {
            "gkg_record_id": str(row.get("GKGRECORDID", "")),
            "source_name": str(row.get("SourceCommonName", "")),
            "tone": str(row.get("V15Tone", "")),
        },
    }


# ---------------------------------------------------------------------------
# Quick smoke test
# ---------------------------------------------------------------------------
def run_gdelt_ingestion_loop(poll_interval: int = 900, max_rows: int = 100):
    """
    Run an infinite loop that polls GDELT GKG every `poll_interval` seconds.
    """
    logger.info("Starting GDELT ingestion loop (interval=%ds)...", poll_interval)
    while True:
        db = SessionLocal()
        try:
            logger.info("Fetching latest GDELT GKG update…")
            df = fetch_latest_gkg(max_rows=max_rows)
            
            inserted_count = 0
            for _, row in df.iterrows():
                record = gkg_row_to_activity(row)
                
                if not record.get("text"):
                    continue

                timestamp_str = record.get("timestamp")
                dt = datetime.fromisoformat(timestamp_str.replace("Z", "+00:00")) if timestamp_str else None
                
                activity = SocialActivity(
                    source="gdelt",
                    text=record["text"],
                    timestamp=dt,
                    raw_location=record.get("raw_location"),
                    latitude=record.get("latitude"),
                    longitude=record.get("longitude"),
                    keywords=record.get("keywords") or [],
                    status="pending"
                )
                db.add(activity)
                inserted_count += 1
            
            try:
                db.commit()
            except Exception as e:
                db.rollback()
                print(f"CRITICAL DB SAVE ERROR (GDELT): {e}")
                logger.error(f"Critical SQL error during GDELT commit: {e}")
            
            if inserted_count > 0:
                logger.info("SUCCESS: Processed %d GDELT articles", inserted_count)
            else:
                logger.info("No valid records found in this GDELT batch.")

        except Exception as exc:
            logger.error("GDELT polling loop encountered an error: %s", exc)
            db.rollback()
        finally:
            db.close()
        
        logger.info("GDELT cycle complete. Sleeping for %ds…", poll_interval)
        time.sleep(poll_interval)


if __name__ == "__main__":
    import sys
    from pathlib import Path
    import time
    from datetime import datetime

    # Ensure project root is in path so we can import 'database.models'
    project_root = str(Path(__file__).resolve().parent.parent)
    if project_root not in sys.path:
        sys.path.insert(0, project_root)

    from database.models import SocialActivity, SessionLocal

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
    )

    print("Connecting to GDELT GKG endpoint — polling every 15 minutes (900s).\n")

    try:
        run_gdelt_ingestion_loop()
    except KeyboardInterrupt:
        print("\nStopped by user.")
