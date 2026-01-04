import json
import os
import sqlite3
import time
from pathlib import Path

import googlemaps
import pandas as pd
from dotenv import load_dotenv
from tqdm import tqdm

# --- CONFIGURATION ---
ROOT_DIR = Path(__file__).resolve().parent.parent

INPUT_FILE: Path = ROOT_DIR / "data" / "voting_centers.csv"
DB_FILE: Path = ROOT_DIR / "data" / "databases" / "google_geocoding.db"
TABLE_NAME = "raw_responses"
COUNTRY_MAP: dict[str, str] = {"HND": "HN", "USA": "US"}

# Load Environment Variables
load_dotenv()
API_KEY = os.getenv("GOOGLE_API_KEY")

if not API_KEY:
    raise ValueError("No API Key found. Please check your .env file.")

# Initialize Client
gmaps = googlemaps.Client(key=API_KEY)


def setup_database():
    """Creates the SQLite table if it doesn't exist."""
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()

    c.execute(f"""
        CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
            center_id TEXT PRIMARY KEY,
            geo_query TEXT,
            full_response JSON,
            status TEXT,
            timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
        )
    """)
    conn.commit()
    return conn


def get_existing_ids(conn):
    """Returns a set of center_ids that are already in the database."""
    c = conn.cursor()
    c.execute(f"SELECT center_id FROM {TABLE_NAME}")
    return set(row[0] for row in c.fetchall())


def save_to_db(conn, center_id, geo_query, response, status):
    """Inserts a new record into the database."""
    c = conn.cursor()
    c.execute(
        f"INSERT INTO {TABLE_NAME} (center_id, geo_query, full_response, status) VALUES (?, ?, ?, ?)",
        (str(center_id), geo_query, json.dumps(response), status),
    )
    conn.commit()


def run_fetcher():
    # 1. Load Data
    df = pd.read_csv(INPUT_FILE, dtype={"voting_center_id": str})

    # 2. Setup DB
    os.makedirs(os.path.dirname(DB_FILE), exist_ok=True)
    conn = setup_database()
    existing_ids = get_existing_ids(conn)

    print(f"Total rows to process: {len(df)}")
    print(f"Already cached: {len(existing_ids)}")

    # 3. Iterate
    processed_count = 0
    for index, row in tqdm(df.iterrows(), total=df.shape[0], desc="Fetching Geocodes"):
        center_id = row["voting_center_id"]
        address = row["geo_query"]
        iso3 = row.get("country_code", "HND")
        country_filter = COUNTRY_MAP.get(iso3, "HN")

        # Skip if already in DB
        if center_id in existing_ids:
            continue

        try:
            # Rate limiting - simple sleep
            time.sleep(0.1)

            # API Call
            response = gmaps.geocode(address, components={"country": country_filter})
            status = "OK" if response else "ZERO_RESULTS"

            # Save raw data
            save_to_db(conn, center_id, address, response, status)
            processed_count += 1

        except Exception as e:
            print(f"Error on {center_id}: {e}")
            # Save error state so we don't retry endlessly
            save_to_db(conn, center_id, address, [], "ERROR")

    conn.close()
    print(f"\nFetcher finished. New records added: {processed_count}")


if __name__ == "__main__":
    run_fetcher()
