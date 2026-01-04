import json
import sqlite3
from pathlib import Path

import pandas as pd
from tqdm import tqdm

# --- CONFIGURATION ---
ROOT_DIR: Path = Path(__file__).resolve().parent.parent

DB_FILE: Path = ROOT_DIR / "data" / "databases" / "google_geocoding.db"
TABLE_NAME = "raw_responses"
OUTPUT_FILE: Path = ROOT_DIR / "data" / "voting_centers_geocoded"


def parse_google_response(response_json):
    """
    Extracts fields from the Google JSON list.
    Accepts raw JSON (list of dicts) or parsed list.
    """
    # Ensure we have a list (if it came from DB as parsed JSON)
    if isinstance(response_json, str):
        response = json.loads(response_json)
    else:
        response = response_json

    if not response:
        return {}

    result = response[0]

    # 1. Basic Geometry & ID
    data = {
        "lat": result.get("geometry", {}).get("location", {}).get("lat"),
        "lng": result.get("geometry", {}).get("location", {}).get("lng"),
        "place_id": result.get("place_id"),
        "types": json.dumps(result.get("types", [])),
        "formatted_address": result.get("formatted_address"),
    }

    # 2. Extract Plus Code
    plus_code_value = None

    # Strategy A: Root
    if "plus_code" in result:
        pc = result["plus_code"]
        if isinstance(pc, dict):
            plus_code_value = pc.get("global_code") or pc.get("compound_code")
        else:
            plus_code_value = str(pc)

    # Strategy B: Address Components
    if not plus_code_value:
        for component in result.get("address_components", []):
            if "plus_code" in component.get("types", []):
                plus_code_value = component.get("long_name")
                break

    data["plus_code"] = plus_code_value

    # 3. Component Mapping
    target_components = {
        "sublocality_level_1": "sublocality_level_1",
        "locality": "locality",
        "administrative_area_level_2": "administrative_area_level_2",
        "administrative_area_level_1": "administrative_area_level_1",
        "country": "country",
    }

    # Initialize None
    for field in target_components:
        if field not in data:
            data[field] = None

    # Fill Matches
    for component in result.get("address_components", []):
        types = component.get("types", [])
        for my_field, google_type in target_components.items():
            if google_type in types:
                data[my_field] = component.get("long_name")

    return data


def run_parser():
    conn = sqlite3.connect(DB_FILE)

    # Read everything from DB
    print("Reading data from database...")
    query = f"SELECT center_id, geo_query, full_response, status FROM {TABLE_NAME}"
    df_raw = pd.read_sql_query(query, conn)
    conn.close()

    print(f"Parsing {len(df_raw)} records...")

    parsed_rows = []

    for _, row in tqdm(df_raw.iterrows(), total=df_raw.shape[0]):
        # Base info from DB
        row_data = {
            "voting_center_id": row["center_id"],
            "geo_query": row["geo_query"],
            "geocoding_status": row["status"],
        }

        # Parse the JSON blob
        extracted_data = parse_google_response(row["full_response"])

        # Merge dicts
        row_data.update(extracted_data)
        parsed_rows.append(row_data)

    # Create final DataFrame
    final_df = pd.DataFrame(parsed_rows)

    # Save as CSV
    final_df.to_csv(f"{OUTPUT_FILE}.csv", index=False)
    print(f"Success! Parsed data saved to {OUTPUT_FILE}.csv")

    final_df.to_parquet(f"{OUTPUT_FILE}.parquet", index=False)
    print(f"Success! Parsed data saved to {OUTPUT_FILE}.parquet")


if __name__ == "__main__":
    run_parser()
