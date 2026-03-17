"""
CLI uploader for canonical analyzer CSV to ingestion API
The uploader detects new exports and pushes them to
POST /v1/ingestions on a schedule
"""

import argparse
import os
import requests
import time
import hashlib
from pathlib import Path
from datetime import datetime
import yaml

from requests.exceptions import ConnectionError as RequestsConnectionError
from requests.exceptions import Timeout as RequestsTimeout
from requests.exceptions import RequestException

WATCH_DIR = Path("csv_uploader/simulated_exports/pending")
PROCESSED_DIR = Path("csv_uploader/simulated_exports/uploaded")
FAILED_DIR = Path("csv_uploader/simulated_exports/failed")
CONFIG_FILE_PATH = Path("csv_uploader/config.yaml")

STABILITY_DELAY_SECONDS = 10  # Time to wait for file to be stable
POLL_INTERVAL_SECONDS = 20
CSV_POST_API_ENDPOINT = "/v1/ingestions"
UPLOADER_ID = "uploader_001"
REQUEST_TIMEOUT_SECONDS = 30
MAX_UPLOAD_RETRIES = 3
RETRY_BACKOFF_SECONDS = 3


def read_config() -> dict:
    """Reads configuration data from a YAML file."""
    with open(CONFIG_FILE_PATH, "r") as file:
        config_data = yaml.safe_load(file)
        return config_data


def process_file(csv_path: Path, config: dict) -> None:
    """Processes a single CSV file: computes hash, uploads, and moves it."""
    print(f"Processing {csv_path.name}...")

    # Ensure file is stable
    while True:
        last_modified_time = csv_path.stat().st_mtime
        time_since_last_modification = time.time() - last_modified_time
        if time_since_last_modification > STABILITY_DELAY_SECONDS:
            print(
                f"File {csv_path.name} is stable (age: {time_since_last_modification:.2f}s)."
            )
            break
        print(
            f"File {csv_path.name} is too new (age: {time_since_last_modification:.2f}s). Waiting..."
        )
        time.sleep(STABILITY_DELAY_SECONDS)

    # Get current time as a string
    uploader_received_at = datetime.now().isoformat()
    # Get run_id from CSV file name
    run_id = csv_path.stem
    print(f"Extracted run id: {run_id}")
    # Compute SHA-256
    sha256_hash = hashlib.sha256()

    try:
        with open(csv_path, "rb") as f:
            # Read and update hash in chunks to handle large files
            for byte_block in iter(lambda: f.read(4096), b""):
                sha256_hash.update(byte_block)
            content_sha256 = sha256_hash.hexdigest()
            print(f"content_sha256: {content_sha256}")

            # Rewind file to be sent via request
            f.seek(0)

            # Upload the file
            url = config["api_base_url"].rstrip("/") + CSV_POST_API_ENDPOINT
            files = {"file": (csv_path.name, f, "text/csv")}
            data = {
                "uploader_id": UPLOADER_ID,
                "spec_version": config["spec_version"],
                "instrument_id": config["instrument_id"],
                "run_id": run_id,
                "content_sha256": content_sha256,
                "uploader_received_at": uploader_received_at,
            }

            # --- For testing: Print the request details ---
            print("--- Preparing HTTP Request ---")
            print(f"URL: POST {url}")
            print(f"Data: {data}")
            print(f"Files: {files['file'][0]}")
            print("-----------------------------")

            print(f"Uploading to {url}...")

            response = None
            for attempt in range(1, MAX_UPLOAD_RETRIES + 1):
                try:
                    response = requests.post(
                        url,
                        files=files,
                        data=data,
                        timeout=REQUEST_TIMEOUT_SECONDS,
                    )
                    break
                except (RequestsConnectionError, RequestsTimeout) as e:
                    print(
                        f"Upload attempt {attempt}/{MAX_UPLOAD_RETRIES} failed: {e}"
                    )
                    if attempt < MAX_UPLOAD_RETRIES:
                        time.sleep(RETRY_BACKOFF_SECONDS * attempt)
                    else:
                        # Keep the file in pending so it can be retried on the next poll.
                        print(
                            f"API not reachable; leaving {csv_path.name} in pending for retry."
                        )
                        return
                except RequestException as e:
                    # Non-connection related requests failures are treated as failures.
                    print(f"Request failed: {e}")
                    FAILED_DIR.mkdir(exist_ok=True)
                    csv_path.rename(FAILED_DIR / csv_path.name)
                    print(f"Moved to {FAILED_DIR}")
                    return

        assert response is not None

        print(f"API Response: {response.status_code}")
        # response.ok means that response code is < 400
        if response.ok:
            print(f"Response: {response.json()}")
            # Move file to processed directory on successful upload
            PROCESSED_DIR.mkdir(exist_ok=True)
            csv_path.rename(PROCESSED_DIR / csv_path.name)
            print(f"Moved to {PROCESSED_DIR}")
        else:
            print(f"Error: {response.text}")
            FAILED_DIR.mkdir(exist_ok=True)
            csv_path.rename(FAILED_DIR / csv_path.name)
            print(f"Moved to {FAILED_DIR}")

    except IOError as e:
        print(f"Error processing file {csv_path}: {e}")
        FAILED_DIR.mkdir(exist_ok=True)
        csv_path.rename(FAILED_DIR / csv_path.name)
        print(f"Moved to {FAILED_DIR}")


def main() -> None:
    try:
        config = read_config()

        print(f"Watching for new CSV files in {WATCH_DIR}...")

        while True:
            csv_files = sorted(WATCH_DIR.glob("*.csv"))
            if not csv_files:
                time.sleep(POLL_INTERVAL_SECONDS)
                continue

            for csv_path in csv_files:
                process_file(csv_path, config)

            time.sleep(POLL_INTERVAL_SECONDS)
    except KeyboardInterrupt:
        print("\nWatcher stopped.")
    except FileNotFoundError:
        print(f"Error: Configuration file not found at {CONFIG_FILE_PATH}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")


if __name__ == "__main__":
    main()
