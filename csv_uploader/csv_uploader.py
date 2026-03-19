"""
CLI uploader for canonical analyzer CSV to ingestion API
The uploader detects new exports and pushes them to
POST /v1/ingestions on a schedule
"""

import argparse
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


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Watch a folder for analyzer CSVs and upload them to the ingestion API. "
            "Supports a one-shot mode for benchmarks."
        )
    )
    parser.add_argument(
        "--once",
        action="store_true",
        help="Process current CSV files once, then exit.",
    )
    parser.add_argument(
        "--watch-dir",
        default=str(WATCH_DIR),
        help="Directory to scan for pending CSV files.",
    )
    parser.add_argument(
        "--processed-dir",
        default=str(PROCESSED_DIR),
        help="Directory to move successfully uploaded CSV files.",
    )
    parser.add_argument(
        "--failed-dir",
        default=str(FAILED_DIR),
        help="Directory to move failed uploads.",
    )
    parser.add_argument(
        "--poll-interval-seconds",
        type=int,
        default=POLL_INTERVAL_SECONDS,
        help="Seconds to wait between polls when watching.",
    )
    parser.add_argument(
        "--stability-delay-seconds",
        type=int,
        default=STABILITY_DELAY_SECONDS,
        help="Seconds a file must be unchanged before uploading (0 disables).",
    )
    parser.add_argument(
        "--timeout-seconds",
        type=int,
        default=REQUEST_TIMEOUT_SECONDS,
        help="HTTP request timeout in seconds.",
    )
    parser.add_argument(
        "--max-upload-retries",
        type=int,
        default=MAX_UPLOAD_RETRIES,
        help="Max retry attempts for connection/timeout errors.",
    )
    parser.add_argument(
        "--retry-backoff-seconds",
        type=int,
        default=RETRY_BACKOFF_SECONDS,
        help="Base backoff seconds; attempt N sleeps backoff*N.",
    )
    parser.add_argument(
        "--debug-request",
        action="store_true",
        help="Print request details (URL + form fields).",
    )
    parser.add_argument(
        "--keep-files",
        action="store_true",
        help=(
            "Do not move files after upload attempt (useful for benchmark fixtures). "
            "Combine with --once to avoid re-uploading in a loop."
        ),
    )
    return parser.parse_args()


def read_config() -> dict:
    """Reads configuration data from a YAML file."""
    with open(CONFIG_FILE_PATH, "r") as file:
        config_data = yaml.safe_load(file)
        return config_data


def process_file(
    *,
    csv_path: Path,
    config: dict,
    session: requests.Session,
    processed_dir: Path,
    failed_dir: Path,
    stability_delay_seconds: int,
    request_timeout_seconds: int,
    max_upload_retries: int,
    retry_backoff_seconds: int,
    debug_request: bool,
    keep_files: bool,
) -> None:
    """Processes a single CSV file: computes hash, uploads, and moves it."""
    print(f"Processing {csv_path.name}...")

    # Ensure file is stable
    if stability_delay_seconds > 0:
        while True:
            last_modified_time = csv_path.stat().st_mtime
            time_since_last_modification = time.time() - last_modified_time
            if time_since_last_modification > stability_delay_seconds:
                print(
                    f"File {csv_path.name} is stable (age: {time_since_last_modification:.2f}s)."
                )
                break
            print(
                f"File {csv_path.name} is too new (age: {time_since_last_modification:.2f}s). Waiting..."
            )
            time.sleep(stability_delay_seconds)

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

            if debug_request:
                print("--- Preparing HTTP Request ---")
                print(f"URL: POST {url}")
                print(f"Data: {data}")
                print(f"Files: {files['file'][0]}")
                print("-----------------------------")

            print(f"Uploading to {url}...")

            response = None
            for attempt in range(1, max_upload_retries + 1):
                try:
                    # Ensure file content is replayable for retries
                    f.seek(0)
                    response = session.post(
                        url,
                        files=files,
                        data=data,
                        timeout=request_timeout_seconds,
                    )
                    break
                except (RequestsConnectionError, RequestsTimeout) as e:
                    print(
                        f"Upload attempt {attempt}/{max_upload_retries} failed: {e}"
                    )
                    if attempt < max_upload_retries:
                        time.sleep(retry_backoff_seconds * attempt)
                    else:
                        # Keep the file in pending so it can be retried on the next poll.
                        print(
                            f"API not reachable; leaving {csv_path.name} in pending for retry."
                        )
                        return
                except RequestException as e:
                    # Non-connection related requests failures are treated as failures.
                    print(f"Request failed: {e}")
                    failed_dir.mkdir(parents=True, exist_ok=True)
                    csv_path.rename(failed_dir / csv_path.name)
                    print(f"Moved to {failed_dir}")
                    return

        assert response is not None

        print(f"API Response: {response.status_code}")
        # response.ok means that response code is < 400
        if response.ok:
            print(f"Response: {response.json()}")
            # Move file to processed directory on successful upload
            if keep_files:
                print("Keeping file in place (--keep-files).")
            else:
                processed_dir.mkdir(parents=True, exist_ok=True)
                csv_path.rename(processed_dir / csv_path.name)
                print(f"Moved to {processed_dir}")
        else:
            print(f"Error: {response.text}")
            if keep_files:
                print("Keeping file in place (--keep-files).")
            else:
                failed_dir.mkdir(parents=True, exist_ok=True)
                csv_path.rename(failed_dir / csv_path.name)
                print(f"Moved to {failed_dir}")

    except IOError as e:
        print(f"Error processing file {csv_path}: {e}")
        if keep_files:
            print("Keeping file in place (--keep-files).")
        else:
            failed_dir.mkdir(parents=True, exist_ok=True)
            csv_path.rename(failed_dir / csv_path.name)
            print(f"Moved to {failed_dir}")


def main() -> None:
    try:
        args = _parse_args()
        config = read_config()

        watch_dir = Path(args.watch_dir)
        processed_dir = Path(args.processed_dir)
        failed_dir = Path(args.failed_dir)

        # Reuse connections across many uploads (important for batch benchmarks)
        session = requests.Session()

        print(f"Watching for new CSV files in {watch_dir}...")

        while True:
            csv_files = sorted(watch_dir.glob("*.csv"))
            if not csv_files:
                if args.once:
                    return

                time.sleep(args.poll_interval_seconds)
                continue

            for csv_path in csv_files:
                process_file(
                    csv_path=csv_path,
                    config=config,
                    session=session,
                    processed_dir=processed_dir,
                    failed_dir=failed_dir,
                    stability_delay_seconds=args.stability_delay_seconds,
                    request_timeout_seconds=args.timeout_seconds,
                    max_upload_retries=args.max_upload_retries,
                    retry_backoff_seconds=args.retry_backoff_seconds,
                    debug_request=args.debug_request,
                    keep_files=args.keep_files,
                )

            if args.once:
                return

            time.sleep(args.poll_interval_seconds)
    except KeyboardInterrupt:
        print("\nWatcher stopped.")
    except FileNotFoundError:
        print(f"Error: Configuration file not found at {CONFIG_FILE_PATH}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")


if __name__ == "__main__":
    main()
