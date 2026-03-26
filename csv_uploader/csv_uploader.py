"""
CLI uploader for canonical analyzer CSV to ingestion API
The uploader detects new exports and pushes them to
POST /v1/ingestions on a schedule
"""

import argparse
import requests
import time
import hashlib
import os
import random
from pathlib import Path
from datetime import datetime
from typing import Any
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
INGESTION_GET_API_ENDPOINT = "/v1/ingestions/{ingestion_id}"
UPLOADER_ID = "uploader_001"
REQUEST_TIMEOUT_SECONDS = 30
MAX_UPLOAD_RETRIES = 3
RETRY_BACKOFF_SECONDS = 3


def _parse_retry_after_seconds(response: requests.Response) -> float | None:
    raw = response.headers.get("Retry-After")
    if raw is None:
        return None
    raw = raw.strip()
    if not raw:
        return None
    try:
        # Most servers provide integer seconds.
        return float(raw)
    except ValueError:
        return None


def _sleep_seconds_for_429(
    *,
    response: requests.Response,
    attempt: int,
    retry_backoff_seconds: int,
    max_sleep_seconds: float,
) -> float:
    """Compute a short delay for 429 retries.

    - Prefer server-provided Retry-After when present.
    - Clamp to max_sleep_seconds to avoid slowing down short benchmark runs.
    - Add jitter to reduce synchronized thundering herds.
    """

    retry_after = _parse_retry_after_seconds(response)
    if retry_after is not None:
        base = max(0.0, retry_after)
    else:
        # Keep this fast: benchmark runs can be single-digit seconds.
        base = min(0.25, float(retry_backoff_seconds))

    # A gentle attempt-based increase, but still capped.
    base = min(float(max_sleep_seconds), base + 0.05 * max(0, attempt - 1))
    jitter = random.uniform(0.0, min(0.05, base))
    return min(float(max_sleep_seconds), base + jitter)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Watch a folder for analyzer CSVs and upload them to the ingestion API. "
            "Supports one-shot and single-file modes for benchmarks."
        )
    )
    parser.add_argument(
        "--file",
        action="append",
        default=None,
        help=(
            "Upload a specific CSV file path. Can be provided multiple times. "
            "If set, the uploader will process only these file(s) once and exit."
        ),
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
        "--wait-for-terminal",
        action="store_true",
        help=(
            "After uploading file(s), poll ingestion status until each reaches a "
            "terminal state, then exit. Useful for end-to-end batch timing."
        ),
    )
    parser.add_argument(
        "--status-poll-seconds",
        type=int,
        default=2,
        help="Polling interval in seconds when waiting for terminal status.",
    )
    parser.add_argument(
        "--batch-results-csv",
        default=None,
        help=(
            "If set, append one aggregate 'batch' row to this results CSV after all "
            "uploads reach terminal status (requires --wait-for-terminal)."
        ),
    )
    parser.add_argument(
        "--batch-id",
        default=None,
        help=(
            "Optional identifier recorded into the batch CSV row (e.g., set_of_50_run_01). "
            "Defaults to an ISO timestamp."
        ),
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
    """Processes a single CSV file: computes hash, uploads, and optionally moves it."""
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
            max_429_retries = int(
                os.getenv("CSV_UPLOADER_MAX_429_RETRIES", "200").strip()
                or "200"
            )
            max_429_sleep_seconds = float(
                os.getenv("CSV_UPLOADER_MAX_429_SLEEP_SECONDS", "0.25").strip()
                or "0.25"
            )
            total_429_retries = 0

            for attempt in range(1, max_upload_retries + 1):
                try:
                    while True:
                        # Ensure file content is replayable for retries
                        f.seek(0)
                        response = session.post(
                            url,
                            files=files,
                            data=data,
                            timeout=request_timeout_seconds,
                        )

                        if response.status_code != 429:
                            break

                        total_429_retries += 1
                        if total_429_retries > max_429_retries:
                            print(
                                "Received too many 429 responses; giving up on this file. "
                                f"total_429_retries={total_429_retries}"
                            )
                            break

                        sleep_s = _sleep_seconds_for_429(
                            response=response,
                            attempt=total_429_retries,
                            retry_backoff_seconds=retry_backoff_seconds,
                            max_sleep_seconds=max_429_sleep_seconds,
                        )
                        print(
                            f"Backpressure 429; retrying after {sleep_s:.3f}s "
                            f"(total_429_retries={total_429_retries})."
                        )
                        time.sleep(sleep_s)

                    assert response is not None
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
                    if keep_files:
                        print("Keeping file in place (--keep-files).")
                    else:
                        failed_dir.mkdir(parents=True, exist_ok=True)
                        csv_path.rename(failed_dir / csv_path.name)
                        print(f"Moved to {failed_dir}")
                    return

        assert response is not None

        if response.status_code == 429:
            print(f"Error: {response.text}")
            if keep_files:
                print("Keeping file in place (--keep-files).")
            else:
                failed_dir.mkdir(parents=True, exist_ok=True)
                csv_path.rename(failed_dir / csv_path.name)
                print(f"Moved to {failed_dir}")
            return

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


def upload_file_and_get_ingestion_id(
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
) -> str | None:
    """Upload a file and return ingestion_id when the API returns one."""

    # Reuse the existing upload logic, but also surface ingestion_id for polling.
    print(f"Processing {csv_path.name}...")

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

    uploader_received_at = datetime.now().isoformat()
    run_id = csv_path.stem
    print(f"Extracted run id: {run_id}")

    sha256_hash = hashlib.sha256()

    try:
        with open(csv_path, "rb") as f:
            for byte_block in iter(lambda: f.read(4096), b""):
                sha256_hash.update(byte_block)
            content_sha256 = sha256_hash.hexdigest()
            print(f"content_sha256: {content_sha256}")

            f.seek(0)

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
            # 429 backpressure can happen during high-throughput runs.
            # Keep retries frequent
            max_429_retries = int(
                os.getenv("CSV_UPLOADER_MAX_429_RETRIES", "200").strip()
                or "200"
            )
            max_429_sleep_seconds = float(
                os.getenv("CSV_UPLOADER_MAX_429_SLEEP_SECONDS", "0.25").strip()
                or "0.25"
            )
            total_429_retries = 0

            for attempt in range(1, max_upload_retries + 1):
                try:
                    while True:
                        f.seek(0)
                        response = session.post(
                            url,
                            files=files,
                            data=data,
                            timeout=request_timeout_seconds,
                        )

                        if response.status_code != 429:
                            break

                        total_429_retries += 1
                        if total_429_retries > max_429_retries:
                            print(
                                "Received too many 429 responses; giving up on this file. "
                                f"total_429_retries={total_429_retries}"
                            )
                            break

                        sleep_s = _sleep_seconds_for_429(
                            response=response,
                            attempt=total_429_retries,
                            retry_backoff_seconds=retry_backoff_seconds,
                            max_sleep_seconds=max_429_sleep_seconds,
                        )
                        print(
                            f"Backpressure 429; retrying after {sleep_s:.3f}s "
                            f"(total_429_retries={total_429_retries})."
                        )
                        time.sleep(sleep_s)

                    # response is always set here unless session.post raised.
                    assert response is not None
                    break
                except (RequestsConnectionError, RequestsTimeout) as e:
                    print(
                        f"Upload attempt {attempt}/{max_upload_retries} failed: {e}"
                    )
                    if attempt < max_upload_retries:
                        time.sleep(retry_backoff_seconds * attempt)
                    else:
                        print(
                            f"API not reachable; leaving {csv_path.name} in pending for retry."
                        )
                        return None
                except RequestException as e:
                    print(f"Request failed: {e}")
                    if keep_files:
                        print("Keeping file in place (--keep-files).")
                    else:
                        failed_dir.mkdir(parents=True, exist_ok=True)
                        csv_path.rename(failed_dir / csv_path.name)
                        print(f"Moved to {failed_dir}")
                    return None

        assert response is not None

        if response.status_code == 429:
            # We exceeded max_429_retries above; treat as a normal failure.
            print(f"Error: {response.text}")
            if keep_files:
                print("Keeping file in place (--keep-files).")
            else:
                failed_dir.mkdir(parents=True, exist_ok=True)
                csv_path.rename(failed_dir / csv_path.name)
                print(f"Moved to {failed_dir}")
            return None

        print(f"API Response: {response.status_code}")
        if not response.ok:
            print(f"Error: {response.text}")
            if keep_files:
                print("Keeping file in place (--keep-files).")
            else:
                failed_dir.mkdir(parents=True, exist_ok=True)
                csv_path.rename(failed_dir / csv_path.name)
                print(f"Moved to {failed_dir}")
            return None

        payload: Any = response.json()
        print(f"Response: {payload}")
        ingestion_id = (
            payload.get("ingestion_id") if isinstance(payload, dict) else None
        )
        if ingestion_id is None:
            print(
                "Warning: API response missing ingestion_id; cannot wait for terminal."
            )

        if keep_files:
            print("Keeping file in place (--keep-files).")
        else:
            processed_dir.mkdir(parents=True, exist_ok=True)
            csv_path.rename(processed_dir / csv_path.name)
            print(f"Moved to {processed_dir}")

        return str(ingestion_id) if ingestion_id is not None else None
    except IOError as e:
        print(f"Error processing file {csv_path}: {e}")
        if keep_files:
            print("Keeping file in place (--keep-files).")
        else:
            failed_dir.mkdir(parents=True, exist_ok=True)
            csv_path.rename(failed_dir / csv_path.name)
            print(f"Moved to {failed_dir}")
        return None


def _is_terminal_status(status: str | None) -> bool:
    if not status:
        return False
    return status.upper() in {"COMPLETED", "FAILED", "FAILED VALIDATION"}


def poll_until_terminal(
    *,
    ingestion_id: str,
    config: dict,
    session: requests.Session,
    request_timeout_seconds: int,
    status_poll_seconds: int,
) -> dict[str, Any] | None:
    url = config["api_base_url"].rstrip(
        "/"
    ) + INGESTION_GET_API_ENDPOINT.format(ingestion_id=ingestion_id)
    while True:
        try:
            r = session.get(url, timeout=request_timeout_seconds)
            if not r.ok:
                print(
                    f"Status GET failed for {ingestion_id}: {r.status_code} {r.text}"
                )
                time.sleep(status_poll_seconds)
                continue
            payload: Any = r.json()
            if isinstance(payload, dict):
                status = payload.get("status")
            else:
                status = None

            if _is_terminal_status(status):
                return payload if isinstance(payload, dict) else None

            time.sleep(status_poll_seconds)
        except RequestException as e:
            print(f"Status poll error for {ingestion_id}: {e}")
            time.sleep(status_poll_seconds)


def main() -> None:
    try:
        args = _parse_args()
        config = read_config()

        watch_dir = Path(args.watch_dir)
        processed_dir = Path(args.processed_dir)
        failed_dir = Path(args.failed_dir)

        # Reuse connections across many uploads (important for batch benchmarks)
        session = requests.Session()

        if args.batch_results_csv and not args.wait_for_terminal:
            raise ValueError(
                "--batch-results-csv requires --wait-for-terminal"
            )

        if args.file:
            csv_files = [Path(p) for p in args.file]
            missing = [str(p) for p in csv_files if not p.exists()]
            if missing:
                raise FileNotFoundError(
                    "CSV file(s) not found: " + ", ".join(missing)
                )

            ingestion_ids: list[str] = []
            batch_start = time.perf_counter()

            for csv_path in sorted(csv_files, key=lambda p: p.name):
                ingestion_id = upload_file_and_get_ingestion_id(
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
                if ingestion_id:
                    ingestion_ids.append(ingestion_id)

            if args.wait_for_terminal and ingestion_ids:
                completed = 0
                failed = 0
                for ingestion_id in ingestion_ids:
                    payload = poll_until_terminal(
                        ingestion_id=ingestion_id,
                        config=config,
                        session=session,
                        request_timeout_seconds=args.timeout_seconds,
                        status_poll_seconds=args.status_poll_seconds,
                    )
                    status = (
                        payload.get("status")
                        if isinstance(payload, dict)
                        else None
                    )
                    if status and status.upper() == "COMPLETED":
                        completed += 1
                    else:
                        failed += 1

                batch_total_s = time.perf_counter() - batch_start
                files_per_min = (
                    None
                    if batch_total_s <= 0
                    else (len(ingestion_ids) / batch_total_s) * 60.0
                )

                print(
                    f"Batch completed: total={len(ingestion_ids)} completed={completed} failed={failed} "
                    f"batch_total_wall_time_s={batch_total_s:.3f} files_per_min={'' if files_per_min is None else f'{files_per_min:.3f}'}"
                )

                if args.batch_results_csv:
                    from datetime import timezone

                    from app.metrics.benchmark_csv_reporter import (
                        append_benchmark_batch_row,
                    )

                    batch_id = args.batch_id or datetime.now().isoformat()
                    append_benchmark_batch_row(
                        csv_path=args.batch_results_csv,
                        measured_at=datetime.now(timezone.utc),
                        git_sha="",
                        api_base_url=config.get("api_base_url"),
                        dataset=os.getenv("CLA_BENCHMARK_DATASET") or "",
                        batch_id=batch_id,
                        batch_file_count=len(ingestion_ids),
                        batch_completed_count=completed,
                        batch_failed_count=failed,
                        batch_total_wall_time_s=batch_total_s,
                        batch_files_per_min=files_per_min,
                    )
            return

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
