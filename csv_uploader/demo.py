from __future__ import annotations

import argparse
import threading
import time
from pathlib import Path

from csv_uploader import csv_generator, csv_uploader


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Local demo runner: periodically generate simulated analyzer CSVs "
            "and optionally run the uploader watcher."
        )
    )
    parser.add_argument(
        "--generate-every",
        type=int,
        default=60,
        help="Seconds between CSV generations (default: 60).",
    )
    parser.add_argument(
        "--once",
        action="store_true",
        help=(
            "Generate a single CSV and wait for the uploader to move it "
            "(uploaded/failed), then exit."
        ),
    )
    parser.add_argument(
        "--no-generate",
        action="store_true",
        help="Do not generate CSVs (useful if you only want the uploader).",
    )
    parser.add_argument(
        "--no-upload",
        action="store_true",
        help="Do not run the uploader watcher.",
    )

    args = parser.parse_args()

    if not args.no_upload:
        t = threading.Thread(target=csv_uploader.main, daemon=True)
        t.start()

    if args.no_generate:
        while True:
            time.sleep(3600)

    def _wait_for_processed(
        created_path: Path, *, timeout_seconds: int = 120
    ) -> None:
        start = time.monotonic()
        uploaded_path = csv_uploader.PROCESSED_DIR / created_path.name
        failed_path = csv_uploader.FAILED_DIR / created_path.name

        while True:
            if uploaded_path.exists():
                print(f"Uploaded OK: {uploaded_path}")
                return
            if failed_path.exists():
                print(f"Upload failed (moved): {failed_path}")
                return
            if not created_path.exists():
                # File moved somewhere (or deleted). Consider this done.
                print(f"CSV moved from pending: {created_path.name}")
                return

            elapsed = time.monotonic() - start
            if elapsed > timeout_seconds:
                print(
                    "Timed out waiting for upload; leaving file for later retry: "
                    f"{created_path}"
                )
                return

            time.sleep(1)

    while True:
        created = csv_generator.main()
        if args.once:
            if created is None:
                return
            if args.no_upload:
                print(f"Generated: {created}")
                return
            _wait_for_processed(created)
            return

        time.sleep(max(1, args.generate_every))


if __name__ == "__main__":
    main()
