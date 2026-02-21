from __future__ import annotations

import argparse
import threading
import time

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
        help="Generate a single CSV and exit (uploader thread is daemonized).",
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

    while True:
        csv_generator.main()
        if args.once:
            return
        time.sleep(max(1, args.generate_every))


if __name__ == "__main__":
    main()
