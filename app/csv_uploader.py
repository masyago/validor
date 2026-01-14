"""
CLI uploader for canonical analyzer CSV to ingestion API
The uploader detects new exports and pushes them to
POST /v1/ingestions on a schedule
"""

import argparse
import os
import requests


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("csv_path")
    p.add_argument("--api-base", default="http://localhost:8000")
    p.add_argument("--instrument-id", default="analyzer-sim-01")
    p.add_argument("--spec-version", default="analyzer_csv_v1")
    args = p.parse_args()

    url = args.api_base.rstrip("/") + "/v1/ingestions"

    with open(args.csv_path, "rb") as f:
        files = {"file": (os.path.basename(args.csv_path), f, "text/csv")}
        data = {
            "instrument_id": args.instrument_id,
            "spec_version": args.spec_version,
        }
        r = requests.post(url, files=files, data=data)

    print(r.status_code)
    print(r.json())


if __name__ == "__main__":
    main()
