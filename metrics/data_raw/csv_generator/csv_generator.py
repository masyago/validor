"""The script generates synthetic CSV files used as representative CSV set for metrics."""

import csv
from datetime import datetime, date, timezone
import yaml
from pathlib import Path
import json
import random
import uuid
import argparse

TARGET_FOLDER = Path("metrics/data_raw")
CONFIG_FILE_PATH = Path("metrics/data_raw/csv_generator/config.yaml")
ANALYTES_FILE_PATH = Path("metrics/data_raw/csv_generator/analytes.yaml")
PROFILES_FILE_PATH = Path(
    "metrics/data_raw/csv_generator/generation_profiles.yaml"
)
TOTAL_ANALYTE_NUMBER = 18


CSV_HEADER = [
    "run_id",
    "sample_id",
    "patient_id",
    "panel_code",
    "test_code",
    "test_name",
    "analyte_type",
    "result",
    "units",
    "reference_range_low",
    "reference_range_high",
    "flag",
    "collection_timestamp",
    "instrument_id",
]


def _parse_args():
    parser = argparse.ArgumentParser(
        description="Number of 18-analyte batches to include to CSV. Should be a positive integer."
    )
    parser.add_argument(
        "number_of_batches", type=int, help="Number of batches."
    )
    return parser.parse_args()


def read_config() -> dict:
    """Reads configuration data from a YAML file."""
    with open(CONFIG_FILE_PATH, "r") as file:
        config_data = yaml.safe_load(file)
        return config_data


def read_analytes() -> list[dict]:
    """Reads predefined analytes from YAML file."""
    with open(ANALYTES_FILE_PATH, "r") as file:
        data = yaml.safe_load(file)
        return data.get("analytes", [])


def read_profiles() -> dict:
    """Reads generation profiles from YAML file."""
    with open(PROFILES_FILE_PATH, "r") as file:
        data = yaml.safe_load(file)
        return data.get("profiles", {})


def generate_result_value(low: float, high: float) -> tuple[float, str | None]:
    """
    Generates a result value and optional flag.
    Most values within range, some outside.
    """
    rand = random.random()

    if rand < 0.70:  # 70% normal (within range)
        result = random.uniform(low, high)
        flag = "NORMAL"
    elif rand < 0.85:  # 15% low (below range)
        range_width = high - low
        result = random.uniform(low - (range_width * 0.3), low)
        if result < 0:
            result = 0
        flag = "LOW"
    else:  # 15% high (above range)
        range_width = high - low
        result = random.uniform(high, high + (range_width * 0.3))
        flag = "HIGH"

    return round(result, 2), flag


def generate_csv_rows_one_batch(
    analytes: list[dict],
    config: dict,
    run_id: str,
    sample_id: str,
    patient_id: str,
    collection_timestamp: str,
) -> list[list]:
    """
    Generates CSV data for one batch (all analytes included once).

    Args:
        analytes: All available analytes
        config: Configuration dict
        run_id, sample_id, patient_id: Generated IDs
        collection_timestamp: Timestamp string
    """

    csv_data_one_batch: list[list] = []

    # One batch = one row per analyte (all analytes included once).
    # Keep analytes ordering as provided in the YAML.
    for analyte in analytes:
        result, flag = generate_result_value(
            analyte["reference_range_low"],
            analyte["reference_range_high"],
        )

        row = {
            "run_id": run_id,
            "sample_id": sample_id,
            "patient_id": patient_id,
            "panel_code": analyte["panel"],
            "test_code": analyte["test_code"],
            "test_name": analyte["test_name"],
            "analyte_type": analyte["analyte_type"],
            "result": result,
            "units": analyte["units"],
            "reference_range_low": analyte["reference_range_low"],
            "reference_range_high": analyte["reference_range_high"],
            "flag": flag if flag else "",
            "collection_timestamp": collection_timestamp,
            "instrument_id": config["instrument_id"],
        }

        # Convert dict to list in header order
        csv_data_one_batch.append([row.get(col, "") for col in CSV_HEADER])

    return csv_data_one_batch


def create_n_data_batches(n: int) -> tuple[str, list[list]] | None:
    try:
        patient_id = f"PAT-{uuid.uuid4()}"
        collection_timestamp = datetime.now(timezone.utc).isoformat()
        today_str = date.today().strftime("%Y%m%d")

        run_id = f"{today_str}_{uuid.uuid4()}"

        csv_filename = run_id + ".csv"

        config = read_config()
        analytes = read_analytes()

        if not analytes:
            print("Error: No analytes found in analytes.yaml")
            return None

        csv_data = []
        csv_data.append(CSV_HEADER)

        for i in range(n):
            # sample_id set at a batch level
            sample_id = f"SAM-{uuid.uuid4()}"

            csv_data_one_batch = generate_csv_rows_one_batch(
                analytes=analytes,
                config=config,
                run_id=run_id,
                sample_id=sample_id,
                patient_id=patient_id,
                collection_timestamp=collection_timestamp,
            )
            csv_data.extend(csv_data_one_batch)
        return csv_filename, csv_data

    except FileNotFoundError:
        print(f"Error: Configuration file not found at {CONFIG_FILE_PATH}")
        return None
    except KeyError as e:
        print(f"Error: Missing key {e} in the configuration file.")
        return None
    except Exception as e:
        print(f"Unexpected error: {e}")
        return None


def create_csv_in_folder(
    folder_path: Path, file_name: str, data: list[list]
) -> Path | None:
    try:
        # Create the directory if it doesn't exist
        folder_path.mkdir(parents=True, exist_ok=True)

        full_path = Path(folder_path) / file_name

        # newline='' is important when working with the csv module
        with open(full_path, mode="w", newline="", encoding="utf-8") as file:
            writer = csv.writer(file)

            # Write the data
            writer.writerows(data)

        print(f"Successfully created CSV file: {full_path}")
        return full_path

    except IOError as e:
        print(f"Error writing to file {full_path}: {e}")
        return None


def main() -> Path | None:
    """Generates a single simulated lab analyzer CSV file."""
    arg = _parse_args()
    n = arg.number_of_batches

    try:
        csv_filename, csv_data = create_n_data_batches(n)
        created_path = create_csv_in_folder(
            TARGET_FOLDER, csv_filename, csv_data
        )

        # Print generation summary
        print(f"  Number of batches: {n}")
        print(f"  Target number of rows: {n * TOTAL_ANALYTE_NUMBER}")
        print(f"  Total rows generated: {len(csv_data) - 1}")  # -1 for header
        print("DONE")

        return created_path

    except Exception as e:
        print(f"Unexpected error: {e}")
        return None


if __name__ == "__main__":
    main()
