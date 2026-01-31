"""The script simulates CSV files produced by a canonical lab analyzer."""

import csv
from datetime import datetime, date, timezone
import yaml
from pathlib import Path
import json
import random
import uuid

TARGET_FOLDER = Path("csv_uploader/simulated_exports/pending")
CONFIG_FILE_PATH = Path("csv_uploader/config.yaml")
ANALYTES_FILE_PATH = Path("csv_uploader/analytes.yaml")
PROFILES_FILE_PATH = Path("csv_uploader/generation_profiles.yaml")
DAILY_RUN_COUNTER_FILE_PATH = Path("csv_uploader/daily_run_counter.json")


def get_next_run_counter() -> str:
    """
    Calculates the next run counter for the day based on a persistent state file.
    The counter resets daily.
    """
    today_str = date.today().strftime("%Y%m%d")
    state = {"last_date": "", "last_counter": 0}

    # Read the last state if the file exists
    if DAILY_RUN_COUNTER_FILE_PATH.exists():
        try:
            with open(DAILY_RUN_COUNTER_FILE_PATH, "r") as f:
                state = json.load(f)
        except (json.JSONDecodeError, IOError):
            print("Warning: Could not read state file. Starting fresh.")

    # If it's a new day, reset the counter. Otherwise, increment it.
    if state.get("last_date") == today_str:
        next_counter = state.get("last_counter", 0) + 1
    else:
        next_counter = 1

    # Update the state for the next run
    new_state = {"last_date": today_str, "last_counter": next_counter}
    with open(DAILY_RUN_COUNTER_FILE_PATH, "w") as f:
        json.dump(new_state, f)

    # Format as a 3-digit string with leading zeros
    return f"{next_counter:03d}"


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


def select_profile(profiles: dict) -> tuple[str, dict]:
    """
    Selects a profile based on probability weights.
    Returns profile name and profile config.
    """
    profile_names = list(profiles.keys())
    probabilities = [
        profiles[name].get("probability", 0) for name in profile_names
    ]

    # Normalize probabilities to sum to 1
    total = sum(probabilities)
    if total == 0:
        # Fallback to equal distribution
        normalized = [1.0 / len(probabilities)] * len(probabilities)
    else:
        normalized = [(p / total) for p in probabilities]

    selected = random.choices(profile_names, weights=normalized, k=1)[0]
    return selected, profiles[selected]


def generate_result_value(low: float, high: float) -> tuple[float, str | None]:
    """
    Generates a result value and optional flag.
    Most values within range, some outside.
    """
    rand = random.random()

    if rand < 0.70:  # 70% normal (within range)
        result = random.uniform(low, high)
        flag = None
    elif rand < 0.85:  # 15% low (below range)
        range_width = high - low
        result = random.uniform(low - (range_width * 0.3), low)
        flag = "low"
    else:  # 15% high (above range)
        range_width = high - low
        result = random.uniform(high, high + (range_width * 0.3))
        flag = "high"

    return round(result, 2), flag


def generate_csv_rows(
    analytes: list[dict],
    panels_count: int,
    missing_columns: list[str],
    config: dict,
    run_id: str,
    sample_id: str,
    patient_id: str,
    collection_timestamp: str,
) -> list[list]:
    """
    Generates CSV data based on profile specifications.

    Args:
        analytes: All available analytes
        panels_count: How many panels to include
        missing_columns: Which columns to randomly omit
        config: Configuration dict
        run_id, sample_id, patient_id: Generated IDs
        collection_timestamp: Timestamp string
    """
    # Organize analytes by panel
    panels = {}
    for analyte in analytes:
        panel = analyte["panel"]
        if panel not in panels:
            panels[panel] = []
        panels[panel].append(analyte)
    # Select N random panels
    available_panels = list(panels.values())
    selected_panels = random.sample(
        available_panels, min(panels_count, len(available_panels))
    )

    # CSV header
    csv_data = [
        [
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
    ]

    # Generate rows for each selected panel
    for panel_analytes in selected_panels:
        for analyte in panel_analytes:
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

            # Apply missing columns strategy
            for col in missing_columns:
                if col in row:
                    row[col] = ""  # Omit the value

            # Convert dict to list in header order
            csv_data.append([row.get(col, "") for col in csv_data[0]])

    return csv_data


def create_csv_in_folder(
    folder_path: Path, file_name: str, data: list[list]
) -> None:
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

    except IOError as e:
        print(f"Error writing to file {full_path}: {e}")


def main() -> None:
    """Generates a single simulated lab analyzer CSV file."""
    try:
        today_str = date.today().strftime("%Y%m%d")
        run_counter = get_next_run_counter()
        run_id = f"{today_str}_{run_counter}"
        csv_filename = run_id + ".csv"

        config = read_config()
        analytes = read_analytes()
        profiles = read_profiles()

        if not analytes:
            print("Error: No analytes found in analytes.yaml")
            return

        if not profiles:
            print("Error: No profiles found in generation_profiles.yaml")
            return

        # Select generation profile
        profile_name, profile_config = select_profile(profiles)

        # Generate metadata
        patient_id = f"PAT-{uuid.uuid4()}"
        sample_id = f"SAM-{uuid.uuid4()}"
        collection_timestamp = datetime.now(timezone.utc).isoformat()

        # Generate CSV based on profile
        csv_data = generate_csv_rows(
            analytes=analytes,
            panels_count=profile_config.get("panels_per_csv", 1),
            missing_columns=profile_config.get("missing_columns", []),
            config=config,
            run_id=run_id,
            sample_id=sample_id,
            patient_id=patient_id,
            collection_timestamp=collection_timestamp,
        )

        create_csv_in_folder(TARGET_FOLDER, csv_filename, csv_data)

        # Print generation summary
        print(f"Generated CSV using profile: {profile_name}")
        print(f"  Description: {profile_config.get('description', 'N/A')}")
        print(f"  Panels: {profile_config.get('panels_per_csv')}")
        print(f"  Total rows: {len(csv_data) - 1}")  # -1 for header
        if profile_config.get("missing_columns"):
            print(
                f"  Missing columns: {', '.join(profile_config.get('missing_columns'))}"
            )
        print("DONE")

    except FileNotFoundError:
        print(f"Error: Configuration file not found at {CONFIG_FILE_PATH}")
    except KeyError as e:
        print(f"Error: Missing key {e} in the configuration file.")
    except Exception as e:
        print(f"Unexpected error: {e}")


if __name__ == "__main__":
    main()
