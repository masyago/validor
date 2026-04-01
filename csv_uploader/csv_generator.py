"""The script simulates CSV files produced by a canonical lab analyzer."""

import csv
from datetime import datetime, date, timezone
import yaml
from pathlib import Path
import json
import random
import uuid
from typing import Any
from rich.rule import Rule

try:
    from csv_uploader.cli_rich import console
except ModuleNotFoundError:  # pragma: no cover
    from cli_rich import console

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
            console.print(
                "Warning: Could not read state file. Starting fresh.",
                style="warning",
            )

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


def generate_result_value(
    low: float, high: float, profile: dict[str, Any]
) -> tuple[float, str | None]:
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
        result = abs(random.uniform(low - (range_width * 0.3), low))
        flag = "LOW"
    else:  # 15% high (above range)
        range_width = high - low
        result = random.uniform(high, high + (range_width * 0.3))
        flag = "HIGH"

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
    profile: dict,
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

    # If this profile is meant to inject a defect, force at least one negative
    # numeric result (but only one) somewhere in the generated CSV.
    force_one_negative = bool(profile.get("negative_results", False))
    apply_missing_columns_to_all_rows = bool(profile.get("valid_csv", True))
    total_result_rows = sum(
        len(panel_analytes) for panel_analytes in selected_panels
    )
    forced_negative_row_index = (
        random.randrange(total_result_rows)
        if force_one_negative and total_result_rows > 0
        else None
    )

    # For invalid CSV profiles, omitting *all* missing columns on *all* rows
    # creates huge, noisy validation error output. Instead, omit each specified
    # column in only one row per CSV (one row per missing field).
    missing_column_row_index: dict[str, int] = {}
    if (
        missing_columns
        and not apply_missing_columns_to_all_rows
        and total_result_rows > 0
    ):
        used_row_indices: set[int] = set()
        all_indices = list(range(total_result_rows))

        for col in missing_columns:
            # Avoid colliding with forced negative row when also omitting result,
            # so we still inject both defects at least once.
            candidates = all_indices
            if (
                col == "result"
                and forced_negative_row_index is not None
                and total_result_rows > 1
            ):
                candidates = [
                    i for i in all_indices if i != forced_negative_row_index
                ]

            # Prefer unique rows for readability when possible.
            unique_candidates = [
                i for i in candidates if i not in used_row_indices
            ]
            pick_from = unique_candidates or candidates
            if not pick_from:
                continue
            chosen = random.choice(pick_from)
            missing_column_row_index[col] = chosen
            used_row_indices.add(chosen)

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
    result_row_index = 0
    for panel_analytes in selected_panels:
        for analyte in panel_analytes:
            result, flag = generate_result_value(
                analyte["reference_range_low"],
                analyte["reference_range_high"],
                profile=profile,
            )

            if (
                forced_negative_row_index is not None
                and result_row_index == forced_negative_row_index
            ):
                # Negative numeric results are invalid by design (validator should reject them).
                result = round(random.uniform(-100.0, -0.1), 2)

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
                    if apply_missing_columns_to_all_rows:
                        if (
                            col == "result"
                            and forced_negative_row_index is not None
                            and result_row_index == forced_negative_row_index
                        ):
                            continue
                        row[col] = ""  # Omit the value
                    else:
                        if (
                            missing_column_row_index.get(col)
                            == result_row_index
                        ):
                            row[col] = ""  # Omit the value

            result_row_index += 1

            # Convert dict to list in header order
            csv_data.append([row.get(col, "") for col in csv_data[0]])

    return csv_data


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

        print(f"  Output file: {full_path}")
        return full_path

    except IOError as e:
        console.print(
            Rule(title="CSV GENERATOR", align="left", characters="─")
        )
        print(f"Error writing to file {full_path}: {e}")
        return None


def main() -> Path | None:
    """Generates a single simulated lab analyzer CSV file."""

    print("")
    print("CSV GENERATOR")
    console.print(Rule(style="white"))

    try:
        today_str = date.today().strftime("%Y%m%d")
        run_counter = get_next_run_counter()
        run_id = f"{today_str}_{run_counter}"
        csv_filename = run_id + ".csv"

        config = read_config()
        analytes = read_analytes()
        profiles = read_profiles()

        if not analytes:
            print("")
            console.print(
                "Error: No analytes found in analytes.yaml", style="error"
            )
            return None

        if not profiles:
            print("")
            console.print(
                "Error: No profiles found in generation_profiles.yaml",
                style="error",
            )
            return None

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
            profile=profile_config,
        )

        created_path = create_csv_in_folder(
            TARGET_FOLDER, csv_filename, csv_data
        )

        # Print generation summary

        print(f"  Profile: {profile_name}")
        print(f"  Description: {profile_config.get('description', 'N/A')}")
        print(f"  Panels: {profile_config.get('panels_per_csv')}")
        print(f"  Total rows: {len(csv_data) - 1}")  # -1 for header
        missing_columns = profile_config.get("missing_columns") or []
        if missing_columns:
            print(
                f"  Missing columns: {', '.join(str(c) for c in missing_columns)}"
            )
        if not profile_config["valid_csv"]:
            print("")
            console.print(
                "STATUS: GENERATED (INTENTIONALLY INVALID)", style="success"
            )
            print("")
        else:
            print("")
            console.print("STATUS: GENERATED VALID CSV", style="success")
            print("")

        return created_path

    except FileNotFoundError:
        print("")
        console.print(
            f"Error: Configuration file not found at {CONFIG_FILE_PATH}",
            style="error",
        )
        return None
    except KeyError as e:
        print("")
        console.print(
            f"Error: Missing key {e} in the configuration file.", style="error"
        )
        return None
    except Exception as e:
        print("")
        console.print(f"Unexpected error: {e}", style="error")
        return None


if __name__ == "__main__":
    main()
