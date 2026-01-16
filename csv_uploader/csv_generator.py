"""The script simulates CSV files produced by a canonical lab analyzer."""

import csv
from datetime import date
import yaml
from pathlib import Path
import json

TARGET_FOLDER = Path("csv_uploader/simulated_exports/pending")
CONFIG_FILE_PATH = Path("csv_uploader/config.yaml")
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
        csv_data = [
            ["instrument_id", "run_id"],
            [config["instrument_id"], run_id],
        ]

        create_csv_in_folder(TARGET_FOLDER, csv_filename, csv_data)
        print("DONE")
    except FileNotFoundError:
        print(f"Error: Configuration file not found at {CONFIG_FILE_PATH}")
    except KeyError as e:
        print(f"Error: Missing key {e} in the configuration file.")


if __name__ == "__main__":
    main()
