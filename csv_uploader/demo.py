from __future__ import annotations

from pathlib import Path
from collections.abc import Sequence
from typing import Any

# Support both `python -m csv_uploader.demo` (preferred) and
# `python csv_uploader/demo.py` (legacy).
#
# In direct-script mode, `sys.path[0]` points at `csv_uploader/`, which means
# `import csv_uploader` may incorrectly resolve to `csv_uploader/csv_uploader.py`
# (a module) instead of the `csv_uploader/` package. Ensure the project root is
# on sys.path before importing any `csv_uploader.*` modules.
if __package__ in (None, ""):
    import sys

    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import argparse
import json
import time
from uuid import UUID

import requests
from rich.rule import Rule
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

try:
    from csv_uploader.cli_rich import console
except ModuleNotFoundError:  # pragma: no cover
    from cli_rich import console

from csv_uploader import csv_generator
from csv_uploader import csv_uploader


def _get_latest_event_details(
    *,
    events: Sequence[Any],
    event_types: set[Any],
) -> dict:
    for event in reversed(events):
        if getattr(event, "event_type", None) in event_types:
            details = getattr(event, "details", None)
            return details if isinstance(details, dict) else {}
    return {}


def _has_any_event_type(
    *, events: Sequence[Any], event_types: set[Any]
) -> bool:
    return any(
        getattr(event, "event_type", None) in event_types for event in events
    )


def _print_ingestion_processing_status(
    *,
    ingestion_id: str,
    config: dict,
    session: requests.Session,
) -> None:
    # Wait for terminal so the stage metrics are stable.
    csv_uploader.poll_until_terminal(
        ingestion_id=ingestion_id,
        config=config,
        session=session,
        request_timeout_seconds=csv_uploader.REQUEST_TIMEOUT_SECONDS,
        status_poll_seconds=1,
    )

    # Pull stage counts + final status from the DB.
    from app.persistence import db as app_db
    from app.persistence.models.provenance import ProcessingEventType
    from app.persistence.repositories.ingestion_repo import IngestionRepository
    from app.persistence.repositories.processing_event_repo import (
        ProcessingEventRepository,
    )

    ingestion_uuid = UUID(ingestion_id)
    engine = create_engine(app_db.DATABASE_URL, echo=False)
    with Session(engine) as db_session:
        ingestion = IngestionRepository(db_session).get_by_ingestion_id(
            ingestion_uuid
        )
        events = ProcessingEventRepository(db_session).list_by_ingestion_id(
            ingestion_uuid
        )

    parse_details = _get_latest_event_details(
        events=events,
        event_types={
            ProcessingEventType.PARSE_SUCCEEDED,
            ProcessingEventType.PARSE_FAILED,
        },
    )
    validation_details = _get_latest_event_details(
        events=events,
        event_types={
            ProcessingEventType.VALIDATION_SUCCEEDED,
            ProcessingEventType.VALIDATION_FAILED,
        },
    )
    normalization_details = _get_latest_event_details(
        events=events,
        event_types={
            ProcessingEventType.NORMALIZATION_RELATIONAL_SUCCEEDED,
            ProcessingEventType.NORMALIZATION_RELATIONAL_FAILED,
        },
    )

    row_count = parse_details.get("row_count")
    panel_count = validation_details.get("panel_count")
    test_count = validation_details.get("test_count")
    dr_count = normalization_details.get("diagnostic_reports_created")
    obs_count = normalization_details.get("observations_created")

    parse_failed = _has_any_event_type(
        events=events, event_types={ProcessingEventType.PARSE_FAILED}
    )
    parse_succeeded = _has_any_event_type(
        events=events, event_types={ProcessingEventType.PARSE_SUCCEEDED}
    )
    validation_failed = _has_any_event_type(
        events=events, event_types={ProcessingEventType.VALIDATION_FAILED}
    )
    validation_succeeded = _has_any_event_type(
        events=events, event_types={ProcessingEventType.VALIDATION_SUCCEEDED}
    )
    normalization_failed = _has_any_event_type(
        events=events,
        event_types={
            ProcessingEventType.NORMALIZATION_FAILED,
            ProcessingEventType.NORMALIZATION_RELATIONAL_FAILED,
        },
    )
    normalization_succeeded = _has_any_event_type(
        events=events,
        event_types={
            ProcessingEventType.NORMALIZATION_SUCCEEDED,
            ProcessingEventType.NORMALIZATION_SUCCEEDED_WITH_WARNINGS,
            ProcessingEventType.NORMALIZATION_RELATIONAL_SUCCEEDED,
        },
    )
    fhir_failed = _has_any_event_type(
        events=events,
        event_types={ProcessingEventType.FHIR_JSON_GENERATION_FAILED},
    )
    fhir_succeeded = _has_any_event_type(
        events=events,
        event_types={ProcessingEventType.FHIR_JSON_GENERATION_SUCCEEDED},
    )

    def _fmt_int(value: object) -> str:
        return str(value) if isinstance(value, int) else "?"

    valid_rows = row_count if isinstance(row_count, int) else None
    invalid_rows = 0 if valid_rows is not None else None

    def _stage_symbol_and_label(
        *, failed: bool, succeeded: bool, skipped: bool
    ) -> tuple[str, str]:
        if failed:
            return "✖", "failed"
        if succeeded:
            return "✔", "completed"
        if skipped:
            return "↷", "skipped"
        return "…", "pending"

    def _print_stage_line(
        *,
        label: str,
        failed: bool,
        succeeded: bool,
        skipped: bool,
        detail: str,
    ) -> None:
        symbol, status = _stage_symbol_and_label(
            failed=failed,
            succeeded=succeeded,
            skipped=skipped,
        )
        console.print(f"{label:<14} {symbol} {status}{detail}")

    console.print("")
    console.print(Rule("INGESTION PROCESSING STATUS"))
    console.print(f"ingestion_id: {ingestion_id}")
    console.print("")

    previous_stage_failed = False

    parse_skipped = False
    _print_stage_line(
        label="[PARSE]",
        failed=parse_failed,
        succeeded=parse_succeeded,
        skipped=parse_skipped,
        detail="",
    )
    previous_stage_failed = previous_stage_failed or parse_failed

    validation_skipped = (
        previous_stage_failed
        and not validation_failed
        and not validation_succeeded
    )
    _print_stage_line(
        label="[VALIDATION]",
        failed=validation_failed,
        succeeded=validation_succeeded,
        skipped=validation_skipped,
        detail="",
    )
    previous_stage_failed = previous_stage_failed or validation_failed

    normalization_skipped = (
        previous_stage_failed
        and not normalization_failed
        and not normalization_succeeded
    )
    _print_stage_line(
        label="[NORMALIZATION]",
        failed=normalization_failed,
        succeeded=normalization_succeeded,
        skipped=normalization_skipped,
        detail="",
    )
    previous_stage_failed = previous_stage_failed or normalization_failed

    fhir_skipped = (
        previous_stage_failed and not fhir_failed and not fhir_succeeded
    )
    _print_stage_line(
        label="[FHIR]",
        failed=fhir_failed,
        succeeded=fhir_succeeded,
        skipped=fhir_skipped,
        detail="",
    )

    console.print("")
    final_status = ingestion.status if ingestion is not None else "UNKNOWN"
    console.print(f"FINAL STATUS: {final_status}")

    if ingestion is None:
        return

    if (
        isinstance(ingestion.status, str)
        and ingestion.status.upper() == "COMPLETED"
    ):
        base_url = "http://localhost:8000"
        console.print("")
        console.print(Rule("LINKS"))
        console.print(
            f"Status:            {base_url}/v1/ingestions/{ingestion_id}"
        )
        console.print("")
        console.print(
            f"DiagnosticReports: {base_url}/v1/ingestions/{ingestion_id}/diagnostic-reports"
        )
        console.print("")
        console.print(
            f"Observations:      {base_url}/v1/ingestions/{ingestion_id}/observations"
        )
        console.print("")
        console.print(
            "FHIR JSON: add `?include_json=1` to DiagnosticReports/Observations."
        )
        console.print("")

    if isinstance(
        ingestion.status, str
    ) and ingestion.status.upper().startswith("FAILED"):
        console.print("")
        console.print(Rule("ERRORS"))
        console.print(f"error_code: {ingestion.error_code or 'UNKNOWN'}")

        details = ingestion.error_detail
        if details is None:
            console.print("error_detail: <none>")
        else:
            if isinstance(details, (dict, list)):
                pretty = json.dumps(
                    details,
                    indent=2,
                    sort_keys=True,
                    default=str,
                )
            elif isinstance(details, str):
                pretty = details
            else:
                pretty = repr(details)
            console.print("error_detail:")
            console.print(pretty)


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Local demo runner: periodically generate simulated analyzer CSVs "
            "and upload them in strict order."
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
        help=("Generate a single CSV, upload it, then exit."),
    )
    parser.add_argument(
        "--no-generate",
        action="store_true",
        help="Do not generate CSVs (useful if you only want the uploader).",
    )
    parser.add_argument(
        "--no-upload",
        action="store_true",
        help="Do not upload the generated CSV.",
    )

    args = parser.parse_args()

    if args.no_generate:
        # Uploader-only mode (watch pending folder).
        csv_uploader.main()
        return

    # Strict demo mode: only upload the CSV we just generated.
    config = csv_uploader.read_config()
    session = requests.Session()

    pending_dir = Path(csv_uploader.WATCH_DIR)
    existing_pending = sorted(pending_dir.glob("*.csv"))
    if existing_pending:
        console.print(
            f"Warning: pending folder already has {len(existing_pending)} CSV(s). "
            "Demo will upload only the newly generated file.",
            style="warning",
        )

    while True:
        created = csv_generator.main()
        if created is None:
            return

        if args.no_upload:
            if args.once:
                return
            time.sleep(max(1, args.generate_every))
            continue

        ingestion_id = csv_uploader.upload_file_and_get_ingestion_id(
            csv_path=created,
            config=config,
            session=session,
            processed_dir=Path(csv_uploader.PROCESSED_DIR),
            failed_dir=Path(csv_uploader.FAILED_DIR),
            stability_delay_seconds=0,
            request_timeout_seconds=csv_uploader.REQUEST_TIMEOUT_SECONDS,
            max_upload_retries=csv_uploader.MAX_UPLOAD_RETRIES,
            retry_backoff_seconds=csv_uploader.RETRY_BACKOFF_SECONDS,
            debug_request=False,
            keep_files=False,
        )

        if ingestion_id:
            _print_ingestion_processing_status(
                ingestion_id=ingestion_id,
                config=config,
                session=session,
            )

        if args.once:
            return

        time.sleep(max(1, args.generate_every))


if __name__ == "__main__":
    main()
