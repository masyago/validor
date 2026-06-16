from __future__ import annotations

from pathlib import Path
from collections.abc import Sequence
from typing import Any

import sys

project_root = str(Path(__file__).resolve().parents[1])
if project_root not in sys.path:
    sys.path.insert(0, project_root)

import argparse
import json
import time

import requests
from rich.rule import Rule

try:
    from csv_uploader.cli_rich import console
except ModuleNotFoundError as e:  # pragma: no cover
    raise ModuleNotFoundError(
        "Unable to import csv_uploader.cli_rich. "
        "Ensure the repository root is on sys.path and that the source "
        "tree is present in the runtime image."
    ) from e

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


def _fetch_ai_annotations(
    *,
    ingestion_id: str,
    base_url: str,
    session: requests.Session,
) -> tuple[list[dict[str, Any]], str | None, str]:
    ai_url = f"{base_url}/v1/ingestions/{ingestion_id}/ai_annotation"
    try:
        response = session.get(
            ai_url,
            timeout=csv_uploader.REQUEST_TIMEOUT_SECONDS,
        )
        if response.status_code == 404:
            return [], "API missing /ai_annotation", ai_url
        if not response.ok:
            return [], f"HTTP {response.status_code}", ai_url

        payload = response.json()
        if not isinstance(payload, list):
            return [], "invalid response", ai_url

        rows = [row for row in payload if isinstance(row, dict)]
        return rows, None, ai_url
    except requests.RequestException:
        return [], "unreachable", ai_url


def _fetch_observations_by_code(
    *,
    ingestion_id: str,
    base_url: str,
    session: requests.Session,
) -> tuple[dict[str, dict[str, Any]], str | None]:
    observations_url = f"{base_url}/v1/ingestions/{ingestion_id}/observations?limit=200&offset=0"
    try:
        response = session.get(
            observations_url,
            timeout=csv_uploader.REQUEST_TIMEOUT_SECONDS,
        )
        if not response.ok:
            return {}, f"HTTP {response.status_code}"

        payload = response.json()
        if not isinstance(payload, list):
            return {}, "invalid response"

        rows = [row for row in payload if isinstance(row, dict)]
        by_code: dict[str, dict[str, Any]] = {}
        for row in rows:
            code = row.get("code")
            if isinstance(code, str) and code and code not in by_code:
                by_code[code] = row
        return by_code, None
    except requests.RequestException:
        return {}, "unreachable"


def _print_labeled_value(
    *,
    out,
    label: str,
    value: str,
    value_style: str | None = None,
) -> None:
    out.print(f"{label:<18}", end="")
    if value_style:
        out.print(value, style=value_style)
    else:
        out.print(value)


def _format_bool_word(value: Any) -> str:
    return "yes" if bool(value) else "no"


def _format_confidence(value: Any) -> str | None:
    if isinstance(value, (int, float)):
        return f"confidence {float(value):.2f}"
    return None


def _format_reference_range(observation: dict[str, Any]) -> str | None:
    low = observation.get("ref_low_num")
    high = observation.get("ref_high_num")
    if low is None and high is None:
        return None
    low_str = "?" if low is None else f"{low:g}"
    high_str = "?" if high is None else f"{high:g}"
    return f"(ref {low_str}-{high_str})"


def _format_observation_value(observation: dict[str, Any]) -> str | None:
    value_num = observation.get("value_num")
    value_text = observation.get("value_text")
    unit = observation.get("unit")

    if isinstance(value_num, (int, float)):
        base = f"{float(value_num):g}"
    elif isinstance(value_text, str) and value_text.strip():
        base = value_text.strip()
    else:
        return None

    if isinstance(unit, str) and unit.strip():
        return f"{base} {unit.strip()}"
    return base


def _format_interpretation(
    observation: dict[str, Any],
) -> tuple[str | None, str | None]:
    raw = observation.get("flag_system_interpretation") or observation.get(
        "flag_analyzer_interpretation"
    )
    if not isinstance(raw, str) or not raw.strip():
        return None, None

    value = raw.strip().upper()
    if value == "HIGH":
        return "[error]▲ HIGH[/error]", value
    if value == "LOW":
        return "[warning]▼ LOW[/warning]", value
    if value == "NORMAL":
        return "[warning]● NORMAL[/warning]", value
    return value, value


def _print_finding_block(
    *,
    out,
    finding: dict[str, Any],
    observation: dict[str, Any] | None,
) -> None:
    analyte_code = str(finding.get("analyte_code") or "UNKNOWN")
    confidence = _format_confidence(finding.get("confidence"))
    trend = finding.get("trend_direction")
    description = finding.get("description")

    headline_parts: list[str] = [f"{analyte_code:<6}"]
    if observation is not None:
        interpretation_markup, _ = _format_interpretation(observation)
        if interpretation_markup is not None:
            headline_parts.append(interpretation_markup)

        observation_value = _format_observation_value(observation)
        if observation_value:
            headline_parts.append(observation_value)

        ref_range = _format_reference_range(observation)
        if ref_range:
            headline_parts.append(ref_range)

    if confidence:
        headline_parts.append(confidence)

    out.print("  ".join(headline_parts))

    if isinstance(trend, str) and trend.strip():
        out.print(f"        trend: {trend}")
    if isinstance(description, str) and description.strip():
        out.print(f"        {description}")


def _print_ai_annotation_section(
    *,
    ingestion_id: str,
    base_url: str,
    session: requests.Session,
    console_out,
) -> None:
    out = console_out
    ai_rows, ai_warning, ai_url = _fetch_ai_annotations(
        ingestion_id=ingestion_id,
        base_url=base_url,
        session=session,
    )
    observations_by_code, observations_warning = _fetch_observations_by_code(
        ingestion_id=ingestion_id,
        base_url=base_url,
        session=session,
    )

    out.print("")
    out.print("AI ANNOTATION", style="bold")
    out.print(Rule(style="white"))

    if ai_warning:
        out.print(
            f"AI annotation unavailable ({ai_warning})",
            style="warning",
        )
        return

    if not ai_rows:
        out.print("No persisted AI annotations for this ingestion.")
        return

    for index, row in enumerate(ai_rows, start=1):
        if index > 1:
            out.print("")

        validation_status = str(row.get("validation_status") or "UNKNOWN")
        validation_style = (
            "success"
            if validation_status == "ACCEPTED"
            else "error" if validation_status == "REJECTED" else None
        )

        _print_labeled_value(
            out=out,
            label="annotation_id",
            value=str(row.get("ai_annotation_id") or "UNKNOWN"),
        )
        _print_labeled_value(
            out=out,
            label="validation_status",
            value=validation_status,
            value_style=validation_style,
        )
        _print_labeled_value(
            out=out,
            label="annotation_type",
            value=str(row.get("annotation_type") or "n/a"),
        )

        provider = row.get("provider")
        model_id = row.get("model_id")
        if provider or model_id:
            _print_labeled_value(
                out=out,
                label="provider / model",
                value=(provider or "UNKNOWN")
                + " / "
                + (model_id or "UNKNOWN"),
            )

        content_json = row.get("content_json")
        if isinstance(content_json, dict):
            if "requires_review" in content_json:
                _print_labeled_value(
                    out=out,
                    label="requires_review",
                    value=_format_bool_word(
                        content_json.get("requires_review")
                    ),
                )
            if content_json.get("review_priority"):
                _print_labeled_value(
                    out=out,
                    label="review_priority",
                    value=str(content_json.get("review_priority")),
                )

            summary = content_json.get("summary")
            if isinstance(summary, str) and summary.strip():
                out.print("")
                out.print("Summary")
                out.print(summary)

            analyte_findings = content_json.get("analyte_findings")
            if isinstance(analyte_findings, list) and analyte_findings:
                out.print("")
                for finding in analyte_findings:
                    if not isinstance(finding, dict):
                        continue
                    observation = observations_by_code.get(
                        str(finding.get("analyte_code") or "")
                    )
                    _print_finding_block(
                        out=out,
                        finding=finding,
                        observation=observation,
                    )
                    out.print("")
            elif validation_status == "ACCEPTED":
                out.print("")
                out.print("content_json:")
                out.print(
                    json.dumps(
                        content_json,
                        indent=2,
                        sort_keys=True,
                        default=str,
                    ),
                    style="white",
                    highlight=False,
                )

        rejection_reason = row.get("rejection_reason")
        if isinstance(rejection_reason, str) and rejection_reason.strip():
            out.print(f"rejection_reason: {rejection_reason}", style="warning")

        if observations_warning and index == 1:
            out.print(
                f"Observation context unavailable ({observations_warning})",
                style="warning",
            )


def _print_ingestion_processing_status(
    *,
    ingestion_id: str,
    config: dict,
    session: requests.Session,
    console_out=None,
    status_payload_override: dict[str, Any] | None = None,
) -> None:
    out = console_out or console
    # Wait for terminal so the stage metrics are stable.
    status_payload = (
        status_payload_override
        if status_payload_override is not None
        else csv_uploader.poll_until_terminal(
            ingestion_id=ingestion_id,
            config=config,
            session=session,
            request_timeout_seconds=csv_uploader.REQUEST_TIMEOUT_SECONDS,
            status_poll_seconds=1,
        )
    )

    base_url = str(config.get("api_base_url", "http://localhost:3000")).rstrip(
        "/"
    )
    events_url = f"{base_url}/v1/ingestions/{ingestion_id}/processing-events"
    events: list[dict[str, Any]] = []
    stage_events_warning: str | None = None
    try:
        r = session.get(
            events_url,
            timeout=csv_uploader.REQUEST_TIMEOUT_SECONDS,
        )
        if r.status_code == 404:
            stage_events_warning = "API missing /processing-events"
        elif not r.ok:
            stage_events_warning = f"HTTP {r.status_code}"
        else:
            payload = r.json()
            if isinstance(payload, list):
                events = [e for e in payload if isinstance(e, dict)]
            else:
                stage_events_warning = "invalid response"
    except requests.RequestException:
        stage_events_warning = "unreachable"

    def _event_types_present() -> set[str]:
        out: set[str] = set()
        for e in events:
            t = e.get("event_type")
            if isinstance(t, str):
                out.add(t)
        return out

    types = _event_types_present()

    parse_failed = "PARSE_FAILED" in types
    parse_succeeded = "PARSE_SUCCEEDED" in types

    validation_failed = "VALIDATION_FAILED" in types
    validation_succeeded = "VALIDATION_SUCCEEDED" in types

    normalization_failed = (
        "NORMALIZATION_FAILED" in types
        or "NORMALIZATION_RELATIONAL_FAILED" in types
    )
    normalization_succeeded = (
        "NORMALIZATION_SUCCEEDED" in types
        or "NORMALIZATION_SUCCEEDED_WITH_WARNINGS" in types
        or "NORMALIZATION_RELATIONAL_SUCCEEDED" in types
    )

    fhir_failed = "FHIR_JSON_GENERATION_FAILED" in types
    fhir_succeeded = "FHIR_JSON_GENERATION_SUCCEEDED" in types

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
        out.print(f"{label:<16} {symbol} {status}{detail}")

    out.print("")
    out.print("")
    out.print("INGESTION PROCESSING STATUS")
    out.print(Rule(style="white"))
    out.print(f"ingestion_id: {ingestion_id}")
    if stage_events_warning:
        out.print(
            f"stage events unavailable ({stage_events_warning})",
            style="warning",
        )
    out.print("")

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

    out.print("")
    final_status = (
        status_payload.get("status")
        if isinstance(status_payload, dict)
        else None
    )
    if not isinstance(final_status, str) or not final_status:
        final_status = "UNKNOWN"

    normalized_status = final_status.strip().upper().replace("_", " ")
    status_style: str | None
    if normalized_status == "COMPLETED":
        status_style = "success"
    elif normalized_status in {"FAILED", "FAILED VALIDATION"}:
        status_style = "error"
    else:
        status_style = None

    out.print("FINAL STATUS: ", end="", style=status_style)
    if status_style:
        out.print(final_status, style=status_style)
    else:
        out.print(final_status)

    if isinstance(final_status, str) and final_status.upper() == "COMPLETED":
        out.print("")
        out.print("")
        out.print("LINKS", style="bold")
        out.print(Rule(style="white"))
        out.print(f"Status: {base_url}/v1/ingestions/{ingestion_id}")
        out.print("")
        out.print(
            f"DiagnosticReports: {base_url}/v1/ingestions/{ingestion_id}/diagnostic-reports"
        )
        out.print("")
        out.print(
            f"Observations: {base_url}/v1/ingestions/{ingestion_id}/observations"
        )
        out.print("")
        out.print(
            f"AI annotations: {base_url}/v1/ingestions/{ingestion_id}/ai_annotation"
        )
        out.print("")
        out.print(
            "FHIR JSON: add `?include_json=1` to DiagnosticReports/Observations."
        )
        out.print("")
        _print_ai_annotation_section(
            ingestion_id=ingestion_id,
            base_url=base_url,
            session=session,
            console_out=out,
        )

    if isinstance(final_status, str) and final_status.upper().startswith(
        "FAILED"
    ):
        out.print("")
        out.print(Rule(title="[red]ERRORS[/red]", style="red"))

        error_code = (
            status_payload.get("error_code")
            if isinstance(status_payload, dict)
            else None
        )
        out.print(
            f"error_code: {error_code if isinstance(error_code, str) and error_code else 'UNKNOWN'}"
        )

        details = (
            status_payload.get("error_detail")
            if isinstance(status_payload, dict)
            else None
        )
        if details is None:
            out.print("error_detail: <none>")
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
            out.print("error_detail:")
            out.print(pretty, style="white", highlight=False)


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
