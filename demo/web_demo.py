import io
import json
import os
from contextlib import redirect_stdout
from pathlib import Path
from typing import Any

import requests
import streamlit as st

if __package__ in (None, ""):
    import sys

    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from demo import cli_demo
from csv_uploader import csv_uploader
import importlib

import csv_uploader.cli_rich as cli_rich

# Streamlit hot-reload sometimes keeps imported modules cached; force a reload so
# changes to `make_console()` (e.g., adding `record=`) are picked up.
cli_rich = importlib.reload(cli_rich)
make_console = cli_rich.make_console

folder_path = "demo/csv_files"

# Get list of files in the directory
# Filter by extension if necessary (e.g., only .csv files)
files = sorted([f for f in os.listdir(folder_path) if f.endswith(".csv")])

# st.title("Clinical Lab Analyzer — Web Demo")
st.header("Clinical Lab Analyzer — Web Demo", divider="rainbow")

# Create the dropdown menu
selected_filename = st.selectbox("Select a file to upload:", files)

if "last_output" not in st.session_state:
    st.session_state["last_output"] = ""
if "last_ingestion_id" not in st.session_state:
    st.session_state["last_ingestion_id"] = None
if "last_final_status" not in st.session_state:
    st.session_state["last_final_status"] = None
if "diagnostic_reports_output" not in st.session_state:
    st.session_state["diagnostic_reports_output"] = ""
if "observations_output" not in st.session_state:
    st.session_state["observations_output"] = ""
if "diagnostic_reports_error" not in st.session_state:
    st.session_state["diagnostic_reports_error"] = ""
if "observations_error" not in st.session_state:
    st.session_state["observations_error"] = ""
if "show_diagnostic_reports" not in st.session_state:
    st.session_state["show_diagnostic_reports"] = False
if "show_observations" not in st.session_state:
    st.session_state["show_observations"] = False


def _run_upload_and_status(
    *, csv_path: Path
) -> tuple[str, str | None, str | None]:
    buffer = io.StringIO()
    # Capture output into plain text so Streamlit can render it via st.code().
    out = make_console(
        file=buffer,
        force_terminal=False,
        color_system=None,
        highlight=False,
    )

    final_status: str | None = None

    with redirect_stdout(buffer):
        config = csv_uploader.read_config()
        session = requests.Session()

        ingestion_id = csv_uploader.upload_file_and_get_ingestion_id(
            csv_path=csv_path,
            config=config,
            session=session,
            processed_dir=Path(csv_uploader.PROCESSED_DIR),
            failed_dir=Path(csv_uploader.FAILED_DIR),
            stability_delay_seconds=0,
            request_timeout_seconds=csv_uploader.REQUEST_TIMEOUT_SECONDS,
            max_upload_retries=csv_uploader.MAX_UPLOAD_RETRIES,
            retry_backoff_seconds=csv_uploader.RETRY_BACKOFF_SECONDS,
            debug_request=False,
            keep_files=True,
            console_out=out,
        )

        if ingestion_id:
            status_payload = csv_uploader.poll_until_terminal(
                ingestion_id=ingestion_id,
                config=config,
                session=session,
                request_timeout_seconds=csv_uploader.REQUEST_TIMEOUT_SECONDS,
                status_poll_seconds=1,
            )
            if isinstance(status_payload, dict):
                s = status_payload.get("status")
                final_status = s if isinstance(s, str) and s else None

            cli_demo._print_ingestion_processing_status(
                ingestion_id=ingestion_id,
                config=config,
                session=session,
                console_out=out,
                status_payload_override=(
                    status_payload
                    if isinstance(status_payload, dict)
                    else None
                ),
            )

    return buffer.getvalue(), ingestion_id, final_status


def _fetch_and_pretty_print(*, url: str) -> tuple[str, str]:
    """Fetch URL and return (pretty_output, error_message)."""

    try:
        r = requests.get(url, timeout=csv_uploader.REQUEST_TIMEOUT_SECONDS)
    except requests.RequestException as e:
        return "", f"Request failed: {e}"

    if not r.ok:
        return "", f"HTTP {r.status_code}: {r.text}"

    # Try JSON first; fall back to raw text.
    try:
        payload: Any = r.json()
    except ValueError:
        return r.text, ""

    try:
        pretty = json.dumps(payload, indent=2, sort_keys=True, default=str)
    except TypeError:
        pretty = repr(payload)
    return pretty, ""


if selected_filename:
    file_path = Path(folder_path) / selected_filename

    upload_clicked = st.button(
        "Upload the file"
    )  # same file can be uploaded >1 time; API may respond duplicate-ok

    if upload_clicked:
        output, ingestion_id, final_status = _run_upload_and_status(
            csv_path=file_path
        )
        st.session_state["last_output"] = output
        st.session_state["last_ingestion_id"] = ingestion_id
        st.session_state["last_final_status"] = final_status

        # Reset persisted-data panes on new upload.
        st.session_state["show_diagnostic_reports"] = False
        st.session_state["show_observations"] = False
        st.session_state["diagnostic_reports_output"] = ""
        st.session_state["observations_output"] = ""
        st.session_state["diagnostic_reports_error"] = ""
        st.session_state["observations_error"] = ""

st.subheader("Uploader & Ingestion Status")

ingestion_id = st.session_state.get("last_ingestion_id")
final_status = st.session_state.get("last_final_status")

if ingestion_id:
    st.caption(f"ingestion_id: {ingestion_id}")

if isinstance(final_status, str) and final_status:
    normalized = final_status.strip().upper().replace("_", " ")
    if normalized == "COMPLETED":
        st.success(f"FINAL STATUS: {final_status}")
    elif normalized.startswith("FAILED"):
        st.error(f"FINAL STATUS: {final_status}")
    else:
        st.info(f"FINAL STATUS: {final_status}")

    if normalized == "COMPLETED" and ingestion_id:
        st.divider()
        st.subheader("Persisted Data")

        config = csv_uploader.read_config()
        base_url = str(
            config.get("api_base_url", "http://localhost:8000")
        ).rstrip("/")

        dr_url = f"{base_url}/v1/ingestions/{ingestion_id}/diagnostic-reports"
        obs_url = f"{base_url}/v1/ingestions/{ingestion_id}/observations"

        col1, col2 = st.columns(2)
        with col1:
            dr_label = (
                "Hide DiagnosticReports Data"
                if st.session_state.get("show_diagnostic_reports")
                else "DiagnosticReports Data"
            )
            if st.button(dr_label, key="toggle_diagnostic_reports"):
                st.session_state["show_diagnostic_reports"] = not bool(
                    st.session_state.get("show_diagnostic_reports")
                )
                if st.session_state["show_diagnostic_reports"]:
                    out, err = _fetch_and_pretty_print(url=dr_url)
                    st.session_state["diagnostic_reports_output"] = out
                    st.session_state["diagnostic_reports_error"] = err
                st.rerun()
        with col2:
            obs_label = (
                "Hide Observations Data"
                if st.session_state.get("show_observations")
                else "Observations Data"
            )
            if st.button(obs_label, key="toggle_observations"):
                st.session_state["show_observations"] = not bool(
                    st.session_state.get("show_observations")
                )
                if st.session_state["show_observations"]:
                    out, err = _fetch_and_pretty_print(url=obs_url)
                    st.session_state["observations_output"] = out
                    st.session_state["observations_error"] = err
                st.rerun()

        if st.session_state.get("show_diagnostic_reports"):
            dr_err = st.session_state.get("diagnostic_reports_error") or ""
            if dr_err:
                st.error(f"DiagnosticReports fetch failed: {dr_err}")
            dr_out = st.session_state.get("diagnostic_reports_output") or ""
            if dr_out:
                st.code(dr_out)

        if st.session_state.get("show_observations"):
            obs_err = st.session_state.get("observations_error") or ""
            if obs_err:
                st.error(f"Observations fetch failed: {obs_err}")
            obs_out = st.session_state.get("observations_output") or ""
            if obs_out:
                st.code(obs_out)

st.code(st.session_state.get("last_output") or "(no output yet)")
