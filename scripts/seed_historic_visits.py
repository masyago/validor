from __future__ import annotations

import argparse
import csv
import enum
import hashlib
import json
import sys
import uuid
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import Any
from uuid import uuid4

from dotenv import load_dotenv

load_dotenv()

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.core.ingestion_status_enums import IngestionStatus
from app.persistence.db import engine
from app.persistence.models.core import Ingestion, RawData
from app.persistence.models.normalization import DiagnosticReport, Observation
from app.persistence.models.parsing import Panel, Test
from app.persistence.models.patient import Patient
from app.persistence.repositories.ingestion_repo import IngestionRepository
from app.persistence.repositories.observation_repo import ObservationRepository
from app.persistence.repositories.panel_repo import PanelRepository
from app.persistence.repositories.raw_data_repo import RawDataRepository
from app.services.ingestion_service import IngestionService

DEFAULT_HISTORIC_DIR = ROOT_DIR / "demo" / "csv_files" / "historic_seed"
DEFAULT_FIXTURE_PATH = ROOT_DIR / "fixtures" / "historic_visits.json"
SPEC_VERSION = "analyzer_csv_v1"
UPLOADER_ID = "seed_historic_loader"


def _read_first_row(csv_path: Path) -> dict[str, str]:
    with csv_path.open("r", newline="") as f:
        reader = csv.DictReader(f)
        first_row = next(reader, None)
    if first_row is None:
        raise ValueError(f"{csv_path} has no data rows.")
    return first_row


def _sha256(content: bytes) -> str:
    return hashlib.sha256(content).hexdigest()


def seed_visit(session: Session, csv_path: Path) -> uuid.UUID:
    content = csv_path.read_bytes()
    first_row = _read_first_row(csv_path)

    instrument_id = first_row["instrument_id"]
    run_id = first_row["run_id"]
    uploader_received_at = datetime.fromisoformat(
        first_row["collection_timestamp"]
    )

    ingestion_repo = IngestionRepository(session)
    existing = ingestion_repo.get_by_instrument_id_run_id(
        instrument_id, run_id
    )
    if existing is not None:
        print(
            f"skip {csv_path.name}: already seeded "
            f"(ingestion_id={existing.ingestion_id}, status={existing.status})"
        )
        return existing.ingestion_id

    ingestion_id = uuid4()
    ingestion_repo.create(
        Ingestion(
            ingestion_id=ingestion_id,
            instrument_id=instrument_id,
            run_id=run_id,
            uploader_id=UPLOADER_ID,
            spec_version=SPEC_VERSION,
            uploader_received_at=uploader_received_at,
            api_received_at=uploader_received_at,
            submitted_sha256=None,
            server_sha256=_sha256(content),
            status=IngestionStatus.RECEIVED,
            source_filename=csv_path.name,
            kind="SEED",
        )
    )
    RawDataRepository(session).create(
        RawData(
            ingestion_id=ingestion_id,
            content_bytes=content,
            content_mime="text/csv",
            content_size_bytes=len(content),
        )
    )
    session.commit()

    IngestionService(session).process_ingestion(
        ingestion_id, skip_ai_stages=True
    )
    session.commit()

    ingestion = ingestion_repo.get_by_ingestion_id(ingestion_id)
    panel_count = len(PanelRepository(session).get_by_ingestion_id(ingestion_id))
    observation_count = len(
        ObservationRepository(session).get_by_ingestion_id(ingestion_id)
    )
    print(
        f"seeded {csv_path.name}: ingestion_id={ingestion_id} "
        f"status={ingestion.status if ingestion else 'UNKNOWN'} "
        f"panels={panel_count} observations={observation_count}"
    )
    return ingestion_id


def _serialize(value: Any) -> Any:
    if isinstance(value, uuid.UUID):
        return str(value)
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, enum.Enum):
        return value.value
    return value


def _dump(obj: Any) -> dict[str, Any]:
    return {
        column.name: _serialize(getattr(obj, column.name))
        for column in obj.__table__.columns
    }


def export_fixture(
    session: Session, ingestion_ids: list[uuid.UUID], output_path: Path
) -> None:
    """Snapshot the fully-processed historic-visit rows to a fixture file.

    Offline/manual step only — `scripts/seed_db.py` bulk-inserts this fixture
    at container startup without re-running parsing/validation/normalization.
    """
    ingestion_repo = IngestionRepository(session)
    ingestions = [
        ingestion
        for ingestion in (
            ingestion_repo.get_by_ingestion_id(iid) for iid in ingestion_ids
        )
        if ingestion is not None
    ]
    panels = (
        session.execute(
            select(Panel).where(Panel.ingestion_id.in_(ingestion_ids))
        )
        .scalars()
        .all()
    )
    panel_ids = [panel.panel_id for panel in panels]
    tests = (
        session.execute(select(Test).where(Test.panel_id.in_(panel_ids)))
        .scalars()
        .all()
        if panel_ids
        else []
    )
    diagnostic_reports = (
        session.execute(
            select(DiagnosticReport).where(
                DiagnosticReport.ingestion_id.in_(ingestion_ids)
            )
        )
        .scalars()
        .all()
    )
    observations = (
        session.execute(
            select(Observation).where(
                Observation.ingestion_id.in_(ingestion_ids)
            )
        )
        .scalars()
        .all()
    )
    patient_ids = sorted({panel.patient_id for panel in panels})
    patients = (
        session.execute(
            select(Patient).where(Patient.patient_id.in_(patient_ids))
        )
        .scalars()
        .all()
        if patient_ids
        else []
    )

    fixture = {
        "patient": [_dump(row) for row in patients],
        "ingestion": [_dump(row) for row in ingestions],
        "panel": [_dump(row) for row in panels],
        "test": [_dump(row) for row in tests],
        "diagnostic_report": [_dump(row) for row in diagnostic_reports],
        "observation": [_dump(row) for row in observations],
    }

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(fixture, indent=2))
    print(
        "Fixture export complete: "
        f"patients={len(fixture['patient'])} "
        f"ingestions={len(fixture['ingestion'])} "
        f"panels={len(fixture['panel'])} tests={len(fixture['test'])} "
        f"diagnostic_reports={len(fixture['diagnostic_report'])} "
        f"observations={len(fixture['observation'])} "
        f"output={output_path}"
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Seed the 3 historic lab visits through the real ingestion "
            "pipeline (kind='SEED', AI stages skipped). Manual/offline step."
        )
    )
    parser.add_argument(
        "--export-fixture",
        nargs="?",
        const=str(DEFAULT_FIXTURE_PATH),
        default=None,
        metavar="PATH",
        help=(
            "After seeding, snapshot the resulting rows to a fixture file "
            f"for scripts/seed_db.py to replay at startup. Defaults to "
            f"{DEFAULT_FIXTURE_PATH} if the flag is passed with no value."
        ),
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()

    historic_dir = DEFAULT_HISTORIC_DIR
    csv_paths = sorted(historic_dir.glob("*.csv"))
    if not csv_paths:
        raise ValueError(f"No CSV files found in {historic_dir}")

    with Session(engine) as session:
        ingestion_ids = [
            seed_visit(session, csv_path) for csv_path in csv_paths
        ]

        if args.export_fixture:
            export_fixture(session, ingestion_ids, Path(args.export_fixture))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
