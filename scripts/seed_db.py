from __future__ import annotations

import json
import sys
import uuid
from collections import defaultdict
from decimal import Decimal
from pathlib import Path
from typing import Any

from dotenv import load_dotenv

load_dotenv()

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from sqlalchemy import func, select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.orm import Session

from app.persistence.db import engine
from app.persistence.models.ai import ChunkType, VectorStore
from app.persistence.models.core import Ingestion
from app.persistence.models.normalization import DiagnosticReport, Observation
from app.persistence.models.parsing import Panel, Test
from app.persistence.models.patient import Patient
from app.persistence.repositories.ingestion_repo import IngestionRepository
from app.persistence.repositories.patient_repo import PatientRepository

DEFAULT_HISTORIC_FIXTURE = ROOT_DIR / "fixtures" / "historic_visits.json"
DEFAULT_GUIDELINE_FIXTURE = ROOT_DIR / "fixtures" / "guideline_chunks.json"

UUID_FIELDS = {
    "ingestion_id",
    "panel_id",
    "test_id",
    "diagnostic_report_id",
    "observation_id",
}
DATETIME_FIELDS = {
    "uploader_received_at",
    "api_received_at",
    "collection_timestamp",
    "effective_at",
    "normalized_at",
    "created_at",
}
DECIMAL_FIELDS = {
    "result_value_num",
    "value_num",
    "ref_low_num",
    "ref_high_num",
}


def _deserialize_row(model, row: dict[str, Any]) -> dict[str, Any]:
    kwargs: dict[str, Any] = {}
    for column in model.__table__.columns:
        if column.name not in row:
            continue
        value = row[column.name]
        if value is None:
            kwargs[column.name] = None
        elif column.name in UUID_FIELDS:
            kwargs[column.name] = uuid.UUID(value)
        elif column.name in DATETIME_FIELDS:
            kwargs[column.name] = _parse_datetime(value)
        elif column.name in DECIMAL_FIELDS:
            kwargs[column.name] = Decimal(value)
        else:
            kwargs[column.name] = value
    return kwargs


def _parse_datetime(value: str):
    from datetime import datetime

    return datetime.fromisoformat(value)


def seed_historic_data(
    session: Session, fixture_path: Path = DEFAULT_HISTORIC_FIXTURE
) -> None:
    if not fixture_path.exists():
        print(f"seed_historic_data: no fixture at {fixture_path}, skipping")
        return

    fixture = json.loads(fixture_path.read_text())
    expected_ingestion_count = len(fixture.get("ingestion", []))
    if expected_ingestion_count == 0:
        print("seed_historic_data: fixture has no ingestion rows, skipping")
        return

    existing_seed_count = session.scalar(
        select(func.count())
        .select_from(Ingestion)
        .where(Ingestion.kind == "SEED")
    )
    if existing_seed_count >= expected_ingestion_count:
        print(
            "seed_historic_data: already seeded "
            f"({existing_seed_count} SEED ingestion rows present), skipping"
        )
        return

    patient_repo = PatientRepository(session)
    for patient_row in fixture.get("patient", []):
        patient_repo.upsert(
            patient_id=patient_row["patient_id"],
            given_name=patient_row["given_name"],
            family_name=patient_row["family_name"],
            email=patient_row.get("email"),
            is_synthetic=patient_row.get("is_synthetic", True),
        )

    panels_by_ingestion: dict[str, list[dict]] = defaultdict(list)
    for panel_row in fixture.get("panel", []):
        panels_by_ingestion[panel_row["ingestion_id"]].append(panel_row)

    tests_by_panel: dict[str, list[dict]] = defaultdict(list)
    for test_row in fixture.get("test", []):
        tests_by_panel[test_row["panel_id"]].append(test_row)

    diagnostic_reports_by_ingestion: dict[str, list[dict]] = defaultdict(list)
    for dr_row in fixture.get("diagnostic_report", []):
        diagnostic_reports_by_ingestion[dr_row["ingestion_id"]].append(dr_row)

    observations_by_ingestion: dict[str, list[dict]] = defaultdict(list)
    for obs_row in fixture.get("observation", []):
        observations_by_ingestion[obs_row["ingestion_id"]].append(obs_row)

    ingestion_repo = IngestionRepository(session)
    inserted = 0
    skipped = 0
    for ingestion_row in fixture["ingestion"]:
        ingestion_id = uuid.UUID(ingestion_row["ingestion_id"])
        if ingestion_repo.get_by_ingestion_id(ingestion_id) is not None:
            skipped += 1
            continue

        # Flush at each FK checkpoint — these models have no relationship()
        # mappings, so the unit of work can't topologically sort inserts by
        # dependency on its own; explicit flushes guarantee ordering.
        session.add(Ingestion(**_deserialize_row(Ingestion, ingestion_row)))
        session.flush()

        for panel_row in panels_by_ingestion[ingestion_row["ingestion_id"]]:
            session.add(Panel(**_deserialize_row(Panel, panel_row)))
        session.flush()

        for panel_row in panels_by_ingestion[ingestion_row["ingestion_id"]]:
            for test_row in tests_by_panel[panel_row["panel_id"]]:
                session.add(Test(**_deserialize_row(Test, test_row)))
        session.flush()

        for dr_row in diagnostic_reports_by_ingestion[
            ingestion_row["ingestion_id"]
        ]:
            session.add(
                DiagnosticReport(**_deserialize_row(DiagnosticReport, dr_row))
            )
        session.flush()

        for obs_row in observations_by_ingestion[
            ingestion_row["ingestion_id"]
        ]:
            session.add(Observation(**_deserialize_row(Observation, obs_row)))
        session.flush()

        inserted += 1

    session.commit()
    print(
        f"seed_historic_data: inserted {inserted} ingestion(s), "
        f"skipped {skipped} already-present"
    )


def seed_vector_store(
    session: Session, fixture_path: Path = DEFAULT_GUIDELINE_FIXTURE
) -> None:
    if not fixture_path.exists():
        print(f"seed_vector_store: no fixture at {fixture_path}, skipping")
        return

    rows = json.loads(fixture_path.read_text())
    if not rows:
        print("seed_vector_store: fixture is empty, skipping")
        return

    pipeline_version = rows[0]["pipeline_version"]
    existing_count = session.scalar(
        select(func.count())
        .select_from(VectorStore)
        .where(VectorStore.pipeline_version == pipeline_version)
    )
    if existing_count >= len(rows):
        print(
            "seed_vector_store: already seeded "
            f"({existing_count} rows for pipeline_version={pipeline_version}), "
            "skipping"
        )
        return

    values = [
        {
            "embedding_id": uuid.uuid4(),
            "embedding": row["embedding"],
            "source_id": uuid.UUID(row["source_id"]),
            "chunk_index": row["chunk_index"],
            "chunk_type": ChunkType(row["chunk_type"]),
            "chunk_text": row["chunk_text"],
            "content_hash": row["content_hash"],
            "embedding_model": row["embedding_model"],
            "pipeline_version": row["pipeline_version"],
            "is_current": True,
        }
        for row in rows
    ]

    stmt = pg_insert(VectorStore).values(values)
    stmt = stmt.on_conflict_do_nothing(
        constraint="uq_vector_chunk_model_pipeline"
    )
    session.execute(stmt)
    session.commit()

    new_count = session.scalar(
        select(func.count())
        .select_from(VectorStore)
        .where(VectorStore.pipeline_version == pipeline_version)
    )
    print(
        f"seed_vector_store: {new_count - existing_count} of {len(values)} "
        f"chunk(s) inserted for pipeline_version={pipeline_version} "
        f"({new_count} total)"
    )


def main() -> int:
    with Session(engine) as session:
        seed_historic_data(session)
        seed_vector_store(session)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
