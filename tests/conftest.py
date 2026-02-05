import pytest
import io
from datetime import datetime
import hashlib
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

TEST_DATABASE_URL = "postgresql+psycopg://localhost:5432/test_cla"


@pytest.fixture(scope="session")
def test_db():
    """Connect to the test database (tables already exist via Alembic)."""
    engine = create_engine(TEST_DATABASE_URL, echo=True)
    yield engine
    engine.dispose()


@pytest.fixture
def db_session(test_db):
    """Provide a clean database session for each test with transaction rollback."""
    connection = test_db.connect()
    transaction = connection.begin()
    session = Session(bind=connection)

    yield session

    session.close()
    transaction.rollback()  # Undo any changes made during the test
    connection.close()


@pytest.fixture(scope="session")
def uploader_id() -> str:
    """Provides the standard uploader ID for tests."""
    return "uploader_001"


@pytest.fixture(scope="session")
def spec_version() -> str:
    """Provides the canonical spec version for tests."""
    return "analyzer_csv_v1"


@pytest.fixture(scope="session")
def instrument_id() -> str:
    """Provides the canonical instrument ID for tests."""
    return "CANONICAL_CHEM_ANALYZER_V1"


@pytest.fixture
def run_id() -> str:
    """Provides a generated run ID for a single test."""
    today = datetime.now().strftime("%Y%m%d")
    return f"{today}_001"


# Test Data Generators


@pytest.fixture
def valid_csv_file_content(run_id, instrument_id):
    """Provides the raw string content for a valid CSV file."""
    return f"instrument_id,run_id\n{instrument_id},{run_id}"


@pytest.fixture
def valid_csv_file(run_id, valid_csv_file_content):
    """Generates a valid CSV file-like object for testing."""
    # csv_content = f"instrument_id,run_id\n{instrument_id},{run_id}"
    csv_bytes = io.BytesIO(valid_csv_file_content.encode("utf-8"))

    # The format for the 'files' parameter is a dictionary
    return {"file": (f"{run_id}.csv", csv_bytes, "text/csv")}


# Generates test content hash
@pytest.fixture
def content_sha256(valid_csv_file_content):
    return hashlib.sha256(valid_csv_file_content.encode("utf-8")).hexdigest()


@pytest.fixture
def valid_form_data(uploader_id, spec_version, instrument_id, run_id):
    """Provides a dictionary of valid form data for a POST request."""
    return {
        "uploader_id": uploader_id,
        "spec_version": spec_version,
        "instrument_id": instrument_id,
        "run_id": run_id,
        "uploader_received_at": datetime.now().isoformat(),
    }
