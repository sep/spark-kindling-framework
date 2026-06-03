"""
System test: static_values on file ingestion registry entries.

Verifies that columns declared in static_values on a FileIngestionEntries.entry
are added as literal columns to every row ingested by that matcher — even though
the source CSV file does not contain those columns.

Setup (handled by fixtures):
  - Uploads a minimal CSV file (2 rows, 2 columns) to the test packages path
    in Azure Blob Storage so the cloud app can read it via ABFS.
  - Deploys a self-contained test app that:
      1. Defines a target entity (TestStaticValuesEntity)
      2. Registers a file ingestion entry with static_values
      3. Processes the ABFS path containing the uploaded CSV
      4. Queries the resulting Delta table and checks for the static columns

Verification:
  - The app prints STATIC_VALUES_TEST: PASSED if both static columns exist
    and have the expected values in every row.
  - BOOTSTRAP COMPLETE / "completed successfully" confirms clean exit.

Run:
    poe test-system --test test_file_ingestion_static_values
"""

import os
import uuid

import pytest

from tests.system.test_helpers import (
    assert_no_fatal_system_test_log_lines,
    create_platform_client,
    get_system_test_poll_interval,
    get_system_test_stream_max_wait,
)

# ── markers ────────────────────────────────────────────────────────────────────
MARKER_PASSED = "STATIC_VALUES_TEST: PASSED"
MARKER_FAILED = "STATIC_VALUES_TEST: FAILED"


# ── helpers ────────────────────────────────────────────────────────────────────


def _test_abfss_path() -> str:
    """Return the abfss:// path matching where the fixture uploads the test CSV."""
    storage_account = os.getenv("AZURE_STORAGE_ACCOUNT", "")
    container = os.getenv("AZURE_CONTAINER", "artifacts")
    base_path = os.getenv("AZURE_BASE_PATH", "").rstrip("/")
    cloud = (os.getenv("AZURE_CLOUD") or "").strip().lower().replace("-", "").replace("_", "")
    if cloud in {"azureusgovernment", "azuregovernment", "government", "gov", "usgov"}:
        suffix = "core.usgovcloudapi.net"
    elif cloud in {"azurechinacloud", "china"}:
        suffix = "core.chinacloudapi.cn"
    else:
        suffix = "core.windows.net"
    path = f"abfss://{container}@{storage_account}.dfs.{suffix}"
    if base_path:
        path = f"{path}/{base_path}"
    return path


def _csv_bytes() -> bytes:
    """Minimal CSV with two data rows and no static columns."""
    return b"row_id,value\n1,alpha\n2,beta\n"


# ── app source ─────────────────────────────────────────────────────────────────


def _app_src(
    csv_abfss_folder: str,
    entity_table_path: str,
    expected_source: str,
    expected_env: str,
) -> str:
    """Generate the test app Python source.

    The CSV is pre-uploaded by the fixture; the app only needs to:
      1. Define a target entity.
      2. Register a file ingestion entry with static_values.
      3. Process the ABFS folder.
      4. Read the entity table and validate static columns.
      5. Print a PASSED/FAILED marker.
    """
    return f"""\
import logging
import sys

_log = logging.getLogger("file_ingestion_static_test")

from kindling.data_entities import DataEntities
from kindling.file_ingestion import FileIngestionEntries
from kindling.injection import get_kindling_service
from kindling.spark_session import get_or_create_spark_session

spark = get_or_create_spark_session()

# ── Step 1: define target entity ──────────────────────────────────────────────
@DataEntities.entity(
    merge_columns=["row_id"],
    tags={{}},
    table_path="{entity_table_path}",
)
class TestStaticValuesEntity:
    row_id: str
    value: str

# ── Step 2: register ingestion entry with static_values ───────────────────────
FileIngestionEntries.entry(
    entry_id="test_static_values",
    name="test static values ingestion",
    patterns=[r"test_static\\.csv"],
    dest_entity_id="TestStaticValuesEntity",
    tags={{}},
    filetype="csv",
    static_values={{
        "source_system": "{expected_source}",
        "environment": "{expected_env}",
    }},
)

# ── Step 3: process the ABFS folder ──────────────────────────────────────────
from kindling.file_ingestion import ParallelizingFileIngestionProcessor
processor = get_kindling_service(ParallelizingFileIngestionProcessor)
processor.process_path("{csv_abfss_folder}")
_log.warning("File ingestion complete")

# ── Step 4: validate static columns ──────────────────────────────────────────
df = spark.read.format("delta").load("{entity_table_path}")
rows = df.collect()

if not rows:
    _log.warning(MARKER_FAILED := "STATIC_VALUES_TEST: FAILED — no rows written")
    sys.exit(1)

col_names = df.columns
if "source_system" not in col_names:
    _log.warning("STATIC_VALUES_TEST: FAILED — 'source_system' column missing")
    sys.exit(1)
if "environment" not in col_names:
    _log.warning("STATIC_VALUES_TEST: FAILED — 'environment' column missing")
    sys.exit(1)

bad_source = [r for r in rows if r["source_system"] != "{expected_source}"]
bad_env    = [r for r in rows if r["environment"] != "{expected_env}"]

if bad_source:
    _log.warning(
        "STATIC_VALUES_TEST: FAILED — source_system mismatch: %s",
        [r["source_system"] for r in bad_source],
    )
    sys.exit(1)
if bad_env:
    _log.warning(
        "STATIC_VALUES_TEST: FAILED — environment mismatch: %s",
        [r["environment"] for r in bad_env],
    )
    sys.exit(1)

_log.warning("STATIC_VALUES_TEST: PASSED")
sys.exit(0)
"""


# ── fixtures ───────────────────────────────────────────────────────────────────


@pytest.fixture(scope="module")
def blob_client():
    """BlobServiceClient authenticated via environment variables."""
    from azure.storage.blob import BlobServiceClient
    from kindling_cli.cli import _resolve_account_url, create_azure_credential

    storage_account = os.getenv("AZURE_STORAGE_ACCOUNT")
    if not storage_account:
        pytest.skip("AZURE_STORAGE_ACCOUNT not set")

    credential = create_azure_credential(additionally_allowed_tenants=["*"])
    return BlobServiceClient(
        account_url=_resolve_account_url(storage_account),
        credential=credential,
    )


@pytest.fixture
def static_values_test_app(platform_client, blob_client):
    """Upload the test CSV and deploy the static_values test app."""
    api_client, _ = platform_client
    suffix = str(uuid.uuid4())[:8]
    app_name = f"systest-fi-static-{suffix}"
    job_name = f"systest-fi-static-job-{suffix}"

    container = os.getenv("AZURE_CONTAINER", "artifacts")
    base_path = os.getenv("AZURE_BASE_PATH", "").rstrip("/")
    blob_folder = f"{base_path}/systest-fi/{suffix}" if base_path else f"systest-fi/{suffix}"
    blob_csv_path = f"{blob_folder}/test_static.csv"

    # Upload the test CSV from the CI runner via BlobServiceClient so the
    # cloud job can read it over ABFS without needing write permissions.
    container_client = blob_client.get_container_client(container)
    container_client.upload_blob(blob_csv_path, _csv_bytes(), overwrite=True)
    print(f"Uploaded test CSV: {blob_csv_path}")

    abfss_base = _test_abfss_path()
    csv_folder = f"{abfss_base}/systest-fi/{suffix}"
    entity_table_path = f"{abfss_base}/systest-fi/{suffix}/entity"

    expected_source = "test_erp"
    expected_env = "ci"

    job_config = {
        "job_name": job_name,
        "app_name": app_name,
        "entry_point": "app.py",
        "test_id": suffix,
        "config_overrides": {"kindling": {"artifacts_storage_path": abfss_base}},
    }

    app_files = {
        "app.py": _app_src(
            csv_abfss_folder=csv_folder,
            entity_table_path=entity_table_path,
            expected_source=expected_source,
            expected_env=expected_env,
        ),
    }

    api_client.deploy_app(app_name, app_files)

    yield api_client, app_name, job_name, job_config

    # Clean up the uploaded CSV
    try:
        container_client.delete_blob(blob_csv_path)
        print(f"Cleaned up test CSV: {blob_csv_path}")
    except Exception as exc:
        print(f"Warning: could not clean up CSV: {exc}")

    try:
        api_client.cleanup_app(app_name)
    except Exception as exc:
        print(f"Warning: app cleanup failed: {exc}")


# ── test ───────────────────────────────────────────────────────────────────────


@pytest.mark.system
class TestFileIngestionStaticValues:
    """Verify that static_values on a file ingestion entry are applied as literal columns."""

    def test_static_values_appear_as_columns_after_ingestion(
        self, static_values_test_app, stdout_validator
    ):
        """
        CSV has two columns (row_id, value).
        Ingestion entry adds static_values: source_system, environment.
        After ingestion the entity table must have all four columns,
        with every row carrying the expected static values.
        """
        api_client, app_name, job_name, job_config = static_values_test_app

        result = api_client.create_job(job_name=job_name, job_config=job_config)
        job_id = result["job_id"]
        print(f"Job created: {job_id}")

        try:
            run_id = api_client.run_job(job_id=job_id)
            assert run_id is not None

            stdout_validator.stream_with_callback(
                job_id=job_id,
                run_id=run_id,
                print_lines=True,
                poll_interval=get_system_test_poll_interval(10.0),
                max_wait=get_system_test_stream_max_wait(900.0),
            )

            log = stdout_validator.get_content()
            assert_no_fatal_system_test_log_lines(log)

            assert MARKER_FAILED not in log, (
                "App reported a validation failure — static columns missing or wrong value. "
                f"Log tail: {log[-500:]}"
            )

            assert (
                MARKER_PASSED in log
                or "completed successfully" in log
                or "BOOTSTRAP COMPLETE" in log
            ), (
                "App did not complete — possible timeout or crash. "
                f"(log length={len(log)} chars)"
            )

        finally:
            try:
                api_client.cancel_job(run_id=run_id)
            except Exception:
                pass
            api_client.delete_job(job_id=job_id)
