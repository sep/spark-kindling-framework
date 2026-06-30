"""
System test: DataPipes.view() execution and CSV write/read roundtrip on cloud platforms.

Verifies that:
  1. DataPipes.view() can be registered with inline SQL and executed against a
     Spark DataFrame on the target cloud platform.
  2. The view output can be written to a local CSV path via CSVEntityProvider
     and read back correctly.

This test does not require ABFS/cloud object storage — it writes to the cluster's
local filesystem.  What it validates is that kindling's view and CSV write code
paths work correctly in the platform's Spark environment, where Spark version,
serialization, and JVM configuration can differ from local development.

Run:
    poe test-system --test test_view_and_csv_roundtrip
"""

import uuid

import pytest

from tests.system.test_helpers import (
    apply_env_config_overrides,
    assert_no_fatal_system_test_log_lines,
    create_platform_client,
    get_system_test_poll_interval,
    get_system_test_stream_max_wait,
    wait_for_job_terminal_teardown,
)

MARKER_PASSED = "VIEW_CSV_TEST: PASSED"
MARKER_FAILED = "VIEW_CSV_TEST: FAILED"


def _app_src() -> str:
    """Generate the self-contained test app that exercises view() and CSV write."""
    return """\
import logging
import sys
import tempfile
import traceback
from pathlib import Path

_log = logging.getLogger("view_csv_roundtrip_test")


class _LogProvider:
    def get_logger(self, name=""):
        return logging.getLogger(f"kindling.{name}")


try:
    from pyspark.sql import SparkSession
    from pyspark.sql.types import IntegerType, StringType, StructField, StructType

    from kindling.data_entities import EntityMetadata
    from kindling.data_pipes import DataPipes, DataPipesManager
    from kindling.entity_provider_csv import CSVEntityProvider

    # ── Step 1: get active Spark session and build test input ─────────────────
    spark = SparkSession.getActiveSession() or SparkSession.builder.getOrCreate()

    schema = StructType([
        StructField("id", IntegerType(), False),
        StructField("label", StringType(), True),
        StructField("amount", IntegerType(), True),
    ])
    input_df = spark.createDataFrame(
        [(1, "alpha", 150), (2, "beta", -10), (3, "gamma", 300)],
        schema=schema,
    )
    print("VIEW_CSV_TEST: input DataFrame created", flush=True)

    # ── Step 2: register DataPipes.view() with inline SQL ─────────────────────
    logger_provider = _LogProvider()
    registry = DataPipesManager(logger_provider)
    DataPipes.reset()
    DataPipes.dpregistry = registry

    DataPipes.view(
        pipeid="view.positive",
        input_entity_ids=["test.input"],
        output_entity_id="view.positive",
        sql="SELECT id, label FROM test_input WHERE amount > 0",
    )
    print("VIEW_CSV_TEST: view pipe registered", flush=True)

    # ── Step 3: execute the view directly ─────────────────────────────────────
    pipe = registry.get_pipe_definition("view.positive")
    result_df = pipe.execute(test_input=input_df)
    result_rows = result_df.collect()
    print(f"VIEW_CSV_TEST: view returned {len(result_rows)} rows", flush=True)

    if len(result_rows) != 2:
        msg = f"VIEW_CSV_TEST: FAILED — view returned {len(result_rows)} rows, expected 2"
        _log.warning(msg)
        print(msg, flush=True)
        sys.exit(1)

    labels = {r["label"] for r in result_rows}
    if labels != {"alpha", "gamma"}:
        msg = f"VIEW_CSV_TEST: FAILED — view returned labels {labels}, expected {{'alpha', 'gamma'}}"
        _log.warning(msg)
        print(msg, flush=True)
        sys.exit(1)

    print("VIEW_CSV_TEST: view output validated", flush=True)

    # ── Step 4: write view output to CSV ──────────────────────────────────────
    tmp_dir = Path(tempfile.mkdtemp())
    csv_path = str(tmp_dir / "view_output")

    csv_provider = CSVEntityProvider(logger_provider)
    entity_meta = EntityMetadata(
        entityid="view.positive",
        name="View Output",
        merge_columns=[],
        tags={"provider.path": csv_path, "provider.header": "true"},
        schema=None,
    )
    csv_provider.write_to_entity(result_df, entity_meta)
    print("VIEW_CSV_TEST: CSV written", flush=True)

    # ── Step 5: read CSV back and validate ────────────────────────────────────
    read_back = spark.read.option("header", "true").csv(csv_path)
    read_rows = read_back.collect()
    print(f"VIEW_CSV_TEST: CSV has {len(read_rows)} rows", flush=True)

    if len(read_rows) != 2:
        msg = f"VIEW_CSV_TEST: FAILED — CSV has {len(read_rows)} rows, expected 2"
        _log.warning(msg)
        print(msg, flush=True)
        sys.exit(1)

    csv_labels = {r["label"] for r in read_rows}
    if csv_labels != {"alpha", "gamma"}:
        msg = f"VIEW_CSV_TEST: FAILED — CSV labels {csv_labels}, expected {{'alpha', 'gamma'}}"
        _log.warning(msg)
        print(msg, flush=True)
        sys.exit(1)

    print("VIEW_CSV_TEST: CSV round-trip validated", flush=True)

    # ── Step 6: verify append_to_entity adds rows ─────────────────────────────
    extra_df = spark.createDataFrame([(4, "delta")], ["id", "label"])
    csv_provider.append_to_entity(extra_df, entity_meta)

    appended = spark.read.option("header", "true").csv(csv_path)
    appended_count = appended.count()
    if appended_count != 3:
        msg = f"VIEW_CSV_TEST: FAILED — after append, CSV has {appended_count} rows, expected 3"
        _log.warning(msg)
        print(msg, flush=True)
        sys.exit(1)

    print("VIEW_CSV_TEST: append_to_entity validated", flush=True)
    _log.warning("VIEW_CSV_TEST: PASSED")
    print("VIEW_CSV_TEST: PASSED", flush=True)
    sys.exit(0)

except SystemExit:
    raise
except Exception as _exc:
    tb = traceback.format_exc()
    msg = f"VIEW_CSV_TEST: FAILED — exception: {_exc}"
    _log.exception(msg)
    print(msg, flush=True)
    print(tb, flush=True)
    sys.exit(1)
"""


@pytest.fixture
def view_csv_test_app(platform_client):
    """Deploy the view+CSV roundtrip test app."""
    api_client, platform_name = platform_client
    suffix = str(uuid.uuid4())[:8]
    app_name = f"systest-view-csv-{suffix}"
    job_name = f"systest-view-csv-job-{suffix}"

    job_config = {
        "job_name": job_name,
        "app_name": app_name,
        "entry_point": "app.py",
        "test_id": suffix,
    }
    job_config = apply_env_config_overrides(job_config, platform_name)

    app_files = {"app.py": _app_src()}
    api_client.deploy_app(app_name, app_files)

    yield api_client, app_name, job_name, job_config

    try:
        api_client.cleanup_app(app_name)
    except Exception as exc:
        print(f"Warning: app cleanup failed: {exc}")


@pytest.mark.system
class TestViewAndCsvRoundtrip:
    """Verify DataPipes.view() execution and CSV write/read roundtrip on cloud platforms."""

    def test_view_executes_and_csv_roundtrip_passes(
        self, platform_client, view_csv_test_app, stdout_validator
    ):
        """
        Deploy an app that:
          1. Creates a Spark DataFrame with 3 rows.
          2. Registers a DataPipes.view() filtering to rows with amount > 0.
          3. Executes the view and validates the 2-row result.
          4. Writes to local CSV via CSVEntityProvider.write_to_entity().
          5. Reads back and validates the CSV.
          6. Appends 1 row and verifies the count reaches 3.

        Confirms that both view() and CSV write/append work in the platform's Spark
        environment.
        """
        _, platform_name = platform_client
        api_client, app_name, job_name, job_config = view_csv_test_app

        result = api_client.create_job(job_name=job_name, job_config=job_config)
        job_id = result["job_id"]
        print(f"Job created: {job_id}")

        run_id = None
        try:
            run_id = api_client.run_job(job_id=job_id)
            assert run_id is not None

            stdout_validator.stream_with_callback(
                job_id=job_id,
                run_id=run_id,
                print_lines=True,
                poll_interval=get_system_test_poll_interval(10.0),
                max_wait=get_system_test_stream_max_wait(600.0, platform_name),
            )

            log = stdout_validator.get_content()
            assert_no_fatal_system_test_log_lines(log)

            assert MARKER_FAILED not in log, (
                "App reported a validation failure. " f"Log tail: {log[-500:]}"
            )

            assert (
                MARKER_PASSED in log
                or "completed successfully" in log
                or "BOOTSTRAP COMPLETE" in log
            ), (
                "App did not report success — possible timeout or crash. "
                f"(log length={len(log)} chars)"
            )

        finally:
            try:
                api_client.cancel_job(run_id=run_id)
            except Exception:
                pass
            if run_id is not None:
                wait_for_job_terminal_teardown(api_client, run_id, platform_name)
            api_client.delete_job(job_id=job_id)

            from tests.system.test_helpers import cleanup_test_storage

            cleanup_test_storage(platform_name, job_config["test_id"])
