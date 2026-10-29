"""system.runs: the opt-in run record built from the runtime manifest."""

import json
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest
from kindling import run_records as rr
from kindling import runtime_manifest as rm
from kindling.injection import GlobalInjector


class _Config:
    def __init__(self, values=None):
        self.values = values or {}

    def get(self, key, default=None):
        return self.values.get(key, default)


ENABLED = _Config({"kindling.runs.enabled": True})


@pytest.fixture(autouse=True)
def _manifest():
    rm.reset_runtime_manifest()
    manifest = rm.begin_runtime_manifest(platform="databricks")
    manifest.app_name, manifest.app_version = "orders", "1.2.0"
    manifest.execution_ids.append("exec-1")
    yield manifest
    rm.reset_runtime_manifest()


@pytest.mark.parametrize(
    "value, enabled",
    [(None, False), (False, False), ("false", False), (True, True), ("true", True), ("yes", True)],
)
def test_runs_are_opt_in(value, enabled):
    assert rr.runs_enabled(_Config({"kindling.runs.enabled": value})) is enabled
    assert rr.runs_enabled(None) is False


def test_the_row_comes_from_the_manifest(_manifest):
    row = dict(zip(rr.RUNS_SCHEMA.fieldNames(), rr.run_row(_manifest, "running")))
    assert row["run_id"] == _manifest.run_id
    assert row["started_at"].tzinfo is not None
    assert row["started_at"] == datetime.fromisoformat(_manifest.started_at)
    assert (row["status"], row["ended_at"], row["error"]) == ("running", None, None)
    assert (row["app_name"], row["app_version"], row["platform"]) == (
        "orders",
        "1.2.0",
        "databricks",
    )
    assert row["execution_ids"] == ["exec-1"]
    assert json.loads(row["manifest"])["run_id"] == _manifest.run_id


def _record(config, status, **kwargs):
    provider = MagicMock()
    spark = MagicMock()
    with (
        patch.object(GlobalInjector, "get", return_value=provider),
        patch("kindling.spark_session.get_or_create_spark_session", return_value=spark),
    ):
        written = rr.record_run(config, status, **kwargs)
    return written, provider, spark


def test_nothing_is_written_when_disabled():
    written, provider, spark = _record(_Config(), "running")
    assert written is False
    provider.merge_to_entity.assert_not_called()
    spark.createDataFrame.assert_not_called()


def test_the_start_row_is_running_with_no_end():
    written, provider, spark = _record(ENABLED, "running")
    assert written is True
    (rows, schema), _ = spark.createDataFrame.call_args
    row = dict(zip(schema.fieldNames(), rows[0]))
    assert (row["status"], row["ended_at"]) == ("running", None)
    df, entity = provider.merge_to_entity.call_args.args
    assert entity is rr.RUNS_ENTITY and entity.merge_columns == ["run_id"]


def test_the_end_row_has_status_end_time_and_error():
    _, _, spark = _record(ENABLED, "failed", error="boom")
    (rows, schema), _ = spark.createDataFrame.call_args
    row = dict(zip(schema.fieldNames(), rows[0]))
    assert (row["status"], row["error"]) == ("failed", "boom")
    assert row["ended_at"] <= datetime.now(timezone.utc)


def test_a_write_failure_is_a_warning_never_an_error():
    provider = MagicMock()
    provider.merge_to_entity.side_effect = RuntimeError("no storage")
    logger = MagicMock()
    with (
        patch.object(GlobalInjector, "get", return_value=provider),
        patch("kindling.spark_session.get_or_create_spark_session", return_value=MagicMock()),
    ):
        assert rr.record_run(ENABLED, "running", logger=logger) is False
    assert "no storage" in logger.warning.call_args.args[0]


def test_no_manifest_no_row():
    rm.reset_runtime_manifest()
    written, provider, _ = _record(ENABLED, "running")
    assert written is False
    provider.merge_to_entity.assert_not_called()
