"""``system.runs``: one durable row per Kindling initialization (opt-in).

Turned on with ``kindling.runs.enabled: true``. Bootstrap inserts the row
when initialization completes (status ``running``) and updates it when an
auto-run app ends (``succeeded`` or ``failed``, with the end time and the
executor run ids made under the run). A run that is killed keeps
``running``; a session without an auto-run app keeps its start row.

The row comes from the runtime manifest (``kindling.runtime_manifest``) and
joins to provenance stamps on ``run_id``; the full manifest is kept as JSON.
Defined like ``system.watermarks`` and written through the same entity
provider. Writing it never breaks bootstrap.
"""

import json
from datetime import datetime, timezone
from types import SimpleNamespace
from typing import Any, Optional

from pyspark.sql.types import (
    ArrayType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

STATUS_RUNNING = "running"
STATUS_SUCCEEDED = "succeeded"
STATUS_FAILED = "failed"

RUNS_SCHEMA = StructType(
    [
        StructField("run_id", StringType(), False),
        StructField("started_at", TimestampType(), False),
        StructField("ended_at", TimestampType(), True),
        StructField("status", StringType(), False),
        StructField("error", StringType(), True),
        StructField("app_name", StringType(), True),
        StructField("app_version", StringType(), True),
        StructField("platform", StringType(), True),
        StructField("platform_runtime_version", StringType(), True),
        StructField("spark_version", StringType(), True),
        StructField("python_version", StringType(), True),
        StructField("kindling_version", StringType(), True),
        StructField("runtime_match", StringType(), True),
        StructField("runtime_fingerprint", StringType(), True),
        StructField("package_fingerprint", StringType(), True),
        StructField("execution_ids", ArrayType(StringType()), True),
        StructField("manifest", StringType(), True),
    ]
)

RUNS_ENTITY = SimpleNamespace(
    entityid="system.runs",
    name="runs",
    schema=RUNS_SCHEMA,
    partition_columns=[],
    merge_columns=["run_id"],
    tags={"provider_type": "delta"},
)


def runs_enabled(config_service: Any) -> bool:
    """Whether ``kindling.runs.enabled`` is on (default off)."""
    if config_service is None:
        return False
    enabled = config_service.get("kindling.runs.enabled", False)
    return str(enabled).strip().lower() in ("true", "1", "yes")


def run_row(manifest, status: str, error: Optional[str] = None, ended_at=None) -> tuple:
    """The ``system.runs`` row for ``manifest`` (in ``RUNS_SCHEMA`` order)."""
    match = manifest.runtime_match
    return (
        manifest.run_id,
        _parse_timestamp(manifest.started_at),
        ended_at,
        status,
        error,
        manifest.app_name,
        manifest.app_version,
        manifest.platform,
        manifest.platform_runtime_version,
        manifest.spark_version,
        manifest.python_version,
        manifest.kindling_version,
        match.label() if match is not None else None,
        manifest.runtime_fingerprint,
        manifest.package_fingerprint,
        list(manifest.execution_ids),
        json.dumps(manifest.to_dict(), sort_keys=True, default=str),
    )


def record_run(
    config_service: Any,
    status: str,
    error: Optional[str] = None,
    logger: Any = None,
) -> bool:
    """Upsert this initialization's ``system.runs`` row when runs are
    enabled. Returns whether a row was written; a failure is logged as a
    warning, never raised."""
    try:
        if not runs_enabled(config_service):
            return False
        from kindling.runtime_manifest import get_runtime_manifest

        manifest = get_runtime_manifest()
        if manifest is None:
            return False
        from kindling.data_entities import EntityProvider
        from kindling.injection import GlobalInjector
        from kindling.spark_session import get_or_create_spark_session

        ended_at = None if status == STATUS_RUNNING else datetime.now(timezone.utc)
        row = run_row(manifest, status, error=error, ended_at=ended_at)
        df = get_or_create_spark_session().createDataFrame([row], RUNS_SCHEMA)
        GlobalInjector.get(EntityProvider).merge_to_entity(df, RUNS_ENTITY)
        return True
    except Exception as record_error:
        if logger is not None:
            logger.warning(f"Could not record the run in system.runs: {record_error}")
        return False


def _parse_timestamp(value: Optional[str]) -> datetime:
    """The manifest's ISO ``started_at`` as an aware datetime (PySpark
    converts aware datetimes exactly; naive ones are taken as local time)."""
    if not value:
        return datetime.now(timezone.utc)
    parsed = datetime.fromisoformat(value)
    return parsed if parsed.tzinfo is not None else parsed.replace(tzinfo=timezone.utc)
