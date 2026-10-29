"""``kindling migrate`` and the columns the framework adds to a table.

SCD2 temporal, surrogate key and provenance columns are not in the declared
schema; the planner must compare against the table the framework creates, so
it never plans them for removal, adds a missing provenance column, and
refuses to add missing SCD2 or key columns as nulls.
"""

import shutil
import tempfile
from datetime import datetime
from pathlib import Path
from unittest.mock import MagicMock

import pytest
from delta import configure_spark_with_delta_pip
from kindling.data_entities import EntityMetadata, EntityNameMapper, EntityPathLocator
from kindling.entity_provider_delta import DeltaEntityProvider, SchemaDriftError
from kindling.migration import (
    ChangeDestructiveness,
    ChangeKind,
    MigrationApplier,
    MigrationPlanner,
)
from kindling.provenance import ProvenanceStamp, stamp_provenance
from kindling.spark_config import ConfigService
from kindling.spark_log_provider import PythonLoggerProvider
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructField, StructType, TimestampType

from tests.spark_test_helper import (
    _teardown_non_delta_jvm,
    _worker_scoped_ivy_dir,
    set_spark_log_level,
)

SCHEMA = StructType(
    [
        StructField("customer_id", StringType(), False),
        StructField("tier", StringType(), True),
        StructField("updated_at", TimestampType(), True),
    ]
)
PROVENANCE = {"provenance.enabled": "true"}
SCD2 = {"scd.type": "2", "scd.sequence_by": "updated_at"}


@pytest.fixture(scope="module")
def spark():
    _teardown_non_delta_jvm()
    builder = (
        SparkSession.builder.appName("MigrationFrameworkColumns")
        .config("spark.jars.ivy", _worker_scoped_ivy_dir())
        .master("local[2]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
    )
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    set_spark_log_level(spark, "ERROR")
    yield spark
    spark.stop()


@pytest.fixture
def temp_dir():
    temp_path = tempfile.mkdtemp(prefix="kindling-migration-test-")
    yield Path(temp_path)
    shutil.rmtree(temp_path, ignore_errors=True)


@pytest.fixture
def logger_provider():
    tp = MagicMock(spec=PythonLoggerProvider)
    tp.get_logger.return_value = MagicMock()
    return tp


@pytest.fixture
def provider(spark, temp_dir, logger_provider, monkeypatch):
    monkeypatch.setattr("kindling.entity_provider_delta.get_or_create_spark_session", lambda: spark)
    monkeypatch.setattr("kindling.spark_session.get_or_create_spark_session", lambda: spark)
    config = MagicMock(spec=ConfigService)
    config.get.side_effect = lambda key, default=None: (
        "storage" if key == "kindling.delta.access_mode" else default
    )
    name_mapper = MagicMock(spec=EntityNameMapper)
    name_mapper.get_table_name.side_effect = lambda entity: entity.entityid.replace(".", "_")
    path_locator = MagicMock(spec=EntityPathLocator)
    path_locator.get_table_path.side_effect = lambda entity: str(temp_dir / entity.entityid)
    return DeltaEntityProvider(
        config=config,
        entity_name_mapper=name_mapper,
        path_locator=path_locator,
        tp=logger_provider,
        signal_provider=None,
    )


def _entity(entityid, tags):
    return EntityMetadata(entityid, entityid.split(".")[-1], ["customer_id"], dict(tags), SCHEMA)


def _plan(provider, logger_provider, entity):
    registry = MagicMock()
    registry.get_entity_ids.return_value = [entity.entityid]
    registry.get_entity_definition.return_value = entity
    plan = MigrationPlanner(registry, provider, logger_provider).plan()
    (status,) = plan.statuses
    return plan, status


def _stamped_batch(spark, entity):
    df = spark.createDataFrame([("c1", "gold", datetime(2026, 10, 1))], SCHEMA)
    return stamp_provenance(
        df, entity, lambda: ProvenanceStamp(pipe_id="p", persist_id="w1", mode="batch")
    )


def test_a_table_with_every_framework_column_is_up_to_date(provider, logger_provider):
    entity = _entity(
        "silver.full",
        {**SCD2, **PROVENANCE, "surrogate.column": "customer_sk"},
    )
    provider.ensure_entity_table(entity)

    _, status = _plan(provider, logger_provider, entity)

    assert status.error is None
    assert status.changes == [], [c.detail for c in status.changes]


def test_enabling_provenance_on_an_existing_table_is_a_safe_add(spark, provider, logger_provider):
    before = _entity("silver.prov_added", {"schema.drift": "fail"})
    provider.ensure_entity_table(before)
    after = _entity("silver.prov_added", {"schema.drift": "fail", **PROVENANCE})

    with pytest.raises(SchemaDriftError):
        provider.merge_to_entity(_stamped_batch(spark, after), after)

    plan, status = _plan(provider, logger_provider, after)
    (change,) = status.changes
    assert change.kind == ChangeKind.ADD_COLUMNS
    assert change.destructiveness == ChangeDestructiveness.SAFE
    assert change.columns_to_add == ["__provenance"]

    MigrationApplier(provider, logger_provider).apply(plan)
    provider.merge_to_entity(_stamped_batch(spark, after), after)

    (row,) = provider.read_entity(after).collect()
    assert row["__provenance"]["persist_id"] == "w1"
    assert _plan(provider, logger_provider, after)[1].changes == []


def test_missing_scd2_columns_need_a_rebuild(provider, logger_provider):
    plain = _entity("silver.became_scd2", {})
    provider.ensure_entity_table(plain)
    scd2 = _entity("silver.became_scd2", SCD2)

    plan, status = _plan(provider, logger_provider, scd2)

    assert status.changes == []
    assert "__effective_from" in status.error and "Rebuild" in status.error
    with pytest.raises(RuntimeError, match="have plan errors"):
        MigrationApplier(provider, logger_provider).apply(plan)
