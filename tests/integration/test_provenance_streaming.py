"""Provenance stamping on a stream started by SimplePipeStreamStarter.

A stamped Delta source is read as a stream into a stamped SCD2 entity
through the real starter and the Delta provider's ``merge_as_stream``:

- the input's stamp is dropped on the streaming read (otherwise the output
  stamp would collide with it);
- each row carries the streaming pipe's stamp: mode ``streaming``, the
  execution id that started the stream, no persist id;
- ``processed_at`` is per micro-batch, and an unchanged row keeps its stamp.
"""

import shutil
import tempfile
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from delta import configure_spark_with_delta_pip
from kindling.data_entities import EntityNameMapper, EntityPathLocator
from kindling.data_pipes import PipeMetadata
from kindling.entity_provider_delta import DeltaEntityProvider
from kindling.pipe_streaming import SimplePipeStreamStarter
from kindling.provenance import ProvenanceStamp, stamp_provenance
from kindling.spark_config import ConfigService
from kindling.spark_log_provider import PythonLoggerProvider
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructField, StructType

from tests.spark_test_helper import (
    _teardown_non_delta_jvm,
    _worker_scoped_ivy_dir,
    set_spark_log_level,
)


@pytest.fixture(scope="module")
def spark():
    _teardown_non_delta_jvm()
    builder = (
        SparkSession.builder.appName("ProvenanceStreaming")
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
    temp_path = tempfile.mkdtemp(prefix="kindling-provenance-stream-test-")
    yield Path(temp_path)
    shutil.rmtree(temp_path, ignore_errors=True)


class _Config:
    """ConfigService stand-in for the starter and the provider."""

    def __init__(self, values):
        self._values = values

    def get(self, key, default=None):
        return self._values.get(key, default)


@pytest.fixture
def path_locator(temp_dir):
    locator = MagicMock(spec=EntityPathLocator)
    locator.get_table_path.side_effect = lambda entity: str(temp_dir / entity.entityid)
    return locator


@pytest.fixture
def delta_provider(spark, path_locator, monkeypatch):
    monkeypatch.setattr("kindling.entity_provider_delta.get_or_create_spark_session", lambda: spark)
    config = MagicMock(spec=ConfigService)
    config.get.side_effect = lambda key, default=None: (
        "storage" if key == "kindling.delta.access_mode" else default
    )
    name_mapper = MagicMock(spec=EntityNameMapper)
    name_mapper.get_table_name.side_effect = lambda entity: entity.entityid.replace(".", "_")
    logger_provider = MagicMock(spec=PythonLoggerProvider)
    logger_provider.get_logger.return_value = MagicMock()
    return DeltaEntityProvider(
        config=config,
        entity_name_mapper=name_mapper,
        path_locator=path_locator,
        tp=logger_provider,
        signal_provider=None,
    )


SCHEMA = StructType(
    [
        StructField("customer_id", StringType(), False),
        StructField("status", StringType(), True),
    ]
)


def _entity(entityid, tags):
    return SimpleNamespace(
        entityid=entityid,
        name=entityid.split(".")[-1],
        partition_columns=[],
        cluster_columns=None,
        merge_columns=["customer_id"],
        tags={"provider_type": "delta", "provenance.enabled": "true", **tags},
        schema=SCHEMA,
    )


def test_stream_stamps_each_micro_batch(spark, delta_provider, path_locator, temp_dir, monkeypatch):
    source = _entity("bronze.events", {})
    output = _entity("silver.customers", {"scd.type": "2"})
    pipe = PipeMetadata(
        pipeid="silver.stream_customers",
        name="Stream customers",
        execute=lambda bronze_events: bronze_events,
        tags={},
        input_entity_ids=[source.entityid],
        output_entity_id=output.entityid,
        output_type="delta",
    )
    entities = {source.entityid: source, output.entityid: output}

    def append_source(rows):
        df = stamp_provenance(
            spark.createDataFrame(rows, SCHEMA),
            source,
            lambda: ProvenanceStamp(pipe_id="bronze.ingest", mode="batch"),
        )
        delta_provider.append_to_entity(df, source)

    registry = MagicMock()
    registry.get_provider_for_entity.return_value = delta_provider
    der = MagicMock()
    der.get_entity_definition.side_effect = entities.get
    dpr = MagicMock()
    dpr.get_pipe_definition.return_value = pipe
    plp = MagicMock(spec=PythonLoggerProvider)
    plp.get_logger.return_value = MagicMock()
    starter = SimplePipeStreamStarter(
        _Config(
            {
                "kindling.storage.checkpoint_root": str(temp_dir / "checkpoints"),
                "kindling.delta.access_mode": "storage",
            }
        ),
        dpr,
        registry,
        der,
        path_locator,
        plp,
    )
    monkeypatch.setattr(
        "kindling.pipe_streaming.current_execution_id",
        lambda pipe_id: "stream-run-1" if pipe_id == pipe.pipeid else None,
    )

    def run_stream():
        starter.start_pipe_stream(
            pipe.pipeid, {"trigger": {"availableNow": True}}
        ).awaitTermination()

    append_source([("c1", "bronze")])
    run_stream()
    (first,) = delta_provider.read_entity(output).collect()
    stamp = first["__provenance"]
    assert stamp["pipe_id"] == "silver.stream_customers", "the input's stamp was dropped"
    assert stamp["mode"] == "streaming"
    assert stamp["execution_id"] == "stream-run-1"
    assert stamp["persist_id"] is None
    assert stamp["input_entity_ids"] == ["bronze.events"]

    append_source([("c1", "bronze"), ("c2", "silver")])
    run_stream()
    rows = {row["customer_id"]: row for row in delta_provider.read_entity(output).collect()}
    assert len(rows) == 2, "c1 is unchanged: no new version"
    assert rows["c1"]["__provenance"] == stamp, "an unchanged row keeps its stamp"
    assert (
        rows["c2"]["__provenance"]["processed_at"] > stamp["processed_at"]
    ), "processed_at is per micro-batch"
