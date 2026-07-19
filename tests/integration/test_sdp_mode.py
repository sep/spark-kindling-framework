"""Integration tests for SDP mode against real Spark and real Delta.

Two seams the Phase-2 unit tests cover only with fakes are exercised for
real here (in the repo's Spark 3.5 environment — neither needs
``pyspark.pipelines``):

- The write guard wrapping a REAL ``DeltaEntityProvider``: reads delegate
  to actual Delta tables; every write path raises ``SdpModeWriteError``
  before touching storage.
- The dataset function ``OssSdpEngine`` emits, evaluated against a REAL
  Spark session: a real ``spark.table()`` read feeding the pipe's execute
  callable, producing a real DataFrame result — exactly what SDP evaluates
  at pipeline run time.

The real ``spark-pipelines dry-run`` (Spark 4.1) lives in
``test_sdp_dry_run_real.py``; platform system tests are deliberately out
of scope until the Databricks adapter (Phases 3/5) gives SDP a cloud
execution story.
"""

import shutil
import tempfile
from pathlib import Path
from unittest.mock import MagicMock

import pytest
from delta import configure_spark_with_delta_pip
from kindling_ext_sdp import OssSdpEngine, SdpModeWriteError, SdpWriteGuardProvider
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

from kindling.data_entities import EntityMetadata, EntityNameMapper, EntityPathLocator
from kindling.data_pipes import PipeMetadata
from kindling.entity_provider_delta import DeltaEntityProvider
from kindling.spark_config import ConfigService
from kindling.spark_log_provider import PythonLoggerProvider


def _teardown_existing_spark_jvm():
    """See test_watermark_incremental_correctness — Delta jars are only on
    the classpath if this module's session launches the JVM."""
    from pyspark import SparkContext

    active = SparkSession.getActiveSession()
    if active is not None:
        active.stop()
    if SparkContext._gateway is not None:
        try:
            SparkContext._gateway.shutdown()
        except Exception:
            pass
        SparkContext._gateway = None
        SparkContext._jvm = None


@pytest.fixture(scope="module")
def warehouse_dir():
    temp_path = tempfile.mkdtemp(prefix="kindling-sdp-mode-test-")
    yield Path(temp_path)
    shutil.rmtree(temp_path, ignore_errors=True)


@pytest.fixture(scope="module")
def spark(warehouse_dir):
    _teardown_existing_spark_jvm()
    builder = (
        SparkSession.builder.appName("SdpModeIntegration")
        .master("local[2]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.sql.warehouse.dir", str(warehouse_dir / "warehouse"))
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
    )
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    yield spark
    spark.stop()


@pytest.fixture
def temp_dir():
    temp_path = tempfile.mkdtemp(prefix="kindling-sdp-guard-test-")
    yield Path(temp_path)
    shutil.rmtree(temp_path, ignore_errors=True)


@pytest.fixture
def delta_provider(spark, temp_dir, monkeypatch):
    monkeypatch.setattr("kindling.entity_provider_delta.get_or_create_spark_session", lambda: spark)
    config = MagicMock(spec=ConfigService)
    config.get.side_effect = lambda key, default=None: (
        "storage" if key == "kindling.delta.access_mode" else default
    )
    name_mapper = MagicMock(spec=EntityNameMapper)
    name_mapper.get_table_name.side_effect = lambda entity: entity.entityid.replace(".", "_")
    path_locator = MagicMock(spec=EntityPathLocator)
    path_locator.get_table_path.side_effect = lambda entity: str(
        Path(str(temp_dir)) / entity.entityid
    )
    logger_provider = MagicMock(spec=PythonLoggerProvider)
    logger_provider.get_logger.return_value = MagicMock()
    return DeltaEntityProvider(
        config=config,
        entity_name_mapper=name_mapper,
        path_locator=path_locator,
        tp=logger_provider,
        signal_provider=None,
    )


ORDERS_SCHEMA = StructType(
    [
        StructField("order_id", IntegerType(), False),
        StructField("status", StringType(), True),
    ]
)


def make_entity(entityid, tags=None):
    return EntityMetadata(
        entityid=entityid,
        name=entityid.split(".")[-1],
        merge_columns=["order_id"],
        tags={"provider_type": "delta", **(tags or {})},
        schema=ORDERS_SCHEMA,
    )


class TestWriteGuardOnRealDeltaProvider:
    def test_reads_delegate_to_real_delta_writes_raise(self, spark, delta_provider):
        entity = make_entity("silver.orders_guard")
        df = spark.createDataFrame([(1, "open"), (2, "closed")], ORDERS_SCHEMA)
        delta_provider.write_to_entity(df, entity)  # seed BEFORE guarding

        guarded = SdpWriteGuardProvider(delta_provider)

        assert guarded.check_entity_exists(entity) is True
        rows = {r["order_id"]: r["status"] for r in guarded.read_entity(entity).collect()}
        assert rows == {1: "open", 2: "closed"}

        for method in ("write_to_entity", "append_to_entity", "merge_to_entity"):
            with pytest.raises(SdpModeWriteError, match="silver.orders_guard"):
                getattr(guarded, method)(df, entity)

        rows_after = guarded.read_entity(entity).collect()
        assert len(rows_after) == 2, "refused writes must not have touched the table"


class FakeDpModule:
    def __init__(self):
        self.declared = {}

    def materialized_view(self, name=None, **kwargs):
        def decorator(fn):
            self.declared[name or fn.__name__] = fn
            return fn

        return decorator


class FakeRegistry(dict):
    def get_entity_ids(self):
        return self.keys()

    get_entity_definition = dict.get

    def get_pipe_ids(self):
        return self.keys()

    get_pipe_definition = dict.get


class TestEmittedDatasetFunctionOnRealSpark:
    def test_dataset_function_reads_real_table_and_transforms(self, spark):
        """The emitted dataset function end-to-end on a real session: a
        real spark.table() read of the external input, the pipe's execute
        receiving a real DataFrame under the runner kwarg contract, and a
        real transformed DataFrame out — what SDP evaluates at run time."""
        spark.sql("CREATE DATABASE IF NOT EXISTS src")
        spark.sql("DROP TABLE IF EXISTS src.orders")
        spark.createDataFrame(
            [(1, "open"), (2, "closed"), (3, "open")], ORDERS_SCHEMA
        ).write.format("delta").saveAsTable("src.orders")

        def silver_execute(**dfs):
            return dfs["src_orders"].filter("status = 'open'").select("order_id")

        entities = FakeRegistry(
            {e.entityid: e for e in [make_entity("src.orders"), make_entity("silver.orders")]}
        )
        pipes = FakeRegistry(
            {
                "src_to_silver.orders": PipeMetadata(
                    pipeid="src_to_silver.orders",
                    name="src_to_silver.orders",
                    execute=silver_execute,
                    tags={},
                    input_entity_ids=["src.orders"],
                    output_entity_id="silver.orders",
                    output_type="delta",
                )
            }
        )

        dp = FakeDpModule()
        engine = OssSdpEngine(entities, pipes, dp_module=dp, session_provider=lambda: spark)
        engine.declare_pipeline(engine.build_plan())

        result = dp.declared["silver.orders"]()

        assert sorted(r["order_id"] for r in result.collect()) == [1, 3]
