"""End-to-end span-tree assertions for gh#210 comprehensive tracing.

Drives real pipe runs through DataPipesExecuter + SimpleReadPersistStrategy
with a RecordingTraceProvider and asserts the acceptance tree:

    run → pipe.run → read×N / persist → provider-op children

plus, in the delta variant, watermark read/save spans from the
WatermarkAspect flow (the local reproduction of cloud pipe behavior).
"""

import shutil
import tempfile
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, Mock, patch

import pytest
from kindling.data_entities import (
    DataEntityManager,
    EntityNameMapper,
    EntityPathLocator,
)
from kindling.data_pipes import DataPipesExecuter, DataPipesManager
from kindling.entity_provider import BaseEntityProvider, WritableEntityProvider
from kindling.signaling import BlinkerSignalProvider
from kindling.simple_read_persist_strategy import SimpleReadPersistStrategy
from kindling.spark_config import ConfigService
from kindling.spark_log_provider import PythonLoggerProvider
from kindling.test_framework import RecordingTraceProvider
from kindling.trace_ops import wrap_provider_ops
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

SOURCE_SCHEMA = StructType(
    [
        StructField("id", IntegerType(), False),
        StructField("label", StringType(), True),
    ]
)


@pytest.fixture
def mock_logger_provider():
    provider = MagicMock(spec=PythonLoggerProvider)
    provider.get_logger.return_value = MagicMock()
    return provider


def _register_entities(registry, *entityids):
    for entityid in entityids:
        registry.register_entity(
            entityid,
            name=entityid.split(".")[-1],
            partition_columns=[],
            merge_columns=["id"],
            tags={"provider_type": "delta"},
            schema=SOURCE_SCHEMA,
        )


def _child_ops(span):
    return [(child.component, child.operation) for child in span.children]


class _InMemoryBatchProvider(BaseEntityProvider, WritableEntityProvider):
    """Minimal real provider: serves and stores DataFrames in a dict."""

    def __init__(self, data):
        self.data = data
        self.written = {}

    def read_entity(self, entity_metadata):
        return self.data[entity_metadata.entityid]

    def check_entity_exists(self, entity_metadata):
        return False

    def write_to_entity(self, df, entity_metadata):
        self.written[entity_metadata.entityid] = df
        df.count()

    def append_to_entity(self, df, entity_metadata):
        self.written[entity_metadata.entityid] = df


class TestPipeRunSpanTree:
    """Acceptance: standalone run_datapipes yields run → pipe → read → persist."""

    @pytest.fixture(scope="class")
    def spark(self):
        spark = (
            SparkSession.builder.appName("TracingTreeTest")
            .master("local[2]")
            .config("spark.sql.shuffle.partitions", "2")
            .getOrCreate()
        )
        yield spark

    def test_run_datapipes_produces_expected_tree(self, spark, mock_logger_provider):
        tp = RecordingTraceProvider()

        entity_registry = DataEntityManager()
        _register_entities(entity_registry, "bronze.readings", "silver.readings")

        source_df = spark.createDataFrame([(1, "A"), (2, "B")], SOURCE_SCHEMA)
        provider = _InMemoryBatchProvider({"bronze.readings": source_df})
        wrap_provider_ops(provider, tp, provider_type="memory")

        provider_registry = Mock()
        provider_registry.get_provider_for_entity.return_value = provider

        strategy = SimpleReadPersistStrategy(
            ep=provider,
            der=entity_registry,
            tp=tp,
            lp=mock_logger_provider,
            provider_registry=provider_registry,
            signal_provider=None,
        )

        pipes_registry = DataPipesManager(mock_logger_provider)
        pipes_registry.register_pipe(
            "pipe.readings",
            name="readings",
            execute=lambda bronze_readings: bronze_readings,
            tags={},
            input_entity_ids=["bronze.readings"],
            output_entity_id="silver.readings",
            output_type="delta",
        )

        executer = DataPipesExecuter(
            lp=mock_logger_provider,
            dpe=entity_registry,
            dpr=pipes_registry,
            erps=strategy,
            tp=tp,
            signal_provider=None,
        )

        with patch("kindling.simple_read_persist_strategy._is_local_execution", return_value=False):
            executer.run_datapipes(["pipe.readings"])

        roots = tp.tree()
        assert len(roots) == 1, f"Expected one root, got {[r.operation for r in roots]}"
        run_span = roots[0]
        assert (run_span.component, run_span.operation) == ("kindling.pipes", "run")

        assert _child_ops(run_span) == [("kindling.pipes", "pipe.run")]
        pipe_span = run_span.children[0]
        assert pipe_span.details["pipe_id"] == "pipe.readings"

        pipe_children = _child_ops(pipe_span)
        assert ("kindling.pipes", "read") in pipe_children
        assert ("kindling.pipes", "persist") in pipe_children

        read_span = next(c for c in pipe_span.children if c.operation == "read")
        assert _child_ops(read_span) == [("kindling.entity.memory", "read_entity")]
        assert read_span.details["resolved"] is False

        persist_span = next(c for c in pipe_span.children if c.operation == "persist")
        assert ("kindling.entity.memory", "write_to_entity") in _child_ops(persist_span)

        assert "silver.readings" in provider.written
        assert all(span.closed for span in tp.spans), "Every span must be closed"


def _teardown_existing_spark_jvm():
    """See test_watermark_incremental_correctness: the Delta jars are only
    honored at JVM launch, so any prior plain session must be torn down."""
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


class TestDeltaWatermarkedSpanTree:
    """Delta variant: provider-op child spans plus the watermark save span."""

    @pytest.fixture(scope="class")
    def spark(self):
        from delta import configure_spark_with_delta_pip

        _teardown_existing_spark_jvm()
        builder = (
            SparkSession.builder.appName("TracingTreeDeltaTest")
            .master("local[2]")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            )
            .config("spark.databricks.delta.properties.defaults.enableChangeDataFeed", "true")
            .config("spark.sql.shuffle.partitions", "2")
            .config("spark.ui.enabled", "false")
        )
        spark = configure_spark_with_delta_pip(builder).getOrCreate()
        spark.sparkContext.setLogLevel("ERROR")
        yield spark
        spark.stop()

    @pytest.fixture
    def temp_dir(self):
        temp_path = tempfile.mkdtemp(prefix="kindling-tracing-tree-")
        yield Path(temp_path)
        shutil.rmtree(temp_path, ignore_errors=True)

    @pytest.fixture
    def delta_provider(self, spark, temp_dir, mock_logger_provider, monkeypatch):
        from kindling.entity_provider_delta import DeltaEntityProvider

        monkeypatch.setattr(
            "kindling.entity_provider_delta.get_or_create_spark_session", lambda: spark
        )
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
        return DeltaEntityProvider(
            config=config,
            entity_name_mapper=name_mapper,
            path_locator=path_locator,
            tp=mock_logger_provider,
            signal_provider=None,
        )

    def test_watermarked_delta_run_spans_provider_ops_and_watermark_save(
        self, spark, delta_provider, mock_logger_provider, monkeypatch
    ):
        from kindling.watermarking import (
            SimpleWatermarkEntityFinder,
            WatermarkAspect,
            WatermarkManager,
        )

        monkeypatch.setattr("kindling.watermarking.get_or_create_spark_session", lambda: spark)

        tp = RecordingTraceProvider()
        wrap_provider_ops(delta_provider, tp, provider_type="delta")

        entity_registry = DataEntityManager()
        _register_entities(entity_registry, "bronze.tracing_src", "silver.tracing_out")

        provider_registry = Mock()
        provider_registry.get_provider_for_entity.return_value = delta_provider

        watermark_manager = WatermarkManager(
            ep=delta_provider,
            wef=SimpleWatermarkEntityFinder(),
            lp=mock_logger_provider,
            signal_provider=None,
            provider_registry=provider_registry,
            tp=tp,
        )

        signal_provider = BlinkerSignalProvider()
        strategy = SimpleReadPersistStrategy(
            ep=delta_provider,
            der=entity_registry,
            tp=tp,
            lp=mock_logger_provider,
            provider_registry=provider_registry,
            signal_provider=signal_provider,
        )
        aspect = WatermarkAspect(
            wms=watermark_manager,
            lp=mock_logger_provider,
            signal_provider=signal_provider,
        )
        aspect.register()

        pipes_registry = DataPipesManager(mock_logger_provider)
        pipes_registry.register_pipe(
            "pipe.tracing",
            name="tracing",
            execute=lambda bronze_tracing_src: bronze_tracing_src,
            tags={},
            input_entity_ids=["bronze.tracing_src"],
            output_entity_id="silver.tracing_out",
            output_type="delta",
            use_watermark=True,
        )

        executer = DataPipesExecuter(
            lp=mock_logger_provider,
            dpe=entity_registry,
            dpr=pipes_registry,
            erps=strategy,
            tp=tp,
            signal_provider=signal_provider,
        )

        # Seed the source table with one commit.
        source_def = entity_registry.get_entity_definition("bronze.tracing_src")
        delta_provider.write_to_entity(
            spark.createDataFrame([(1, "A"), (2, "B")], SOURCE_SCHEMA), source_def
        )
        tp.spans.clear()  # only the pipe run's spans matter below

        with patch("kindling.simple_read_persist_strategy._is_local_execution", return_value=False):
            executer.run_datapipes(["pipe.tracing"])

        run_span = next(s for s in tp.tree() if s.operation == "run")
        pipe_span = run_span.children[0]
        assert pipe_span.operation == "pipe.run"

        read_span = next(c for c in pipe_span.children if c.operation == "read")
        assert read_span.details["resolved"] is True, "WatermarkAspect owned the read"
        read_ops = _child_ops(read_span)
        assert ("kindling.watermark", "read_changes") in read_ops

        read_changes_span = next(c for c in read_span.children if c.operation == "read_changes")
        nested_ops = _child_ops(read_changes_span)
        assert ("kindling.watermark", "get_cursor") in nested_ops

        persist_span = next(c for c in pipe_span.children if c.operation == "persist")
        persist_ops = _child_ops(persist_span)
        assert ("kindling.entity.delta", "write_to_entity") in persist_ops
        assert (
            "kindling.watermark",
            "save_cursor",
        ) in persist_ops, "Watermark save must appear under the persist span"

        save_span = next(c for c in persist_span.children if c.operation == "save_cursor")
        assert ("kindling.entity.delta", "merge_to_entity") in _child_ops(save_span)

        out_path = delta_provider.read_entity(
            entity_registry.get_entity_definition("silver.tracing_out")
        )
        assert out_path.count() == 2
        assert all(span.closed for span in tp.spans)
