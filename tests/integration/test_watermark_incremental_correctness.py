"""Integration tests exposing incremental-read correctness bugs in the
watermark path, using real Delta tables with Change Data Feed.

Bug 1 — skipped intermediate versions (watermarking.py):
    ``read_current_entity_changes`` passes the table's *latest* version to
    ``read_entity_since_version`` instead of ``watermark + 1``. CDF's
    ``startingVersion`` is inclusive, so when the source advanced more than
    one commit since the watermark, every commit except the last is silently
    never read.

Bug 2 — watermark over-advance (simple_read_persist_strategy.py):
    ``persist_lambda`` re-fetches ``get_entity_version`` at persist time
    rather than using the version that was actually read. A commit landing
    on the source between read and persist gets its version recorded as
    processed without its data ever being read — the next incremental read
    reports "no new data" and the commit is lost.

Both tests drive the real DeltaEntityProvider against local Delta tables so
the change-feed semantics are Delta's own, not a mock's.
"""

import shutil
import tempfile
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, Mock

import pytest
from delta import configure_spark_with_delta_pip
from kindling.data_entities import (
    DataEntityManager,
    EntityNameMapper,
    EntityPathLocator,
)
from kindling.entity_provider_delta import DeltaEntityProvider
from kindling.signaling import BlinkerSignalProvider
from kindling.simple_read_persist_strategy import SimpleReadPersistStrategy
from kindling.spark_config import ConfigService
from kindling.spark_log_provider import PythonLoggerProvider
from kindling.watermarking import (
    SimpleWatermarkEntityFinder,
    WatermarkAspect,
    WatermarkManager,
)
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    IntegerType,
    StringType,
    StructField,
    StructType,
)


def _teardown_existing_spark_jvm():
    """Stop any session created by earlier test modules and shut down the
    py4j gateway so a NEW JVM launches with the Delta jars on its classpath.

    Spark's JVM is a process-wide singleton: ``spark.jars.packages`` is only
    honored at gateway launch, so a plain session created by another module
    would leave this module without the Delta data source no matter what
    configs we pass to ``getOrCreate``.
    """
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
def spark():
    _teardown_existing_spark_jvm()
    builder = (
        SparkSession.builder.appName("WatermarkIncrementalCorrectness")
        .master("local[2]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        # Every table created in these tests gets CDF from version 0.
        .config("spark.databricks.delta.properties.defaults.enableChangeDataFeed", "true")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
    )
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    yield spark
    spark.stop()


@pytest.fixture
def temp_dir():
    temp_path = tempfile.mkdtemp(prefix="kindling-wm-test-")
    yield Path(temp_path)
    shutil.rmtree(temp_path, ignore_errors=True)


@pytest.fixture
def mock_logger_provider():
    provider = MagicMock(spec=PythonLoggerProvider)
    provider.get_logger.return_value = MagicMock()
    return provider


@pytest.fixture
def delta_provider(spark, temp_dir, mock_logger_provider, monkeypatch):
    """Real DeltaEntityProvider in storage mode, rooted at a temp dir."""
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

    return DeltaEntityProvider(
        config=config,
        entity_name_mapper=name_mapper,
        path_locator=path_locator,
        tp=mock_logger_provider,
        signal_provider=None,
    )


@pytest.fixture
def watermark_manager(spark, delta_provider, mock_logger_provider, monkeypatch):
    monkeypatch.setattr("kindling.watermarking.get_or_create_spark_session", lambda: spark)
    return WatermarkManager(
        ep=delta_provider,
        wef=SimpleWatermarkEntityFinder(),
        lp=mock_logger_provider,
        signal_provider=None,
    )


SOURCE_SCHEMA = StructType(
    [
        StructField("id", IntegerType(), False),
        StructField("label", StringType(), True),
    ]
)


def _make_source_entity(entityid="bronze.readings"):
    return SimpleNamespace(
        entityid=entityid,
        name=entityid.split(".")[-1],
        partition_columns=[],
        merge_columns=["id"],
        tags={"provider_type": "delta"},
        schema=SOURCE_SCHEMA,
        cluster_columns=None,
    )


def _append_commit(spark, delta_provider, entity, rows):
    """One append = exactly one Delta commit."""
    df = spark.createDataFrame(rows, SOURCE_SCHEMA)
    if delta_provider.check_entity_exists(entity):
        delta_provider.append_to_entity(df, entity)
    else:
        delta_provider.write_to_entity(df, entity)


class TestSkippedIntermediateVersions:
    """Bug 1: read_current_entity_changes must return changes from EVERY
    commit after the watermark, not only the latest one."""

    def test_read_changes_includes_all_commits_since_watermark(
        self, spark, delta_provider, watermark_manager, monkeypatch
    ):
        entity = _make_source_entity("bronze.readings_skip_test")

        # Three separate writes with distinct keys so latest-change-per-key
        # dedup cannot mask a skipped commit. Capture the actual version
        # after each write (a single provider write can produce more than
        # one Delta commit).
        _append_commit(spark, delta_provider, entity, [(1, "A")])
        version_after_a = delta_provider.get_entity_version(entity)
        _append_commit(spark, delta_provider, entity, [(2, "B")])
        version_after_b = delta_provider.get_entity_version(entity)
        _append_commit(spark, delta_provider, entity, [(3, "C")])
        version_after_c = delta_provider.get_entity_version(entity)

        assert version_after_a < version_after_b < version_after_c

        # Watermark says: processed through the version that wrote row A.
        monkeypatch.setattr(
            watermark_manager,
            "get_watermark",
            lambda source_entity_id, reader_id: version_after_a,
        )

        pipe = SimpleNamespace(pipeid="pipe.skip_test", name="skip_test")
        result = watermark_manager.read_current_entity_changes(entity, pipe)

        assert result is not None, "There ARE unprocessed changes (versions 1 and 2)"
        labels = {row["label"] for row in result.collect()}

        # Versions 1 (B) and 2 (C) are both after the watermark; both must
        # be returned. The bug reads CDF from startingVersion=<latest>,
        # returning only C and silently dropping B.
        assert labels == {"B", "C"}, (
            f"Expected changes from every commit after the watermark "
            f"(B from v1, C from v2), got {labels} — intermediate commits "
            f"were silently skipped"
        )


class TestWatermarkOverAdvance:
    """Bug 2: the watermark saved after persist must reflect the version
    that was READ, not the source version at persist time."""

    def _make_strategy(
        self, watermark_manager, delta_provider, entity_registry, mock_logger_provider
    ):
        """Wire the strategy and the WatermarkAspect exactly as production
        does: a shared signal provider, aspect registered on it."""
        trace_provider = Mock()
        trace_provider.span.return_value.__enter__ = Mock()
        trace_provider.span.return_value.__exit__ = Mock(return_value=False)

        provider_registry = MagicMock()
        provider_registry.get_provider_for_entity.return_value = delta_provider

        signal_provider = BlinkerSignalProvider()
        strategy = SimpleReadPersistStrategy(
            ep=delta_provider,
            der=entity_registry,
            tp=trace_provider,
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
        return strategy

    def test_commit_between_read_and_persist_is_not_marked_processed(
        self, spark, delta_provider, watermark_manager, mock_logger_provider
    ):
        source_entity = _make_source_entity("bronze.readings_race_test")
        output_entity = _make_source_entity("silver.readings_race_test")

        entity_registry = DataEntityManager()
        for e in (source_entity, output_entity):
            entity_registry.register_entity(
                e.entityid,
                name=e.name,
                partition_columns=[],
                merge_columns=["id"],
                tags={"provider_type": "delta"},
                schema=SOURCE_SCHEMA,
            )

        strategy = self._make_strategy(
            watermark_manager, delta_provider, entity_registry, mock_logger_provider
        )

        pipe = SimpleNamespace(
            pipeid="pipe.race_test",
            name="race_test",
            input_entity_ids=[source_entity.entityid],
            output_entity_id=output_entity.entityid,
            use_watermark=True,
        )

        # Write row A; this state (version_at_read) is what the pipe reads,
        # through the strategy's real read path so the aspect captures the
        # version at read time.
        _append_commit(spark, delta_provider, source_entity, [(1, "A")])
        source_def = entity_registry.get_entity_definition(source_entity.entityid)
        version_at_read = delta_provider.get_entity_version(source_def)
        reader = strategy.create_pipe_entity_reader(pipe)
        df_read = reader(source_def, True)
        assert df_read is not None

        # A concurrent commit lands AFTER the read, BEFORE the persist.
        _append_commit(spark, delta_provider, source_entity, [(2, "B")])
        assert delta_provider.get_entity_version(source_def) > version_at_read

        # Persist the (version-0) read result.
        persist = strategy.create_pipe_persist_activator(pipe)
        persist(df_read)

        # The watermark must record the version that was read. Recording the
        # persist-time version marks row B processed though it was never read.
        saved = watermark_manager.get_watermark(source_entity.entityid, pipe.pipeid)
        assert saved == version_at_read, (
            f"Watermark advanced to {saved}, but only version "
            f"{version_at_read} was read — row B's commit is now marked "
            f"processed without ever being processed"
        )

        # The observable consequence: the next incremental read must surface
        # row B. With the over-advanced watermark it returns None ("no new
        # data") and row B is lost forever.
        next_read = watermark_manager.read_current_entity_changes(
            entity_registry.get_entity_definition(source_entity.entityid), pipe
        )
        assert next_read is not None, (
            "Next incremental read reported 'no new data' — the commit that "
            "landed between read and persist was silently lost"
        )
        labels = {row["label"] for row in next_read.collect()}
        assert "B" in labels
