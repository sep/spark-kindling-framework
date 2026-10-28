"""Provenance phase 2: ``input_versions`` on batch stamps.

A stamped output records the version of each input its pipe read: a Delta
input is read pinned at a looked-up version (so the stamp names exactly the
snapshot read, even when a commit lands before the write), and a
watermarked input records the cursor its read covers.
"""

import shutil
import tempfile
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, Mock

import pytest
from delta import configure_spark_with_delta_pip
from kindling.data_entities import EntityNameMapper, EntityPathLocator
from kindling.data_pipes import PipeMetadata
from kindling.entity_provider_delta import DeltaEntityProvider
from kindling.signaling import BlinkerSignalProvider
from kindling.simple_read_persist_strategy import SimpleReadPersistStrategy
from kindling.spark_config import ConfigService
from kindling.spark_log_provider import PythonLoggerProvider
from kindling.watermarking import ResolvedRead
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructField, StructType

from tests.spark_test_helper import (
    _teardown_non_delta_jvm,
    _worker_scoped_ivy_dir,
    set_spark_log_level,
)

SCHEMA = StructType(
    [
        StructField("customer_id", StringType(), False),
        StructField("status", StringType(), True),
    ]
)


@pytest.fixture(scope="module")
def spark():
    _teardown_non_delta_jvm()
    builder = (
        SparkSession.builder.appName("ProvenanceInputVersions")
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
    temp_path = tempfile.mkdtemp(prefix="kindling-input-versions-test-")
    yield Path(temp_path)
    shutil.rmtree(temp_path, ignore_errors=True)


def _delta_provider(spark, temp_dir, monkeypatch, access_mode="storage"):
    monkeypatch.setattr("kindling.entity_provider_delta.get_or_create_spark_session", lambda: spark)
    config = MagicMock(spec=ConfigService)
    config.get.side_effect = lambda key, default=None: (
        access_mode if key == "kindling.delta.access_mode" else default
    )
    name_mapper = MagicMock(spec=EntityNameMapper)
    name_mapper.get_table_name.side_effect = lambda entity: entity.entityid.replace(".", "_")
    path_locator = MagicMock(spec=EntityPathLocator)
    path_locator.get_table_path.side_effect = lambda entity: str(temp_dir / entity.entityid)
    lp = MagicMock(spec=PythonLoggerProvider)
    lp.get_logger.return_value = MagicMock()
    return DeltaEntityProvider(
        config=config,
        entity_name_mapper=name_mapper,
        path_locator=path_locator,
        tp=lp,
        signal_provider=None,
    )


@pytest.fixture
def provider(spark, temp_dir, monkeypatch):
    return _delta_provider(spark, temp_dir, monkeypatch)


@pytest.fixture
def signals():
    return BlinkerSignalProvider()


class _Registry:
    def __init__(self, entities):
        self._entities = {entity.entityid: entity for entity in entities}

    def get_entity_definition(self, entity_id):
        return self._entities.get(entity_id)


def _entity(entityid, tags=None):
    return SimpleNamespace(
        entityid=entityid,
        name=entityid.split(".")[-1],
        partition_columns=[],
        cluster_columns=None,
        merge_columns=["customer_id"],
        tags={"provider_type": "delta", **(tags or {})},
        schema=SCHEMA,
    )


STAMPED = {"provenance.enabled": "true"}


def _pipe(source, output):
    return PipeMetadata(
        pipeid=f"build_{output.entityid.replace('.', '_')}",
        name="Build output",
        execute=lambda df: df,
        tags={},
        input_entity_ids=[source.entityid],
        output_entity_id=output.entityid,
        output_type="delta",
    )


def _strategy(provider, signals, entities):
    lp = MagicMock(spec=PythonLoggerProvider)
    lp.get_logger.return_value = MagicMock()
    registry = Mock()
    registry.get_provider_for_entity.return_value = provider
    return SimpleReadPersistStrategy(
        ep=Mock(),
        der=_Registry(entities),
        tp=MagicMock(),
        lp=lp,
        provider_registry=registry,
        signal_provider=signals,
    )


def _write_versions(spark, provider, entity, *batches):
    for rows in batches:
        provider.merge_to_entity(spark.createDataFrame(rows, SCHEMA), entity)


def _stamps(provider, output):
    return {
        row["customer_id"]: row["__provenance"] for row in provider.read_entity(output).collect()
    }


def test_a_delta_input_records_the_version_read(spark, provider, signals):
    source, output = _entity("bronze.src_v"), _entity("silver.out_v", STAMPED)
    _write_versions(spark, provider, source, [("c1", "new")], [("c1", "open")])
    version = provider.get_entity_version(source)
    pipe = _pipe(source, output)
    strategy = _strategy(provider, signals, [source, output])

    df = strategy.create_pipe_entity_reader(pipe)(source, False)
    strategy.create_pipe_persist_activator(pipe)(df)

    assert _stamps(provider, output)["c1"]["input_versions"] == {source.entityid: str(version)}


def test_the_read_is_pinned_at_the_recorded_version(spark, provider, signals):
    source, output = _entity("bronze.src_pin"), _entity("silver.out_pin", STAMPED)
    _write_versions(spark, provider, source, [("c1", "new")])
    version = provider.get_entity_version(source)
    pipe = _pipe(source, output)
    strategy = _strategy(provider, signals, [source, output])

    df = strategy.create_pipe_entity_reader(pipe)(source, False)
    # A commit lands between the read and the write.
    _write_versions(spark, provider, source, [("c1", "closed"), ("c2", "new")])
    strategy.create_pipe_persist_activator(pipe)(df)

    stamps = _stamps(provider, output)
    assert set(stamps) == {"c1"}, "the write holds exactly the snapshot read"
    assert provider.read_entity(output).collect()[0]["status"] == "new"
    assert stamps["c1"]["input_versions"] == {source.entityid: str(version)}


def test_an_unstamped_output_reads_without_pinning(spark, provider, signals, monkeypatch):
    source, output = _entity("bronze.src_plain"), _entity("silver.out_plain")
    _write_versions(spark, provider, source, [("c1", "new")])
    pinned = MagicMock(side_effect=AssertionError("pinned read for an unstamped output"))
    monkeypatch.setattr(provider, "read_entity_versioned", pinned)
    strategy = _strategy(provider, signals, [source, output])

    df = strategy.create_pipe_entity_reader(_pipe(source, output))(source, False)

    assert df.count() == 1


def test_a_watermarked_input_records_its_cursor(spark, provider, signals):
    source, output = _entity("bronze.src_wm"), _entity("silver.out_wm", STAMPED)
    batch = spark.createDataFrame([("c1", "new")], SCHEMA)
    signals.create_signal("read.resolve_read").connect(
        lambda sender, **kwargs: ResolvedRead(df=batch, version="7"), weak=False
    )
    pipe = _pipe(source, output)
    strategy = _strategy(provider, signals, [source, output])

    df = strategy.create_pipe_entity_reader(pipe)(source, True)
    strategy.create_pipe_persist_activator(pipe)(df)

    assert _stamps(provider, output)["c1"]["input_versions"] == {source.entityid: "7"}


def test_versions_reset_with_each_pipe_execution(spark, provider, signals):
    source, output = _entity("bronze.src_reset"), _entity("silver.out_reset", STAMPED)
    _write_versions(spark, provider, source, [("c1", "new")])
    pipe = _pipe(source, output)
    strategy = _strategy(provider, signals, [source, output])
    strategy.create_pipe_entity_reader(pipe)(source, False)

    # The next execution reads nothing versioned (a resolved read without
    # a version), so the previous execution's version must not carry over.
    batch = spark.createDataFrame([("c1", "closed")], SCHEMA)
    signals.create_signal("read.resolve_read").connect(
        lambda sender, **kwargs: ResolvedRead(df=batch), weak=False
    )
    df = strategy.create_pipe_entity_reader(pipe)(source, True)
    strategy.create_pipe_persist_activator(pipe)(df)

    assert _stamps(provider, output)["c1"]["input_versions"] is None


def test_a_catalog_table_is_read_pinned(spark, temp_dir, monkeypatch):
    provider = _delta_provider(spark, temp_dir, monkeypatch, access_mode="catalog")
    source = _entity("bronze.src_catalog")
    table = source.entityid.replace(".", "_")
    spark.sql(f"DROP TABLE IF EXISTS {table}")
    try:
        _write_versions(spark, provider, source, [("c1", "new")])
        version = provider.get_entity_version(source)

        df, read_version = provider.read_entity_versioned(source)
        _write_versions(spark, provider, source, [("c2", "new")])

        assert read_version == str(version)
        assert [row["customer_id"] for row in df.collect()] == ["c1"]
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {table}")


def test_an_initial_watermarked_load_is_pinned_at_its_cursor(spark, provider, signals):
    """Through the real watermark resolution: WatermarkAspect -> Delta
    read_entity_changes(entity, None). A commit between the read and the
    write must not ride along with the version the cursor names."""
    from kindling.watermarking import WatermarkAspect, WatermarkService

    source, output = _entity("bronze.src_wm_init"), _entity("silver.out_wm_init", STAMPED)
    _write_versions(spark, provider, source, [("c1", "new")])
    version = provider.get_entity_version(source)
    wms = MagicMock(spec=WatermarkService)
    wms.read_changes.side_effect = lambda entity, pipe: provider.read_entity_changes(entity, None)
    lp = MagicMock(spec=PythonLoggerProvider)
    lp.get_logger.return_value = MagicMock()
    WatermarkAspect(wms=wms, lp=lp, signal_provider=signals).register()
    pipe = _pipe(source, output)
    strategy = _strategy(provider, signals, [source, output])

    df = strategy.create_pipe_entity_reader(pipe)(source, True)
    _write_versions(spark, provider, source, [("c2", "new")])
    strategy.create_pipe_persist_activator(pipe)(df.drop("SourceVersion", "SourceTimestamp"))

    stamps = _stamps(provider, output)
    assert set(stamps) == {"c1"}, "the initial load holds exactly the version its cursor names"
    assert stamps["c1"]["input_versions"] == {source.entityid: str(version)}
