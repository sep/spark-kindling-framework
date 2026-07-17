"""Round-trip integration tests for ParquetEntityProvider with real local Spark.

No cloud resources needed: create → write → append → read against a temp
directory, partitioned writes, and a streaming file-source → file-sink hop.
"""

import logging
from unittest.mock import MagicMock

import pytest

from kindling.data_entities import EntityMetadata
from kindling.entity_provider_parquet import ParquetEntityProvider


@pytest.fixture(scope="module")
def spark():
    try:
        from pyspark.sql import SparkSession
    except ImportError:
        pytest.skip("pyspark not available")

    session = (
        SparkSession.builder.appName("kindling-parquet-roundtrip")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )

    # get_or_create_spark_session() prefers __main__.spark (notebook
    # convention) — point it at this session so the provider uses it, and
    # clear it afterward so no stale/stopped session leaks to later tests.
    import __main__

    previous = getattr(__main__, "spark", None)
    __main__.spark = session

    yield session

    __main__.spark = previous
    session.stop()


@pytest.fixture
def provider():
    logger_provider = MagicMock()
    logger_provider.get_logger.return_value = logging.getLogger("parquet-test")
    return ParquetEntityProvider(logger_provider)


def _entity(path, partition_columns=None, schema=None, extra_tags=None):
    return EntityMetadata(
        entityid="test.exports",
        name="exports",
        partition_columns=partition_columns or [],
        merge_columns=[],
        tags={"provider_type": "parquet", "provider.path": str(path), **(extra_tags or {})},
        schema=schema,
    )


def test_write_append_read_roundtrip(spark, provider, tmp_path):
    path = tmp_path / "exports"
    entity = _entity(path)

    assert provider.check_entity_exists(entity) is False

    df = spark.createDataFrame([(1, "alpha"), (2, "bravo")], ["id", "name"])
    provider.write_to_entity(df, entity)

    assert provider.check_entity_exists(entity) is True
    assert provider.read_entity(entity).count() == 2

    provider.append_to_entity(spark.createDataFrame([(3, "charlie")], ["id", "name"]), entity)
    result = provider.read_entity(entity)
    assert result.count() == 3
    assert {row["name"] for row in result.collect()} == {"alpha", "bravo", "charlie"}


def test_overwrite_replaces_dataset(spark, provider, tmp_path):
    path = tmp_path / "exports"
    entity = _entity(path)

    provider.write_to_entity(spark.createDataFrame([(1, "old")], ["id", "name"]), entity)
    provider.write_to_entity(spark.createDataFrame([(2, "new")], ["id", "name"]), entity)

    rows = provider.read_entity(entity).collect()
    assert len(rows) == 1
    assert rows[0]["name"] == "new"


def test_partitioned_write_creates_partition_directories(spark, provider, tmp_path):
    path = tmp_path / "partitioned"
    entity = _entity(path, partition_columns=["region"])

    df = spark.createDataFrame([(1, "us"), (2, "eu"), (3, "us")], ["id", "region"])
    provider.write_to_entity(df, entity)

    partition_dirs = {p.name for p in path.iterdir() if p.is_dir()}
    assert {"region=us", "region=eu"} <= partition_dirs
    assert provider.read_entity(entity).count() == 3


def test_ensure_destination_creates_directory(spark, provider, tmp_path):
    path = tmp_path / "ensured"
    entity = _entity(path)

    provider.ensure_destination(entity)

    assert path.is_dir()


def test_streaming_file_source_to_file_sink(spark, provider, tmp_path):
    source_path = tmp_path / "stream-source"
    sink_path = tmp_path / "stream-sink"
    checkpoint = tmp_path / "checkpoint"

    source_entity = _entity(source_path)
    df = spark.createDataFrame([(1, "alpha"), (2, "bravo")], ["id", "name"])
    provider.write_to_entity(df, source_entity)

    # Stream read requires the entity schema (file-source rule).
    streaming_source = _entity(source_path, schema=df.schema)
    stream_df = provider.read_entity_as_stream(streaming_source)
    assert stream_df.isStreaming

    sink_entity = _entity(sink_path)
    query = provider.append_as_stream(stream_df, sink_entity, str(checkpoint))
    try:
        query.processAllAvailable()
    finally:
        query.stop()

    assert provider.read_entity(sink_entity).count() == 2
