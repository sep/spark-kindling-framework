from unittest.mock import MagicMock, patch

import pytest

from kindling.data_entities import EntityMetadata
from kindling.entity_provider import (
    can_ensure_destination,
    is_stream_writable,
    is_streamable,
    is_writable,
)
from kindling.entity_provider_parquet import ParquetEntityProvider


def _entity(tags, partition_columns=None, schema=None):
    return EntityMetadata(
        entityid="boundary.exports",
        name="exports",
        partition_columns=partition_columns or [],
        merge_columns=[],
        tags={"provider_type": "parquet", **tags},
        schema=schema,
    )


BASE_TAGS = {"provider.path": "/data/exports"}


def _provider():
    logger_provider = MagicMock()
    logger_provider.get_logger.return_value = MagicMock()
    return ParquetEntityProvider(logger_provider)


def _patched_spark():
    spark = MagicMock()
    return (
        patch(
            "kindling.entity_provider_parquet.get_or_create_spark_session",
            return_value=spark,
        ),
        spark,
    )


def test_implements_all_capability_interfaces():
    provider = _provider()
    assert is_writable(provider)
    assert is_streamable(provider)
    assert is_stream_writable(provider)
    assert can_ensure_destination(provider)


def test_read_entity_loads_path():
    provider = _provider()
    patcher, spark = _patched_spark()

    with patcher:
        df = provider.read_entity(_entity(BASE_TAGS))

    spark.read.format.assert_called_once_with("parquet")
    reader = spark.read.format.return_value
    reader.load.assert_called_once_with("/data/exports")
    assert df is reader.load.return_value


def test_read_entity_requires_path():
    provider = _provider()

    with pytest.raises(ValueError, match="provider.path"):
        provider.read_entity(_entity({}))


def test_read_merge_schema_option():
    provider = _provider()
    patcher, spark = _patched_spark()
    reader = spark.read.format.return_value
    reader.option.return_value = reader

    with patcher:
        provider.read_entity(_entity({**BASE_TAGS, "provider.merge_schema": "true"}))

    reader.option.assert_any_call("mergeSchema", "true")


def test_write_defaults_to_overwrite_with_partitioning():
    provider = _provider()
    df = MagicMock()
    writer = df.write.format.return_value
    writer.mode.return_value = writer
    writer.partitionBy.return_value = writer
    writer.option.return_value = writer

    provider.write_to_entity(df, _entity(BASE_TAGS, partition_columns=["region", "date"]))

    df.write.format.assert_called_once_with("parquet")
    writer.mode.assert_called_once_with("overwrite")
    writer.partitionBy.assert_called_once_with("region", "date")
    writer.save.assert_called_once_with("/data/exports")


def test_append_uses_append_mode():
    provider = _provider()
    df = MagicMock()
    writer = df.write.format.return_value
    writer.mode.return_value = writer
    writer.option.return_value = writer

    provider.append_to_entity(df, _entity(BASE_TAGS))

    writer.mode.assert_called_once_with("append")
    writer.save.assert_called_once_with("/data/exports")


def test_extra_options_pass_through():
    provider = _provider()
    df = MagicMock()
    writer = df.write.format.return_value
    writer.mode.return_value = writer
    writer.option.return_value = writer

    provider.append_to_entity(df, _entity({**BASE_TAGS, "provider.option.compression": "zstd"}))

    writer.option.assert_any_call("compression", "zstd")


def test_stream_read_requires_schema():
    provider = _provider()

    with pytest.raises(ValueError, match="requires an explicit schema"):
        provider.read_entity_as_stream(_entity(BASE_TAGS, schema=None))


def test_stream_read_applies_schema_and_path():
    provider = _provider()
    patcher, spark = _patched_spark()
    schema = MagicMock(name="schema")
    reader = spark.readStream.format.return_value
    reader.schema.return_value = reader
    reader.option.return_value = reader

    with patcher:
        provider.read_entity_as_stream(_entity(BASE_TAGS, schema=schema))

    spark.readStream.format.assert_called_once_with("parquet")
    reader.schema.assert_called_once_with(schema)
    reader.load.assert_called_once_with("/data/exports")


def test_stream_append_configures_file_sink():
    provider = _provider()
    df = MagicMock()
    writer = df.writeStream.format.return_value
    writer.outputMode.return_value = writer
    writer.option.return_value = writer
    writer.partitionBy.return_value = writer

    query = provider.append_as_stream(
        df, _entity(BASE_TAGS, partition_columns=["region"]), "/chk/exports"
    )

    df.writeStream.format.assert_called_once_with("parquet")
    writer.outputMode.assert_called_once_with("append")
    writer.option.assert_any_call("checkpointLocation", "/chk/exports")
    writer.option.assert_any_call("path", "/data/exports")
    writer.partitionBy.assert_called_once_with("region")
    assert query is writer.start.return_value


def test_check_entity_exists_true_when_readable():
    provider = _provider()
    patcher, spark = _patched_spark()

    with patcher:
        assert provider.check_entity_exists(_entity(BASE_TAGS)) is True


def test_check_entity_exists_false_on_error():
    provider = _provider()
    patcher, spark = _patched_spark()
    spark.read.format.return_value.load.side_effect = Exception("no such path")

    with patcher:
        assert provider.check_entity_exists(_entity(BASE_TAGS)) is False


def test_check_entity_exists_false_without_path():
    assert _provider().check_entity_exists(_entity({})) is False


def test_ensure_destination_creates_missing_directory():
    provider = _provider()
    patcher, spark = _patched_spark()
    fs = MagicMock()
    hadoop_path = spark._jvm.org.apache.hadoop.fs.Path.return_value
    hadoop_path.getFileSystem.return_value = fs
    fs.exists.return_value = False

    with patcher:
        provider.ensure_destination(_entity(BASE_TAGS))

    fs.mkdirs.assert_called_once_with(hadoop_path)


def test_ensure_destination_noop_when_exists():
    provider = _provider()
    patcher, spark = _patched_spark()
    fs = MagicMock()
    hadoop_path = spark._jvm.org.apache.hadoop.fs.Path.return_value
    hadoop_path.getFileSystem.return_value = fs
    fs.exists.return_value = True

    with patcher:
        provider.ensure_destination(_entity(BASE_TAGS))

    fs.mkdirs.assert_not_called()


def test_registry_registers_parquet_provider():
    from kindling.entity_provider_registry import EntityProviderRegistry

    with patch("kindling.entity_provider_registry.GlobalInjector"):
        logger_provider = MagicMock()
        logger_provider.get_logger.return_value = MagicMock()
        registry = EntityProviderRegistry(logger_provider)

    assert "parquet" in registry.list_registered_providers()


def test_stream_read_applies_direct_options():
    provider = _provider()
    patcher, spark = _patched_spark()
    reader = spark.readStream.format.return_value
    reader.schema.return_value = reader
    reader.option.return_value = reader

    with patcher:
        provider.read_entity_as_stream(
            _entity(BASE_TAGS, schema=MagicMock()), options={"maxFilesPerTrigger": "10"}
        )

    reader.option.assert_any_call("maxFilesPerTrigger", "10")


def test_stream_append_applies_direct_options():
    provider = _provider()
    df = MagicMock()
    writer = df.writeStream.format.return_value
    writer.outputMode.return_value = writer
    writer.option.return_value = writer

    provider.append_as_stream(
        df, _entity(BASE_TAGS), "/chk/exports", options={"maxRecordsPerFile": "1000"}
    )

    writer.option.assert_any_call("maxRecordsPerFile", "1000")
