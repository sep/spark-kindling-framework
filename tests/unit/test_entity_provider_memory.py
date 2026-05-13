"""
Unit tests for MemoryEntityProvider.

Tests in-memory operations, rate streams, and memory sinks.
"""

from unittest.mock import MagicMock, patch

import pytest

from kindling.data_entities import EntityMetadata
from kindling.entity_provider_memory import MemoryEntityProvider


class TestMemoryEntityProvider:
    """Tests for MemoryEntityProvider"""

    @pytest.fixture
    def provider(self):
        """Create Memory provider with mocked dependencies"""
        logger_provider = MagicMock()
        logger_provider.get_logger.return_value = MagicMock()
        provider = MemoryEntityProvider(logger_provider)
        provider.spark = MagicMock()
        return provider

    @pytest.fixture
    def entity_metadata(self):
        """Create sample entity metadata for memory"""
        return EntityMetadata(
            entityid="temp.results",
            name="results",
            partition_columns=[],
            merge_columns=["id"],
            tags={
                "provider_type": "memory",
                "provider.table_name": "test_table",
            },
            schema=None,
        )

    def test_read_entity_from_memory_table(self, provider, entity_metadata):
        """Test reading entity from memory table"""
        mock_df = MagicMock()
        mock_df.count.return_value = 50
        provider.spark.table.return_value = mock_df

        result = provider.read_entity(entity_metadata)

        assert result == mock_df
        provider.spark.table.assert_called_with("test_table")

    def test_read_entity_from_memory_store(self, provider, entity_metadata):
        """Test reading entity from in-memory store when table doesn't exist"""
        mock_df = MagicMock()
        provider._memory_store[entity_metadata.entityid] = mock_df
        provider.spark.table.side_effect = Exception("Table not found")

        result = provider.read_entity(entity_metadata)

        assert result == mock_df

    def test_read_entity_not_found_raises(self, provider, entity_metadata):
        """Test that reading non-existent entity raises ValueError when schema is None"""
        provider.spark.table.side_effect = Exception("Table not found")
        # entity_metadata.schema is None by default in the fixture

        with pytest.raises(ValueError, match="Memory entity.*not found"):
            provider.read_entity(entity_metadata)

    def test_read_entity_not_found_returns_empty_df_when_schema_known(
        self, provider, entity_metadata
    ):
        """Empty DataFrame returned (not raised) when entity absent but schema is available."""
        from pyspark.sql.types import IntegerType, StringType, StructField, StructType

        schema = StructType([StructField("id", IntegerType()), StructField("name", StringType())])
        entity_metadata.schema = schema
        provider.spark.table.side_effect = Exception("Table not found")
        mock_empty_df = MagicMock()
        provider.spark.createDataFrame.return_value = mock_empty_df

        result = provider.read_entity(entity_metadata)

        provider.spark.createDataFrame.assert_called_once_with([], schema)
        assert result is mock_empty_df

    def test_read_entity_as_stream_rate_source(self, provider, entity_metadata):
        """Test reading entity as rate stream"""
        entity_metadata.tags = {
            "provider_type": "memory",
            "provider.stream_type": "rate",
            "provider.rowsPerSecond": "20",
            "provider.numPartitions": "2",
        }

        mock_stream = MagicMock()
        mock_reader = MagicMock()
        mock_reader.load.return_value = mock_stream
        mock_reader.option.return_value = mock_reader

        provider.spark.readStream.format.return_value = mock_reader

        result = provider.read_entity_as_stream(entity_metadata)

        assert result == mock_stream
        provider.spark.readStream.format.assert_called_with("rate")
        mock_reader.option.assert_any_call("rowsPerSecond", 20)
        mock_reader.option.assert_any_call("numPartitions", 2)

    def test_read_entity_as_stream_rate_source_defaults(self, provider, entity_metadata):
        """Test rate stream with default options"""
        entity_metadata.tags = {
            "provider_type": "memory",
            "provider.stream_type": "rate",
        }

        mock_reader = MagicMock()
        mock_reader.load.return_value = MagicMock()
        mock_reader.option.return_value = mock_reader

        provider.spark.readStream.format.return_value = mock_reader

        provider.read_entity_as_stream(entity_metadata)

        # Should use default rowsPerSecond=10, numPartitions=1
        mock_reader.option.assert_any_call("rowsPerSecond", 10)
        mock_reader.option.assert_any_call("numPartitions", 1)

    def test_read_entity_as_stream_memory_source(self, provider, entity_metadata):
        """Test reading entity as memory stream"""
        entity_metadata.tags = {
            "provider_type": "memory",
            "provider.stream_type": "memory",
            "provider.table_name": "stream_table",
        }

        mock_stream = MagicMock()
        provider.spark.readStream.table.return_value = mock_stream

        result = provider.read_entity_as_stream(entity_metadata)

        assert result == mock_stream
        provider.spark.readStream.table.assert_called_with("stream_table")

    def test_read_entity_as_stream_unknown_type_raises(self, provider, entity_metadata):
        """Test that unknown stream type raises ValueError"""
        entity_metadata.tags = {
            "provider_type": "memory",
            "provider.stream_type": "unknown",
        }

        with pytest.raises(ValueError, match="Unknown stream_type 'unknown'"):
            provider.read_entity_as_stream(entity_metadata)

    def test_write_to_entity(self, provider, entity_metadata):
        """write_to_entity registers a temp view and mirrors into _memory_store"""
        mock_df = MagicMock()
        mock_df.count.return_value = 100

        provider.write_to_entity(mock_df, entity_metadata)

        mock_df.createOrReplaceTempView.assert_called_once_with("test_table")
        assert provider._memory_store[entity_metadata.entityid] is mock_df

    def test_append_to_entity_new_entity(self, provider, entity_metadata):
        """append_to_entity on a fresh entity registers the view directly"""
        mock_df = MagicMock()
        mock_df.count.return_value = 50

        provider.append_to_entity(mock_df, entity_metadata)

        mock_df.createOrReplaceTempView.assert_called_once_with("test_table")
        assert provider._memory_store[entity_metadata.entityid] is mock_df

    def test_append_to_entity_existing_entity(self, provider, entity_metadata):
        """append_to_entity on an existing entity unions then re-registers the view"""
        combined_df = MagicMock()
        existing_df = MagicMock()
        existing_df.union.return_value = combined_df
        provider._memory_store[entity_metadata.entityid] = existing_df

        mock_df = MagicMock()
        mock_df.count.return_value = 10

        provider.append_to_entity(mock_df, entity_metadata)

        existing_df.union.assert_called_once_with(mock_df)
        combined_df.createOrReplaceTempView.assert_called_once_with("test_table")
        assert provider._memory_store[entity_metadata.entityid] is combined_df

    def test_append_as_stream(self, provider, entity_metadata):
        """Test streaming write to memory sink"""
        entity_metadata.tags = {
            "provider_type": "memory",
            "provider.table_name": "test_table",
            "provider.output_mode": "append",
            "provider.query_name": "test_query",
        }

        mock_df = MagicMock()
        mock_query = MagicMock()
        mock_writer = MagicMock()
        mock_writer.outputMode.return_value = mock_writer
        mock_writer.queryName.return_value = mock_writer
        mock_writer.option.return_value = mock_writer
        mock_writer.start.return_value = mock_query

        mock_df.writeStream.format.return_value = mock_writer

        result = provider.append_as_stream(
            mock_df, entity_metadata, checkpoint_location="/tmp/checkpoint"
        )

        assert result == mock_query
        mock_df.writeStream.format.assert_called_with("memory")
        mock_writer.outputMode.assert_called_with("append")
        mock_writer.queryName.assert_called_with("test_query")
        mock_writer.option.assert_called_with("checkpointLocation", "/tmp/checkpoint")
        mock_writer.start.assert_called_with("test_table")

    def test_append_as_stream_with_defaults(self, provider, entity_metadata):
        """Test streaming write uses defaults when config is minimal"""
        entity_metadata.tags = {"provider_type": "memory"}

        mock_df = MagicMock()
        mock_writer = MagicMock()
        mock_writer.outputMode.return_value = mock_writer
        mock_writer.queryName.return_value = mock_writer
        mock_writer.option.return_value = mock_writer
        mock_writer.start.return_value = MagicMock()

        mock_df.writeStream.format.return_value = mock_writer

        provider.append_as_stream(mock_df, entity_metadata, checkpoint_location="/tmp/cp")

        # Should use default output_mode="append" and query_name=entity.name
        mock_writer.outputMode.assert_called_with("append")
        mock_writer.queryName.assert_called_with("results")

    def test_check_entity_exists_in_table(self, provider, entity_metadata):
        """Test check_entity_exists returns True when table exists"""
        provider.spark.table.return_value = MagicMock()

        result = provider.check_entity_exists(entity_metadata)

        assert result is True

    def test_check_entity_exists_in_memory_store(self, provider, entity_metadata):
        """Test check_entity_exists returns True when in memory store"""
        provider.spark.table.side_effect = Exception("Table not found")
        provider._memory_store[entity_metadata.entityid] = MagicMock()

        result = provider.check_entity_exists(entity_metadata)

        assert result is True

    def test_check_entity_exists_not_found(self, provider, entity_metadata):
        """Test check_entity_exists returns False when entity doesn't exist"""
        provider.spark.table.side_effect = Exception("Table not found")

        result = provider.check_entity_exists(entity_metadata)

        assert result is False

    def test_get_table_name_from_config(self, provider, entity_metadata):
        """Test table name resolution from config"""
        table_name = provider._get_table_name(entity_metadata)
        assert table_name == "test_table"

    def test_get_table_name_defaults_to_entity_name(self, provider, entity_metadata):
        """Test table name defaults to entity name when not in config"""
        entity_metadata.tags = {"provider_type": "memory"}
        table_name = provider._get_table_name(entity_metadata)
        assert table_name == "results"


class TestMemoryProviderSeedRows:
    """Tests for provider.seed.rows inline seeding."""

    @pytest.fixture
    def provider(self):
        logger_provider = MagicMock()
        logger_provider.get_logger.return_value = MagicMock()
        provider = MemoryEntityProvider(logger_provider)
        provider.spark = MagicMock()
        return provider

    @pytest.fixture
    def seeded_entity(self):
        from pyspark.sql.types import StringType, StructField, StructType

        schema = StructType([StructField("id", StringType()), StructField("value", StringType())])
        return EntityMetadata(
            entityid="temp.seed_test",
            name="seed_test",
            partition_columns=[],
            merge_columns=["id"],
            tags={
                "provider_type": "memory",
                "provider.seed.rows": [{"id": "1", "value": "hello"}],
            },
            schema=schema,
        )

    def test_seed_rows_creates_dataframe_when_entity_absent(self, provider, seeded_entity):
        """Seed rows are materialized when no table/store data exists."""
        provider.spark.table.side_effect = Exception("Table not found")
        mock_seed_df = MagicMock()
        provider.spark.createDataFrame.return_value = mock_seed_df
        mock_seed_df.count.return_value = 1
        mock_seed_df.createOrReplaceTempView = MagicMock()

        result = provider.read_entity(seeded_entity)

        assert result is mock_seed_df
        provider.spark.createDataFrame.assert_called_once_with(
            [{"id": "1", "value": "hello"}], seeded_entity.schema
        )

    def test_existing_store_data_wins_over_seed_rows(self, provider, seeded_entity):
        """Imperative writes to _memory_store take priority over seed rows."""
        provider.spark.table.side_effect = Exception("Table not found")
        existing_df = MagicMock()
        provider._memory_store[seeded_entity.entityid] = existing_df

        result = provider.read_entity(seeded_entity)

        assert result is existing_df
        provider.spark.createDataFrame.assert_not_called()

    def test_existing_table_wins_over_seed_rows(self, provider, seeded_entity):
        """A registered temp view takes priority over seed rows."""
        table_df = MagicMock()
        table_df.count.return_value = 5
        provider.spark.table.return_value = table_df

        result = provider.read_entity(seeded_entity)

        assert result is table_df
        provider.spark.createDataFrame.assert_not_called()

    def test_seed_rows_without_schema_raises(self, provider):
        """Seed rows on a schema-less entity raise ValueError."""
        entity = EntityMetadata(
            entityid="temp.no_schema",
            name="no_schema",
            partition_columns=[],
            merge_columns=[],
            tags={"provider_type": "memory", "provider.seed.rows": [{"id": "1"}]},
            schema=None,
        )
        provider.spark.table.side_effect = Exception("not found")

        with pytest.raises(ValueError, match="no schema defined"):
            provider.read_entity(entity)

    def test_seed_rows_non_list_raises(self, provider, seeded_entity):
        """Non-list seed.rows raises ValueError."""
        seeded_entity.tags["provider.seed.rows"] = "not-a-list"
        provider.spark.table.side_effect = Exception("not found")

        with pytest.raises(ValueError, match="must be a list"):
            provider.read_entity(seeded_entity)

    def test_seed_rows_non_dict_row_raises(self, provider, seeded_entity):
        """Non-mapping row in seed.rows raises ValueError with row index."""
        seeded_entity.tags["provider.seed.rows"] = ["not-a-dict"]
        provider.spark.table.side_effect = Exception("not found")

        with pytest.raises(ValueError, match=r"seed\.rows\[0\].*mapping"):
            provider.read_entity(seeded_entity)

    def test_seed_rows_unknown_field_raises(self, provider, seeded_entity):
        """Row with a field not in the schema raises ValueError naming the bad field."""
        seeded_entity.tags["provider.seed.rows"] = [{"id": "1", "value": "x", "extra": "bad"}]
        provider.spark.table.side_effect = Exception("not found")

        with pytest.raises(ValueError, match="unknown.*field.*extra"):
            provider.read_entity(seeded_entity)

    def test_seed_rows_spark_error_wrapped(self, provider, seeded_entity):
        """Spark createDataFrame errors are wrapped with entity context."""
        provider.spark.table.side_effect = Exception("not found")
        provider.spark.createDataFrame.side_effect = Exception("type mismatch")

        with pytest.raises(ValueError, match="could not be materialized"):
            provider.read_entity(seeded_entity)

    def test_no_seed_rows_config_keeps_empty_df_behavior(self, provider):
        """Entities without seed config still return empty DataFrame when schema is known."""
        from pyspark.sql.types import StringType, StructField, StructType

        schema = StructType([StructField("id", StringType())])
        entity = EntityMetadata(
            entityid="temp.plain",
            name="plain",
            partition_columns=[],
            merge_columns=[],
            tags={"provider_type": "memory"},
            schema=schema,
        )
        provider.spark.table.side_effect = Exception("not found")
        mock_empty = MagicMock()
        provider.spark.createDataFrame.return_value = mock_empty

        result = provider.read_entity(entity)

        assert result is mock_empty
        provider.spark.createDataFrame.assert_called_once_with([], schema)


class TestMemoryProviderErrorHandling:
    """Tests for error handling in MemoryEntityProvider"""

    @pytest.fixture
    def provider(self):
        """Create Memory provider"""
        logger_provider = MagicMock()
        logger_provider.get_logger.return_value = MagicMock()
        provider = MemoryEntityProvider(logger_provider)
        provider.spark = MagicMock()
        return provider

    def test_write_failure_logs_error(self, provider):
        """Test that write failures are logged"""
        entity = EntityMetadata(
            entityid="temp.test",
            name="test",
            partition_columns=[],
            merge_columns=[],
            tags={"provider_type": "memory"},
            schema=None,
        )

        mock_df = MagicMock()
        mock_df.count.return_value = 0
        mock_df.createOrReplaceTempView.side_effect = Exception("Write error")

        with pytest.raises(Exception, match="Write error"):
            provider.write_to_entity(mock_df, entity)

        provider.logger.error.assert_called_once()
        call_kwargs = provider.logger.error.call_args[1]
        assert call_kwargs.get("include_traceback") is True

    def test_stream_write_failure_logs_error(self, provider):
        """Test that streaming write failures are logged"""
        entity = EntityMetadata(
            entityid="temp.test",
            name="test",
            partition_columns=[],
            merge_columns=[],
            tags={"provider_type": "memory"},
            schema=None,
        )

        mock_df = MagicMock()
        mock_df.writeStream.format.side_effect = Exception("Stream write error")

        with pytest.raises(Exception, match="Stream write error"):
            provider.append_as_stream(mock_df, entity, checkpoint_location="/tmp/cp")

        provider.logger.error.assert_called_once()
