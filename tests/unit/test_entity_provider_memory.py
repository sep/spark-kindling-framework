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
        """Test that reading non-existent entity raises ValueError"""
        provider.spark.table.side_effect = Exception("Table not found")

        with pytest.raises(ValueError, match="Memory entity.*not found"):
            provider.read_entity(entity_metadata)

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
        """Test writing DataFrame to memory table"""
        mock_df = MagicMock()
        mock_df.count.return_value = 100
        mock_writer = MagicMock()
        mock_writer.mode.return_value = mock_writer
        mock_writer.saveAsTable.return_value = None

        mock_df.write.format.return_value = mock_writer

        provider.write_to_entity(mock_df, entity_metadata)

        mock_df.write.format.assert_called_with("memory")
        mock_writer.mode.assert_called_with("overwrite")
        mock_writer.saveAsTable.assert_called_with("test_table")

        # Should also store in memory dict
        assert entity_metadata.entityid in provider._memory_store

    def test_append_to_entity_new_entity(self, provider, entity_metadata):
        """Test appending to new entity"""
        mock_df = MagicMock()
        mock_df.count.return_value = 50
        mock_writer = MagicMock()
        mock_writer.mode.return_value = mock_writer
        mock_writer.saveAsTable.return_value = None

        mock_df.write.format.return_value = mock_writer

        provider.append_to_entity(mock_df, entity_metadata)

        mock_writer.mode.assert_called_with("append")
        assert entity_metadata.entityid in provider._memory_store

    def test_append_to_entity_existing_entity(self, provider, entity_metadata):
        """Test appending to existing entity unions DataFrames"""
        existing_df = MagicMock()
        existing_df.union.return_value = MagicMock()
        provider._memory_store[entity_metadata.entityid] = existing_df

        mock_df = MagicMock()
        mock_writer = MagicMock()
        mock_writer.mode.return_value = mock_writer
        mock_writer.saveAsTable.return_value = None

        mock_df.write.format.return_value = mock_writer

        provider.append_to_entity(mock_df, entity_metadata)

        # Should union with existing DataFrame
        existing_df.union.assert_called_with(mock_df)

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
        mock_df.write.format.side_effect = Exception("Write error")

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
