"""
Unit tests for CSVEntityProvider.

Tests CSV reading, configuration handling, and error cases.
"""

from unittest.mock import MagicMock, patch

import pytest

from kindling.data_entities import EntityMetadata
from kindling.entity_provider_csv import CSVEntityProvider


class TestCSVEntityProvider:
    """Tests for CSVEntityProvider"""

    @pytest.fixture
    def provider(self):
        """Create CSV provider with mocked dependencies"""
        logger_provider = MagicMock()
        logger_provider.get_logger.return_value = MagicMock()
        provider = CSVEntityProvider(logger_provider)
        # Mock the spark session
        provider.spark = MagicMock()
        return provider

    @pytest.fixture(autouse=True)
    def mock_spark_session(self, provider):
        """Route Spark session factory calls to the mocked provider spark."""
        with patch(
            "kindling.entity_provider_csv.get_or_create_spark_session", return_value=provider.spark
        ):
            yield

    @pytest.fixture
    def entity_metadata(self):
        """Create sample entity metadata for CSV"""
        return EntityMetadata(
            entityid="ref.categories",
            name="categories",
            partition_columns=[],
            merge_columns=["id"],
            tags={
                "provider_type": "csv",
                "provider.path": "Files/reference/categories.csv",
                "provider.header": "true",
                "provider.inferSchema": "true",
            },
            schema=None,
        )

    def test_read_entity_success(self, provider, entity_metadata):
        """Test successful CSV read"""
        mock_df = MagicMock()
        mock_df.count.return_value = 100
        mock_df.columns = ["id", "name", "description"]

        mock_reader = MagicMock()
        mock_reader.load.return_value = mock_df
        mock_reader.option.return_value = mock_reader
        mock_reader.options.return_value = mock_reader

        provider.spark.read.format.return_value = mock_reader

        result = provider.read_entity(entity_metadata)

        assert result == mock_df
        provider.spark.read.format.assert_called_with("csv")
        mock_reader.load.assert_called_with("Files/reference/categories.csv")

    def test_read_entity_missing_path_raises(self, provider):
        """Test that missing path in config raises ValueError"""
        entity = EntityMetadata(
            entityid="ref.categories",
            name="categories",
            partition_columns=[],
            merge_columns=["id"],
            tags={"provider_type": "csv"},  # Missing 'provider.path'
            schema=None,
        )

        with pytest.raises(ValueError, match="CSV provider requires 'path'"):
            provider.read_entity(entity)

    def test_read_entity_with_custom_delimiter(self, provider):
        """Test CSV read with custom delimiter"""
        entity = EntityMetadata(
            entityid="ref.data",
            name="data",
            partition_columns=[],
            merge_columns=[],
            tags={
                "provider_type": "csv",
                "provider.path": "Files/data.tsv",
                "provider.delimiter": "\t",  # Tab-separated
                "provider.header": "true",
            },
            schema=None,
        )

        mock_reader = MagicMock()
        mock_reader.load.return_value = MagicMock()
        mock_reader.option.return_value = mock_reader
        mock_reader.options.return_value = mock_reader

        provider.spark.read.format.return_value = mock_reader

        provider.read_entity(entity)

        # Verify delimiter option was set
        calls = [str(call) for call in mock_reader.option.call_args_list]
        assert any("delimiter" in call for call in calls)

    def test_read_entity_with_additional_options(self, provider):
        """Test CSV read with additional Spark options"""
        entity = EntityMetadata(
            entityid="ref.data",
            name="data",
            partition_columns=[],
            merge_columns=[],
            tags={
                "provider_type": "csv",
                "provider.path": "Files/data.csv",
                "provider.dateFormat": "yyyy-MM-dd",
                "provider.timestampFormat": "yyyy-MM-dd HH:mm:ss",
            },
            schema=None,
        )

        mock_reader = MagicMock()
        mock_reader.load.return_value = MagicMock()
        mock_reader.option.return_value = mock_reader
        mock_reader.options.return_value = mock_reader

        provider.spark.read.format.return_value = mock_reader

        provider.read_entity(entity)

        # Verify additional options were passed
        mock_reader.options.assert_called_once()

    def test_read_entity_defaults(self, provider):
        """Test that default CSV options are applied"""
        entity = EntityMetadata(
            entityid="ref.simple",
            name="simple",
            partition_columns=[],
            merge_columns=[],
            tags={
                "provider_type": "csv",
                "provider.path": "Files/simple.csv",
                # No other options - should use defaults
            },
            schema=None,
        )

        mock_reader = MagicMock()
        mock_reader.load.return_value = MagicMock()
        mock_reader.option.return_value = mock_reader
        mock_reader.options.return_value = mock_reader

        provider.spark.read.format.return_value = mock_reader

        provider.read_entity(entity)

        # Should have called option() for defaults
        assert mock_reader.option.called

    def test_check_entity_exists_true(self, provider, entity_metadata):
        """Test check_entity_exists returns True when CSV file exists"""
        mock_reader = MagicMock()
        mock_reader.load.return_value = MagicMock(schema=MagicMock())
        mock_reader.option.return_value = mock_reader

        provider.spark.read.format.return_value = mock_reader

        result = provider.check_entity_exists(entity_metadata)

        assert result is True

    def test_check_entity_exists_false(self, provider, entity_metadata):
        """Test check_entity_exists returns False when CSV file doesn't exist"""
        mock_reader = MagicMock()
        mock_reader.load.side_effect = Exception("File not found")
        mock_reader.option.return_value = mock_reader

        provider.spark.read.format.return_value = mock_reader

        result = provider.check_entity_exists(entity_metadata)

        assert result is False

    def test_check_entity_exists_missing_path(self, provider):
        """Test check_entity_exists returns False when path is missing"""
        entity = EntityMetadata(
            entityid="ref.test",
            name="test",
            partition_columns=[],
            merge_columns=[],
            tags={"provider_type": "csv"},  # Missing provider.path
            schema=None,
        )

        result = provider.check_entity_exists(entity)

        assert result is False

    def test_read_entity_failure_logs_error(self, provider, entity_metadata):
        """Test that read failures are logged with traceback"""
        mock_reader = MagicMock()
        mock_reader.load.side_effect = Exception("Read error")
        mock_reader.option.return_value = mock_reader
        mock_reader.options.return_value = mock_reader

        provider.spark.read.format.return_value = mock_reader

        with pytest.raises(Exception, match="Read error"):
            provider.read_entity(entity_metadata)

        # Verify error was logged
        provider.logger.error.assert_called_once()
        call_kwargs = provider.logger.error.call_args[1]
        assert call_kwargs.get("include_traceback") is True


class TestCSVProviderConfiguration:
    """Tests for CSV provider configuration handling"""

    @pytest.fixture
    def provider(self):
        """Create CSV provider"""
        logger_provider = MagicMock()
        logger_provider.get_logger.return_value = MagicMock()
        provider = CSVEntityProvider(logger_provider)
        provider.spark = MagicMock()
        return provider

    @pytest.fixture(autouse=True)
    def mock_spark_session(self, provider):
        """Route Spark session factory calls to the mocked provider spark."""
        with patch(
            "kindling.entity_provider_csv.get_or_create_spark_session", return_value=provider.spark
        ):
            yield

    def test_no_provider_config_uses_empty_dict(self, provider):
        """Test that empty tags result in missing path error"""
        entity = EntityMetadata(
            entityid="ref.test",
            name="test",
            partition_columns=[],
            merge_columns=[],
            tags={},  # No provider tags
            schema=None,
        )

        with pytest.raises(ValueError, match="CSV provider requires 'path'"):
            provider.read_entity(entity)

    def test_multiline_option_supported(self, provider):
        """Test that multiLine option is supported"""
        entity = EntityMetadata(
            entityid="ref.multiline",
            name="multiline",
            partition_columns=[],
            merge_columns=[],
            tags={
                "provider_type": "csv",
                "provider.path": "Files/multiline.csv",
                "provider.multiLine": "true",
            },
            schema=None,
        )

        mock_reader = MagicMock()
        mock_reader.load.return_value = MagicMock()
        mock_reader.option.return_value = mock_reader
        mock_reader.options.return_value = mock_reader

        provider.spark.read.format.return_value = mock_reader

        provider.read_entity(entity)

        # Verify multiLine option was set
        calls = [str(call) for call in mock_reader.option.call_args_list]
        assert any("multiLine" in call for call in calls)


class TestCSVEntityProviderWrites:
    """Tests for CSVEntityProvider write operations."""

    @pytest.fixture
    def provider(self):
        logger_provider = MagicMock()
        logger_provider.get_logger.return_value = MagicMock()
        return CSVEntityProvider(logger_provider)

    @pytest.fixture
    def entity(self):
        return EntityMetadata(
            entityid="output.results",
            name="results",
            partition_columns=[],
            merge_columns=[],
            tags={
                "provider_type": "csv",
                "provider.path": "Files/output/results.csv",
            },
            schema=None,
        )

    def _mock_writer(self):
        writer = MagicMock()
        writer.format.return_value = writer
        writer.mode.return_value = writer
        writer.option.return_value = writer
        writer.save.return_value = None
        return writer

    def test_write_to_entity_uses_overwrite_mode(self, provider, entity):
        df = MagicMock()
        writer = self._mock_writer()
        df.write = writer

        provider.write_to_entity(df, entity)

        writer.mode.assert_called_once_with("overwrite")
        writer.save.assert_called_once_with("Files/output/results.csv")

    def test_append_to_entity_uses_append_mode(self, provider, entity):
        df = MagicMock()
        writer = self._mock_writer()
        df.write = writer

        provider.append_to_entity(df, entity)

        writer.mode.assert_called_once_with("append")
        writer.save.assert_called_once_with("Files/output/results.csv")

    def test_write_to_entity_missing_path_raises(self, provider):
        df = MagicMock()
        entity = EntityMetadata(
            entityid="output.results",
            name="results",
            partition_columns=[],
            merge_columns=[],
            tags={"provider_type": "csv"},
            schema=None,
        )
        with pytest.raises(ValueError, match="CSV provider requires 'path'"):
            provider.write_to_entity(df, entity)

    def test_write_to_entity_uses_csv_format(self, provider, entity):
        df = MagicMock()
        writer = self._mock_writer()
        df.write = writer

        provider.write_to_entity(df, entity)

        writer.format.assert_called_once_with("csv")

    def test_write_to_entity_passes_custom_delimiter(self, provider):
        df = MagicMock()
        writer = self._mock_writer()
        df.write = writer
        entity = EntityMetadata(
            entityid="output.tsv",
            name="tsv",
            partition_columns=[],
            merge_columns=[],
            tags={
                "provider_type": "csv",
                "provider.path": "Files/output.tsv",
                "provider.delimiter": "\t",
            },
            schema=None,
        )

        provider.write_to_entity(df, entity)

        option_calls = {c.args[0]: c.args[1] for c in writer.option.call_args_list}
        assert option_calls.get("delimiter") == "\t"

    def test_write_to_entity_defaults_compression_to_none(self, provider, entity):
        df = MagicMock()
        writer = self._mock_writer()
        df.write = writer

        provider.write_to_entity(df, entity)

        option_calls = {c.args[0]: c.args[1] for c in writer.option.call_args_list}
        assert option_calls.get("compression") == "none"

    def test_write_to_entity_passes_custom_compression(self, provider):
        df = MagicMock()
        writer = self._mock_writer()
        df.write = writer
        entity = EntityMetadata(
            entityid="output.results_gz",
            name="results_gz",
            partition_columns=[],
            merge_columns=[],
            tags={
                "provider_type": "csv",
                "provider.path": "Files/output/results.csv",
                "provider.compression": "gzip",
            },
            schema=None,
        )

        provider.write_to_entity(df, entity)

        option_calls = {c.args[0]: c.args[1] for c in writer.option.call_args_list}
        assert option_calls.get("compression") == "gzip"


def test_csv_entity_provider_implements_writable_entity_provider():
    from unittest.mock import MagicMock

    from kindling.entity_provider import WritableEntityProvider
    from kindling.entity_provider_csv import CSVEntityProvider

    logger_provider = MagicMock()
    logger_provider.get_logger.return_value = MagicMock()
    provider = CSVEntityProvider(logger_provider)

    assert isinstance(provider, WritableEntityProvider)
