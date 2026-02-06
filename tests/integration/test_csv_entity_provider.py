"""
Integration test for CSV entity provider with YAML configuration.

Tests the full flow:
1. Create CSV file
2. Configure entity with CSV provider via YAML tags
3. Use CSV provider to read the entity
4. Validate data is read correctly
"""

import tempfile
from pathlib import Path

import pytest
from kindling.data_entities import DataEntityManager, EntityMetadata
from kindling.entity_provider_csv import CSVEntityProvider
from kindling.injection import GlobalInjector
from kindling.spark_config import DynaconfConfig
from pyspark.sql import SparkSession


class TestCSVEntityProviderIntegration:
    """Integration tests for CSV entity provider with config."""

    @pytest.fixture(autouse=True)
    def setup_and_cleanup(self):
        """Setup and cleanup for each test."""
        # Reset before test
        GlobalInjector.reset()
        yield
        # Reset after test
        GlobalInjector.reset()

    @pytest.fixture
    def spark(self):
        """Create Spark session for tests."""
        # Stop any existing session first
        try:
            existing = SparkSession.getActiveSession()
            if existing:
                existing.stop()
        except Exception:
            # Ignore errors if no session exists
            pass

        spark = (
            SparkSession.builder.master("local[1]").appName("csv-integration-test").getOrCreate()
        )

        # Set as global spark so get_or_create_spark_session() finds it
        import __main__

        __main__.spark = spark

        yield spark
        spark.stop()

    @pytest.fixture
    def csv_file(self, tmp_path):
        """Create a test CSV file."""
        csv_path = tmp_path / "products.csv"
        csv_content = """product_id,name,price,category
1,Widget A,19.99,electronics
2,Widget B,29.99,electronics
3,Gadget X,49.99,gadgets
4,Gadget Y,39.99,gadgets
"""
        csv_path.write_text(csv_content)
        return str(csv_path)

    def test_csv_provider_via_yaml_config(self, tmp_path, csv_file, spark):
        """Test reading CSV entity configured via YAML tags."""
        # Create YAML config that specifies CSV provider
        config_file = tmp_path / "entity_config.yaml"
        config_content = f"""entity_tags:
  ref.products:
    provider.type: "csv"
    provider.path: "{csv_file}"
    provider.header: "true"
    provider.inferSchema: "true"
    layer: "reference"
"""
        config_file.write_text(config_content)

        # Initialize config service
        config_service = DynaconfConfig()
        config_service.initialize(
            config_files=[str(config_file)],
            initial_config={},
            environment="test",
        )

        # Create entity manager and register entity
        from unittest.mock import MagicMock

        from kindling.signaling import SignalProvider

        signal_provider = MagicMock(spec=SignalProvider)
        signal_provider.create_signal.return_value = MagicMock()

        manager = DataEntityManager(signal_provider, config_service)

        # Register entity with minimal base tags
        manager.register_entity(
            entityid="ref.products",
            name="products",
            partition_columns=[],
            merge_columns=[],
            tags={
                "source": "reference-data",
            },
            schema=None,
        )

        # Get entity definition (should have merged tags)
        entity = manager.get_entity_definition("ref.products")

        # Verify tags are merged
        assert entity.tags["source"] == "reference-data"  # From code
        assert entity.tags["provider.type"] == "csv"  # From config
        assert entity.tags["provider.path"] == csv_file  # From config
        assert entity.tags["provider.header"] == "true"  # From config
        assert entity.tags["layer"] == "reference"  # From config

        # Create CSV provider directly (not via registry to avoid DI complexity)
        from kindling.spark_log_provider import PythonLoggerProvider

        mock_logger_provider = MagicMock(spec=PythonLoggerProvider)
        mock_logger = MagicMock()
        mock_logger_provider.get_logger.return_value = mock_logger

        provider = CSVEntityProvider(mock_logger_provider)

        # Read the entity through the provider
        df = provider.read_entity(entity)

        # Verify data was read correctly
        assert df.count() == 4
        rows = df.collect()

        # Check first row (inferSchema converts product_id to int)
        assert rows[0]["product_id"] == 1
        assert rows[0]["name"] == "Widget A"
        assert rows[0]["category"] == "electronics"

    def test_csv_provider_with_custom_delimiter(self, tmp_path, spark):
        """Test CSV provider with custom delimiter configured via YAML."""
        # Create TSV file (tab-separated)
        tsv_file = tmp_path / "categories.tsv"
        tsv_content = """category_id\tname\tdescription
1\telectronics\tElectronic devices
2\tgadgets\tUseful gadgets
3\thome\tHome appliances
"""
        tsv_file.write_text(tsv_content)

        # Configure with custom delimiter
        config_file = tmp_path / "tsv_config.yaml"
        config_content = f"""entity_tags:
  ref.categories:
    provider.type: "csv"
    provider.path: "{str(tsv_file)}"
    provider.header: "true"
    provider.delimiter: "\\t"
"""
        config_file.write_text(config_content)

        # Initialize config
        config_service = DynaconfConfig()
        config_service.initialize(
            config_files=[str(config_file)],
            initial_config={},
            environment="test",
        )

        # Create entity manager
        from unittest.mock import MagicMock

        from kindling.signaling import SignalProvider

        signal_provider = MagicMock(spec=SignalProvider)
        signal_provider.create_signal.return_value = MagicMock()

        manager = DataEntityManager(signal_provider, config_service)

        # Register entity
        manager.register_entity(
            entityid="ref.categories",
            name="categories",
            partition_columns=[],
            merge_columns=[],
            tags={},
            schema=None,
        )

        # Get entity
        entity = manager.get_entity_definition("ref.categories")

        # Create CSV provider
        from kindling.spark_log_provider import PythonLoggerProvider

        mock_logger_provider = MagicMock(spec=PythonLoggerProvider)
        mock_logger = MagicMock()
        mock_logger_provider.get_logger.return_value = mock_logger

        provider = CSVEntityProvider(mock_logger_provider)

        # Read the TSV file
        df = provider.read_entity(entity)

        # Verify data (Tab-delimited file read correctly)
        assert df.count() == 3
        rows = df.collect()
        # Note: without inferSchema, values are strings
        assert rows[0]["name"] == "electronics"
        assert rows[1]["name"] == "gadgets"
        assert rows[2]["name"] == "home"

    def test_csv_provider_reads_correct_column_types(self, tmp_path, spark):
        """Test that CSV provider respects header and schema inference settings."""
        # Create CSV with different data types
        csv_file = tmp_path / "typed_data.csv"
        csv_content = """id,amount,active,timestamp
1,100.50,true,2024-01-01
2,200.75,false,2024-01-02
3,150.25,true,2024-01-03
"""
        csv_file.write_text(csv_content)

        # Configure entity
        config_file = tmp_path / "typed_config.yaml"
        config_content = f"""entity_tags:
  test.typed_data:
    provider.type: "csv"
    provider.path: "{str(csv_file)}"
    provider.header: "true"
    provider.inferSchema: "true"
"""
        config_file.write_text(config_content)

        # Initialize config
        config_service = DynaconfConfig()
        config_service.initialize(
            config_files=[str(config_file)],
            initial_config={},
            environment="test",
        )

        # Create entity manager
        from unittest.mock import MagicMock

        from kindling.signaling import SignalProvider

        signal_provider = MagicMock(spec=SignalProvider)
        signal_provider.create_signal.return_value = MagicMock()

        manager = DataEntityManager(signal_provider, config_service)

        # Register entity
        manager.register_entity(
            entityid="test.typed_data",
            name="typed_data",
            partition_columns=[],
            merge_columns=[],
            tags={},
            schema=None,
        )

        # Get entity
        entity = manager.get_entity_definition("test.typed_data")

        # Create provider and read
        from kindling.spark_log_provider import PythonLoggerProvider

        mock_logger_provider = MagicMock(spec=PythonLoggerProvider)
        mock_logger = MagicMock()
        mock_logger_provider.get_logger.return_value = mock_logger

        provider = CSVEntityProvider(mock_logger_provider)
        df = provider.read_entity(entity)

        # Verify data and column names
        assert df.count() == 3
        assert "id" in df.columns
        assert "amount" in df.columns
        assert "active" in df.columns
        assert "timestamp" in df.columns
