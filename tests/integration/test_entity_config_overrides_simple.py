"""
Integration test for entity configuration overrides via YAML.

Simplified version that tests the core config → entity manager interaction
without requiring full framework bootstrap.
"""

import tempfile
from pathlib import Path

import pytest
from kindling.data_entities import DataEntityManager, EntityMetadata
from kindling.injection import GlobalInjector
from kindling.spark_config import DynaconfConfig


class TestEntityConfigOverridesSimple:
    """Simplified integration tests for entity configuration overrides."""

    @pytest.fixture(autouse=True)
    def setup_and_cleanup(self):
        """Setup and cleanup for each test."""
        # Reset before test
        GlobalInjector.reset()
        yield
        # Reset after test
        GlobalInjector.reset()

    def test_entity_tags_merged_from_config(self, tmp_path):
        """Test that entity tags from config are merged with base tags at retrieval."""
        # Create config file with entity tag overrides
        config_file = tmp_path / "test_config.yaml"
        config_content = """entity_tags:
  test.orders:
    provider.path: "/tmp/test-data/orders"
    environment: "test"
    retention_days: "30"
"""
        config_file.write_text(config_content)

        # Initialize config service with the YAML file
        config_service = DynaconfConfig()
        config_service.initialize(
            config_files=[str(config_file)],
            initial_config={},
            environment="test",
        )

        # Create entity manager with config service
        from unittest.mock import MagicMock

        from kindling.signaling import SignalProvider

        signal_provider = MagicMock(spec=SignalProvider)
        signal_provider.create_signal.return_value = MagicMock()

        manager = DataEntityManager(signal_provider, config_service)

        # Register entity with base tags
        manager.register_entity(
            entityid="test.orders",
            name="orders",
            partition_columns=["date"],
            merge_columns=["order_id"],
            tags={
                "layer": "bronze",
                "provider.type": "delta",
            },
            schema=None,
        )

        # Retrieve entity and verify tags are merged
        entity = manager.get_entity_definition("test.orders")

        assert entity is not None
        assert entity.tags["layer"] == "bronze"  # From code (base)
        assert entity.tags["provider.type"] == "delta"  # From code (base)
        assert entity.tags["provider.path"] == "/tmp/test-data/orders"  # From config
        assert entity.tags["environment"] == "test"  # From config
        assert entity.tags["retention_days"] == "30"  # From config

    def test_config_overrides_base_tags(self, tmp_path):
        """Test that config tags override base tags with the same key."""
        # Create config that overrides a base tag
        config_file = tmp_path / "override.yaml"
        config_content = """
entity_tags:
  test.orders:
    provider.type: "csv"
    environment: "test"
"""
        config_file.write_text(config_content)

        # Initialize config service
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

        # Register entity with base tags
        manager.register_entity(
            entityid="test.orders",
            name="orders",
            partition_columns=[],
            merge_columns=[],
            tags={
                "provider.type": "delta",  # This will be overridden
                "layer": "bronze",
            },
            schema=None,
        )

        # Retrieve and verify override
        entity = manager.get_entity_definition("test.orders")

        assert entity.tags["provider.type"] == "csv"  # Overridden by config
        assert entity.tags["layer"] == "bronze"  # Preserved from base
        assert entity.tags["environment"] == "test"  # Added by config

    def test_entity_without_config_uses_base_tags_only(self, tmp_path):
        """Test that entities without config overrides use only base tags."""
        # Create config with overrides for other entities
        config_file = tmp_path / "other.yaml"
        config_content = """
entity_tags:
  "test.orders":
    environment: "test"
"""
        config_file.write_text(config_content)

        # Initialize config service
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

        # Register entity that has NO config overrides
        manager.register_entity(
            entityid="test.products",  # Not in config
            name="products",
            partition_columns=[],
            merge_columns=[],
            tags={
                "layer": "bronze",
                "provider.type": "delta",
            },
            schema=None,
        )

        # Retrieve entity - should have only base tags
        entity = manager.get_entity_definition("test.products")

        assert entity.tags == {
            "layer": "bronze",
            "provider.type": "delta",
        }

    def test_config_service_direct_access(self, tmp_path):
        """Test that ConfigService can be used directly to get entity tags."""
        # Create config file
        config_file = tmp_path / "test.yaml"
        config_content = """
entity_tags:
  test.orders:
    provider.path: "/tmp/test-data/orders"
    environment: "test"
"""
        config_file.write_text(config_content)

        # Initialize config service
        config_service = DynaconfConfig()
        config_service.initialize(
            config_files=[str(config_file)],
            initial_config={},
            environment="test",
        )

        # Get entity tags directly from config service
        tags = config_service.get_entity_tags("test.orders")

        assert tags["provider.path"] == "/tmp/test-data/orders"
        assert tags["environment"] == "test"

        # Non-existent entity should return empty dict
        empty_tags = config_service.get_entity_tags("nonexistent.entity")
        assert empty_tags == {}

    def test_programmatic_config_override(self):
        """Test that config overrides can be set programmatically."""
        # Initialize config service without config files
        config_service = DynaconfConfig()
        config_service.initialize(
            config_files=[],
            initial_config={},
            environment="test",
        )

        # Set entity tags programmatically
        config_service.set_entity_tags(
            "test.orders",
            {
                "provider.path": "/programmatic/path",
                "environment": "dev",
            },
        )

        # Create entity manager
        from unittest.mock import MagicMock

        from kindling.signaling import SignalProvider

        signal_provider = MagicMock(spec=SignalProvider)
        signal_provider.create_signal.return_value = MagicMock()

        manager = DataEntityManager(signal_provider, config_service)

        # Register entity
        manager.register_entity(
            entityid="test.orders",
            name="orders",
            partition_columns=[],
            merge_columns=[],
            tags={"layer": "bronze"},
            schema=None,
        )

        # Retrieve and verify programmatic tags are merged
        entity = manager.get_entity_definition("test.orders")

        assert entity.tags["layer"] == "bronze"
        assert entity.tags["provider.path"] == "/programmatic/path"
        assert entity.tags["environment"] == "dev"
