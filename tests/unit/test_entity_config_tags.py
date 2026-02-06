"""
Unit tests for entity tag configuration via ConfigService.

Tests retrieval-time tag merging where config-based tags override base entity tags.
"""

from unittest.mock import MagicMock

import pytest
from kindling.data_entities import DataEntityManager, EntityMetadata


class TestEntityConfigTags:
    """Tests for entity tag configuration and retrieval-time merging"""

    @pytest.fixture
    def config_service(self):
        """Create mock config service with entity tags"""
        mock_config = MagicMock()
        # Default: no config tags
        mock_config.get_entity_tags.return_value = {}
        return mock_config

    @pytest.fixture
    def manager(self, config_service):
        """Create entity manager with mocked config service"""
        signal_provider = MagicMock()
        return DataEntityManager(signal_provider, config_service)

    def test_get_entity_without_config_tags(self, manager, config_service):
        """Test getting entity when no config tags are set"""
        # Register entity with base tags
        manager.register_entity(
            "bronze.orders",
            name="orders",
            partition_columns=["date"],
            merge_columns=["order_id"],
            tags={"layer": "bronze", "provider.type": "delta"},
            schema=None,
        )

        # Get entity - should return base tags only
        entity = manager.get_entity_definition("bronze.orders")

        assert entity is not None
        assert entity.tags == {"layer": "bronze", "provider.type": "delta"}
        config_service.get_entity_tags.assert_called_once_with("bronze.orders")

    def test_get_entity_with_config_tags(self, manager, config_service):
        """Test retrieval-time merging of config tags with base tags"""
        # Set up config tags
        config_service.get_entity_tags.return_value = {
            "provider.path": "abfss://production@storage/bronze/orders",
            "environment": "production",
        }

        # Register entity with base tags
        manager.register_entity(
            "bronze.orders",
            name="orders",
            partition_columns=["date"],
            merge_columns=["order_id"],
            tags={"layer": "bronze", "provider.type": "delta"},
            schema=None,
        )

        # Get entity - should merge base + config tags
        entity = manager.get_entity_definition("bronze.orders")

        assert entity is not None
        assert entity.tags == {
            "layer": "bronze",  # From base
            "provider.type": "delta",  # From base
            "provider.path": "abfss://production@storage/bronze/orders",  # From config
            "environment": "production",  # From config
        }

    def test_config_tags_override_base_tags(self, manager, config_service):
        """Test that config tags override base tags with same key"""
        # Set up config tags that override base tags
        config_service.get_entity_tags.return_value = {
            "provider.type": "csv",  # Override base value
            "environment": "production",
        }

        # Register entity with base tags
        manager.register_entity(
            "bronze.orders",
            name="orders",
            partition_columns=[],
            merge_columns=[],
            tags={"provider.type": "delta", "layer": "bronze"},
            schema=None,
        )

        # Get entity - config tag should override base tag
        entity = manager.get_entity_definition("bronze.orders")

        assert entity is not None
        assert entity.tags == {
            "provider.type": "csv",  # Overridden by config
            "layer": "bronze",  # Preserved from base
            "environment": "production",  # Added by config
        }

    def test_order_independence_config_before_registration(self, manager, config_service):
        """Test that config can be set before entity registration"""
        # Config is already set (simulated by fixture returning tags)
        config_service.get_entity_tags.return_value = {
            "environment": "production",
        }

        # Register entity AFTER config is already loaded
        manager.register_entity(
            "bronze.orders",
            name="orders",
            partition_columns=[],
            merge_columns=[],
            tags={"layer": "bronze"},
            schema=None,
        )

        # Get entity - should still merge correctly
        entity = manager.get_entity_definition("bronze.orders")

        assert entity is not None
        assert entity.tags == {
            "layer": "bronze",
            "environment": "production",
        }

    def test_get_nonexistent_entity_returns_none(self, manager, config_service):
        """Test that getting non-existent entity returns None"""
        entity = manager.get_entity_definition("nonexistent.entity")
        assert entity is None

    def test_entity_without_config_service(self):
        """Test that entity manager works without config service"""
        signal_provider = MagicMock()
        manager = DataEntityManager(signal_provider, config_service=None)

        # Register entity
        manager.register_entity(
            "bronze.orders",
            name="orders",
            partition_columns=[],
            merge_columns=[],
            tags={"layer": "bronze"},
            schema=None,
        )

        # Get entity - should return base tags only (no config merging)
        entity = manager.get_entity_definition("bronze.orders")

        assert entity is not None
        assert entity.tags == {"layer": "bronze"}

    def test_multiple_entities_with_different_config(self, manager, config_service):
        """Test multiple entities with different config overrides"""
        # Register multiple entities
        manager.register_entity(
            "bronze.orders",
            name="orders",
            partition_columns=[],
            merge_columns=[],
            tags={"layer": "bronze"},
            schema=None,
        )
        manager.register_entity(
            "bronze.customers",
            name="customers",
            partition_columns=[],
            merge_columns=[],
            tags={"layer": "bronze"},
            schema=None,
        )

        # Set up different config for each entity
        def mock_get_entity_tags(entityid):
            if entityid == "bronze.orders":
                return {"retention_days": "90"}
            elif entityid == "bronze.customers":
                return {"retention_days": "365"}
            return {}

        config_service.get_entity_tags.side_effect = mock_get_entity_tags

        # Get entities - each should have its own config
        orders = manager.get_entity_definition("bronze.orders")
        customers = manager.get_entity_definition("bronze.customers")

        assert orders.tags == {"layer": "bronze", "retention_days": "90"}
        assert customers.tags == {"layer": "bronze", "retention_days": "365"}
