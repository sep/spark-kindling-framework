"""
Unit tests for EntityProviderRegistry.

Tests provider registration, retrieval, and DI integration.
"""

from unittest.mock import MagicMock, patch

import pytest
from kindling.data_entities import EntityMetadata
from kindling.entity_provider import BaseEntityProvider
from kindling.entity_provider_registry import EntityProviderRegistry
from kindling.injection import GlobalInjector


class MockProvider(BaseEntityProvider):
    """Mock provider for testing"""

    def __init__(self, *args, **kwargs):
        """Accept any arguments for DI compatibility"""
        pass

    def read_entity(self, entity_metadata):
        return MagicMock()

    def check_entity_exists(self, entity_metadata):
        return True


class TestEntityProviderRegistry:
    """Tests for EntityProviderRegistry"""

    @pytest.fixture
    def registry(self):
        """Create a fresh registry for each test"""
        with patch("kindling.entity_provider_registry.GlobalInjector"):
            logger_provider = MagicMock()
            logger_provider.get_logger.return_value = MagicMock()
            return EntityProviderRegistry(logger_provider)

    def test_initialization(self, registry):
        """Test that registry initializes with builtin providers"""
        # Delta provider should always be registered
        assert "delta" in registry.list_registered_providers()

    def test_register_provider(self, registry):
        """Test registering a custom provider"""
        registry.register_provider("custom", MockProvider)

        assert "custom" in registry.list_registered_providers()

    def test_register_provider_overwrites_existing(self, registry):
        """Test that re-registering a provider type overwrites the previous one"""

        class Provider1(BaseEntityProvider):
            def read_entity(self, entity_metadata):
                return "provider1"

            def check_entity_exists(self, entity_metadata):
                return True

        class Provider2(BaseEntityProvider):
            def read_entity(self, entity_metadata):
                return "provider2"

            def check_entity_exists(self, entity_metadata):
                return False

        registry.register_provider("test", Provider1)
        registry.register_provider("test", Provider2)

        assert "test" in registry.list_registered_providers()
        # Should have the second provider class
        assert registry._provider_classes["test"] == Provider2

    def test_get_provider_unknown_type_raises(self, registry):
        """Test that getting an unknown provider type raises ValueError"""
        with pytest.raises(ValueError, match="Unknown provider type: 'nonexistent'"):
            registry.get_provider("nonexistent")

    def test_get_provider_returns_singleton(self, registry):
        """Test that get_provider returns the same instance on multiple calls"""
        registry.register_provider("test", MockProvider)

        with patch.object(GlobalInjector, "get", return_value=MockProvider(MagicMock())):
            provider1 = registry.get_provider("test")
            provider2 = registry.get_provider("test")

            assert provider1 is provider2

    def test_get_provider_for_entity_uses_provider_type(self, registry):
        """Test that get_provider_for_entity uses entity's provider_type"""
        registry.register_provider("custom", MockProvider)

        entity = EntityMetadata(
            entityid="test.entity",
            name="test_entity",
            partition_columns=[],
            merge_columns=["id"],
            tags={"provider_type": "custom"},
            schema=None,
        )

        with patch.object(GlobalInjector, "get", return_value=MockProvider(MagicMock())):
            provider = registry.get_provider_for_entity(entity)
            assert isinstance(provider, MockProvider)

    def test_get_provider_for_entity_defaults_to_delta(self, registry):
        """Test that get_provider_for_entity defaults to 'delta' when provider_type is not in tags"""
        entity = EntityMetadata(
            entityid="test.entity",
            name="test_entity",
            partition_columns=[],
            merge_columns=["id"],
            tags={},  # No provider_type tag
            schema=None,
        )

        with patch.object(GlobalInjector, "get") as mock_get:
            mock_get.return_value = MagicMock()
            registry.get_provider_for_entity(entity)

            # Should request 'delta' provider
            assert mock_get.called

    def test_list_registered_providers(self, registry):
        """Test listing all registered provider types"""
        registry.register_provider("test1", MockProvider)
        registry.register_provider("test2", MockProvider)

        providers = registry.list_registered_providers()

        assert "delta" in providers  # Builtin
        assert "test1" in providers
        assert "test2" in providers

    def test_get_provider_instantiation_failure_raises(self, registry):
        """Test that provider instantiation failure raises ValueError"""

        class FailingProvider(BaseEntityProvider):
            def __init__(self):
                raise RuntimeError("Instantiation failed")

            def read_entity(self, entity_metadata):
                pass

            def check_entity_exists(self, entity_metadata):
                pass

        registry.register_provider("failing", FailingProvider)

        with patch.object(GlobalInjector, "get", side_effect=RuntimeError("Instantiation failed")):
            with pytest.raises(ValueError, match="Failed to create provider instance"):
                registry.get_provider("failing")


class TestBuiltinProviders:
    """Tests for builtin provider registration"""

    def test_delta_provider_always_registered(self):
        """Test that Delta provider is always registered"""
        with patch("kindling.entity_provider_registry.GlobalInjector"):
            logger_provider = MagicMock()
            logger_provider.get_logger.return_value = MagicMock()
            registry = EntityProviderRegistry(logger_provider)

            assert "delta" in registry.list_registered_providers()

    def test_optional_providers_gracefully_handled(self):
        """Test that missing optional providers don't cause errors"""
        # This should not raise even if some providers are not available
        with patch("kindling.entity_provider_registry.GlobalInjector"):
            logger_provider = MagicMock()
            logger_provider.get_logger.return_value = MagicMock()
            registry = EntityProviderRegistry(logger_provider)

            # Registry should initialize successfully
            assert isinstance(registry, EntityProviderRegistry)
