"""
Unit tests for DeltaEntityProvider tag-based configuration.

Tests that DeltaEntityProvider correctly uses tag-based overrides
while maintaining backward compatibility with service-based configuration.
"""

from unittest.mock import MagicMock, Mock

import pytest
from kindling.data_entities import EntityMetadata, EntityNameMapper, EntityPathLocator
from kindling.entity_provider_delta import DeltaEntityProvider
from kindling.signaling import SignalProvider
from kindling.spark_config import ConfigService
from kindling.spark_log_provider import PythonLoggerProvider


class TestDeltaEntityProviderConfig:
    """Unit tests for DeltaEntityProvider tag-based configuration."""

    @pytest.fixture
    def mock_dependencies(self):
        """Create mock dependencies for DeltaEntityProvider."""
        config = MagicMock(spec=ConfigService)
        config.get.return_value = "auto"  # Default access mode

        entity_name_mapper = MagicMock(spec=EntityNameMapper)
        entity_name_mapper.get_table_name.return_value = "default_catalog.default_db.default_table"

        path_locator = MagicMock(spec=EntityPathLocator)
        path_locator.get_table_path.return_value = "Tables/default/path"

        logger_provider = MagicMock(spec=PythonLoggerProvider)
        logger = MagicMock()
        logger_provider.get_logger.return_value = logger

        signal_provider = MagicMock(spec=SignalProvider)

        return {
            "config": config,
            "entity_name_mapper": entity_name_mapper,
            "path_locator": path_locator,
            "logger_provider": logger_provider,
            "signal_provider": signal_provider,
        }

    def test_uses_service_defaults_when_no_tags(self, mock_dependencies):
        """Test that provider uses injected services when no tag overrides are present."""
        provider = DeltaEntityProvider(
            config=mock_dependencies["config"],
            entity_name_mapper=mock_dependencies["entity_name_mapper"],
            path_locator=mock_dependencies["path_locator"],
            tp=mock_dependencies["logger_provider"],
            signal_provider=mock_dependencies["signal_provider"],
        )

        entity = EntityMetadata(
            entityid="sales.transactions",
            name="transactions",
            partition_columns=["date"],
            merge_columns=["id"],
            tags={},  # No tag overrides
            schema=None,
        )

        table_ref = provider._get_table_reference(entity)

        # Should use service defaults
        assert table_ref.table_name == "default_catalog.default_db.default_table"
        assert table_ref.table_path == "Tables/default/path"
        assert table_ref.access_mode == "auto"

    def test_uses_tag_override_for_path(self, mock_dependencies):
        """Test that provider.path tag overrides EntityPathLocator."""
        provider = DeltaEntityProvider(
            config=mock_dependencies["config"],
            entity_name_mapper=mock_dependencies["entity_name_mapper"],
            path_locator=mock_dependencies["path_locator"],
            tp=mock_dependencies["logger_provider"],
            signal_provider=mock_dependencies["signal_provider"],
        )

        entity = EntityMetadata(
            entityid="sales.transactions",
            name="transactions",
            partition_columns=["date"],
            merge_columns=["id"],
            tags={
                "provider.path": "Tables/custom/sales_transactions",
            },
            schema=None,
        )

        table_ref = provider._get_table_reference(entity)

        # Should use tag override for path
        assert table_ref.table_path == "Tables/custom/sales_transactions"
        # Should still use service default for table name
        assert table_ref.table_name == "default_catalog.default_db.default_table"

    def test_uses_tag_override_for_table_name(self, mock_dependencies):
        """Test that provider.table_name tag overrides EntityNameMapper."""
        provider = DeltaEntityProvider(
            config=mock_dependencies["config"],
            entity_name_mapper=mock_dependencies["entity_name_mapper"],
            path_locator=mock_dependencies["path_locator"],
            tp=mock_dependencies["logger_provider"],
            signal_provider=mock_dependencies["signal_provider"],
        )

        entity = EntityMetadata(
            entityid="sales.transactions",
            name="transactions",
            partition_columns=["date"],
            merge_columns=["id"],
            tags={
                "provider.table_name": "custom_catalog.custom_db.custom_table",
            },
            schema=None,
        )

        table_ref = provider._get_table_reference(entity)

        # Should use tag override for table name
        assert table_ref.table_name == "custom_catalog.custom_db.custom_table"
        # Should still use service default for path
        assert table_ref.table_path == "Tables/default/path"

    def test_uses_tag_override_for_access_mode(self, mock_dependencies):
        """Test that provider.access_mode tag overrides config."""
        provider = DeltaEntityProvider(
            config=mock_dependencies["config"],
            entity_name_mapper=mock_dependencies["entity_name_mapper"],
            path_locator=mock_dependencies["path_locator"],
            tp=mock_dependencies["logger_provider"],
            signal_provider=mock_dependencies["signal_provider"],
        )

        entity = EntityMetadata(
            entityid="sales.transactions",
            name="transactions",
            partition_columns=["date"],
            merge_columns=["id"],
            tags={
                "provider.access_mode": "forPath",
            },
            schema=None,
        )

        table_ref = provider._get_table_reference(entity)

        # Should use tag override for access mode
        assert table_ref.access_mode == "forPath"

    def test_uses_all_tag_overrides_together(self, mock_dependencies):
        """Test that all tag overrides work together."""
        provider = DeltaEntityProvider(
            config=mock_dependencies["config"],
            entity_name_mapper=mock_dependencies["entity_name_mapper"],
            path_locator=mock_dependencies["path_locator"],
            tp=mock_dependencies["logger_provider"],
            signal_provider=mock_dependencies["signal_provider"],
        )

        entity = EntityMetadata(
            entityid="sales.transactions",
            name="transactions",
            partition_columns=["date"],
            merge_columns=["id"],
            tags={
                "provider.path": "Tables/custom/sales",
                "provider.table_name": "custom.db.sales",
                "provider.access_mode": "forName",
            },
            schema=None,
        )

        table_ref = provider._get_table_reference(entity)

        # Should use all tag overrides
        assert table_ref.table_path == "Tables/custom/sales"
        assert table_ref.table_name == "custom.db.sales"
        assert table_ref.access_mode == "forName"

    def test_validates_access_mode_value(self, mock_dependencies):
        """Test that invalid access_mode values are rejected with warning."""
        provider = DeltaEntityProvider(
            config=mock_dependencies["config"],
            entity_name_mapper=mock_dependencies["entity_name_mapper"],
            path_locator=mock_dependencies["path_locator"],
            tp=mock_dependencies["logger_provider"],
            signal_provider=mock_dependencies["signal_provider"],
        )

        entity = EntityMetadata(
            entityid="sales.transactions",
            name="transactions",
            partition_columns=["date"],
            merge_columns=["id"],
            tags={
                "provider.access_mode": "invalidMode",
            },
            schema=None,
        )

        table_ref = provider._get_table_reference(entity)

        # Should fall back to config default when invalid
        assert table_ref.access_mode == "auto"
        # Should have logged a warning
        provider.logger.warning.assert_called_once()

    def test_accesses_non_provider_tags(self, mock_dependencies):
        """Test that provider can access non-provider.* tags via _get_provider_config."""
        provider = DeltaEntityProvider(
            config=mock_dependencies["config"],
            entity_name_mapper=mock_dependencies["entity_name_mapper"],
            path_locator=mock_dependencies["path_locator"],
            tp=mock_dependencies["logger_provider"],
            signal_provider=mock_dependencies["signal_provider"],
        )

        entity = EntityMetadata(
            entityid="sales.transactions",
            name="transactions",
            partition_columns=["date"],
            merge_columns=["id"],
            tags={
                "provider.path": "Tables/sales",
                "region": "us-west",
                "pii": "true",
                "retention_days": "90",
            },
            schema=None,
        )

        # Get config to verify all tags are accessible
        config = provider._get_provider_config(entity)

        # Should have provider.* tags with prefix stripped
        assert config["path"] == "Tables/sales"

        # Should have non-provider tags included
        assert config["region"] == "us-west"
        assert config["pii"] is True  # Type converted
        assert config["retention_days"] == 90  # Type converted
