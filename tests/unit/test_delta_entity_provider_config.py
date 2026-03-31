"""
Unit tests for DeltaEntityProvider tag-based configuration.

Tests that DeltaEntityProvider correctly uses tag-based overrides
while maintaining backward compatibility with service-based configuration.
"""

from unittest.mock import MagicMock, Mock

import pytest

from kindling.data_entities import EntityMetadata, EntityNameMapper, EntityPathLocator
from kindling.entity_provider_delta import DeltaEntityProvider, DeltaTableReference
from kindling.signaling import SignalProvider
from kindling.spark_config import ConfigService
from kindling.spark_log_provider import PythonLoggerProvider


class TestDeltaEntityProviderConfig:
    """Unit tests for DeltaEntityProvider tag-based configuration."""

    @pytest.fixture(autouse=True)
    def mock_spark_session(self, monkeypatch):
        spark = MagicMock()
        monkeypatch.setattr(
            "kindling.entity_provider_delta.get_or_create_spark_session", lambda: spark
        )
        return spark

    @pytest.fixture
    def mock_dependencies(self):
        """Create mock dependencies for DeltaEntityProvider."""
        config = MagicMock(spec=ConfigService)

        def _config_get(key, default=None):
            if key == "kindling.delta.access_mode":
                return "catalog"
            if key.startswith("kindling.features.") or key.startswith("kindling.runtime.features."):
                return default
            return default

        config.get.side_effect = _config_get

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
        assert table_ref.access_mode == "catalog"

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
                "provider.access_mode": "storage",
            },
            schema=None,
        )

        table_ref = provider._get_table_reference(entity)

        # Should use tag override for access mode
        assert table_ref.access_mode == "storage"

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
                "provider.access_mode": "catalog",
            },
            schema=None,
        )

        table_ref = provider._get_table_reference(entity)

        # Should use all tag overrides
        assert table_ref.table_path == "Tables/custom/sales"
        assert table_ref.table_name == "custom.db.sales"
        assert table_ref.access_mode == "catalog"

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
        assert table_ref.access_mode == "catalog"
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

    def test_for_name_mode_allows_missing_path_during_reference_resolution(self, mock_dependencies):
        """catalog mode should not fail if path locator cannot resolve before table exists."""
        mock_dependencies["path_locator"].get_table_path.side_effect = ValueError(
            "table does not exist yet"
        )

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
            partition_columns=[],
            merge_columns=["id"],
            tags={},
            schema=None,
        )

        table_ref = provider._get_table_reference(entity)
        assert table_ref.table_name == "default_catalog.default_db.default_table"
        assert table_ref.table_path is None

    def test_ensure_table_exists_creates_managed_table_when_path_missing_for_name_mode(
        self, mock_dependencies
    ):
        """catalog mode can bootstrap table by name when path is unavailable."""
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
            partition_columns=[],
            merge_columns=["id"],
            tags={},
            schema=None,
        )
        table_ref = DeltaTableReference(
            table_name="default_catalog.default_db.default_table",
            table_path=None,
            access_mode="catalog",
        )

        provider._check_catalog_table_exists = MagicMock(return_value=False)
        provider._create_managed_table = MagicMock(
            side_effect=lambda _entity, ref: setattr(ref, "table_path", "abfss://managed/location")
        )
        provider._resolve_catalog_table_location = MagicMock(
            return_value="abfss://managed/location"
        )

        provider._ensure_table_exists(entity, table_ref)

        provider._create_managed_table.assert_called_once_with(entity, table_ref)
        assert table_ref.table_path == "abfss://managed/location"

    def test_ensure_entity_table_emits_failed_when_reference_resolution_fails(
        self, mock_dependencies
    ):
        """ensure_entity_table should emit failure even when table ref lookup fails."""
        mock_dependencies["config"].get.side_effect = lambda key, default=None: (
            "storage" if key == "kindling.delta.access_mode" else default
        )
        mock_dependencies["path_locator"].get_table_path.side_effect = RuntimeError(
            "path lookup failed"
        )

        provider = DeltaEntityProvider(
            config=mock_dependencies["config"],
            entity_name_mapper=mock_dependencies["entity_name_mapper"],
            path_locator=mock_dependencies["path_locator"],
            tp=mock_dependencies["logger_provider"],
            signal_provider=mock_dependencies["signal_provider"],
        )
        provider.emit = MagicMock()

        entity = EntityMetadata(
            entityid="sales.transactions",
            name="transactions",
            partition_columns=[],
            merge_columns=["id"],
            tags={},
            schema=None,
        )

        with pytest.raises(RuntimeError):
            provider.ensure_entity_table(entity)

        emitted = [c.args[0] for c in provider.emit.call_args_list]
        assert "entity.before_ensure_table" in emitted
        assert "entity.ensure_failed" in emitted

    def test_write_for_name_managed_table_does_not_re_register_with_location(
        self, mock_dependencies
    ):
        """Managed catalog writes should not issue a redundant CREATE TABLE ... LOCATION statement."""
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
            partition_columns=[],
            merge_columns=["id"],
            tags={},
            schema=None,
        )
        table_ref = DeltaTableReference(
            table_name="default_catalog.default_db.default_table",
            table_path=None,
            access_mode="catalog",
        )

        df = MagicMock()
        writer = MagicMock()
        df.write.format.return_value = writer
        writer.option.return_value = writer
        writer.mode.return_value = writer

        provider._resolve_catalog_table_location = MagicMock(
            return_value="abfss://managed/location"
        )
        provider.spark.sql = MagicMock()

        provider._write_to_delta_table(df, entity, table_ref)

        writer.mode.assert_called_once_with("append")
        writer.saveAsTable.assert_called_once_with("default_catalog.default_db.default_table")
        provider.spark.sql.assert_not_called()
        assert table_ref.table_path == "abfss://managed/location"

    def test_write_for_name_prefers_save_as_table_when_location_is_known(self, mock_dependencies):
        """catalog writes should remain name-based even if a managed location is already known."""
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
            partition_columns=[],
            merge_columns=["id"],
            tags={},
            schema=None,
        )
        table_ref = DeltaTableReference(
            table_name="default_catalog.default_db.default_table",
            table_path="abfss://managed/location",
            access_mode="catalog",
        )

        df = MagicMock()
        writer = MagicMock()
        df.write.format.return_value = writer
        writer.option.return_value = writer
        writer.mode.return_value = writer

        provider._resolve_catalog_table_location = MagicMock(
            return_value="abfss://managed/location"
        )

        provider._write_to_delta_table(df, entity, table_ref)

        writer.mode.assert_called_once_with("append")
        writer.saveAsTable.assert_called_once_with("default_catalog.default_db.default_table")
        writer.save.assert_not_called()

    def test_append_for_name_prefers_save_as_table_when_location_is_known(self, mock_dependencies):
        """catalog append should keep table-name writes, even when table location is known."""
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
            partition_columns=[],
            merge_columns=["id"],
            tags={},
            schema=None,
        )
        table_ref = DeltaTableReference(
            table_name="default_catalog.default_db.default_table",
            table_path="abfss://managed/location",
            access_mode="catalog",
        )

        df = MagicMock()
        writer = MagicMock()
        df.write.format.return_value = writer
        writer.mode.return_value = writer
        writer.option.return_value = writer

        provider._append_to_delta_table(df, entity, table_ref)

        writer.saveAsTable.assert_called_once_with("default_catalog.default_db.default_table")
        writer.save.assert_not_called()

    def test_write_for_path_appends_to_existing_path(self, mock_dependencies):
        """storage writes should append into ensured paths instead of trying to recreate them."""
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
            partition_columns=[],
            merge_columns=["id"],
            tags={},
            schema=None,
        )
        table_ref = DeltaTableReference(
            table_name=None,
            table_path="Tables/default/path",
            access_mode="storage",
        )

        df = MagicMock()
        writer = MagicMock()
        df.write.format.return_value = writer
        writer.option.return_value = writer
        writer.mode.return_value = writer

        provider._write_to_delta_table(df, entity, table_ref)

        writer.option.assert_called_with("path", "Tables/default/path")
        writer.mode.assert_called_once_with("append")
        writer.save.assert_called_once_with()

    def test_write_to_entity_ensures_destination_before_write(self, mock_dependencies):
        """Batch writes should ensure the destination so entity metadata can prepare the table."""
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
            partition_columns=[],
            merge_columns=["id"],
            tags={},
            schema=[Mock(name="id_field")],
        )

        df = MagicMock()
        provider._ensure_destination_for_write = MagicMock()
        provider._write_to_delta_table = MagicMock()

        provider.write_to_entity(df, entity)

        provider._ensure_destination_for_write.assert_called_once()
        provider._write_to_delta_table.assert_called_once()

    def test_ensure_destination_for_write_falls_back_for_schema_less_for_path(
        self, mock_dependencies
    ):
        """Schema-less storage entities should preserve legacy implicit-create behavior."""
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
            partition_columns=[],
            merge_columns=["id"],
            tags={},
            schema=None,
        )
        table_ref = DeltaTableReference(
            table_name="default_catalog.default_db.default_table",
            table_path="Tables/default/path",
            access_mode="storage",
        )

        provider._ensure_table_exists = MagicMock(
            side_effect=ValueError(
                "Cannot create physical Delta table at 'Tables/default/path' without schema"
            )
        )

        provider._ensure_destination_for_write(entity, table_ref)

        provider._ensure_table_exists.assert_called_once_with(entity, table_ref)
        provider.logger.warning.assert_called()

    def test_ensure_destination_for_write_respects_config_opt_out(self, mock_dependencies):
        """Teams can disable ensure-before-write with normal Delta config."""

        def _config_get(key, default=None):
            if key == "kindling.delta.ensure_on_write":
                return False
            if key == "kindling.delta.access_mode":
                return "catalog"
            return default

        mock_dependencies["config"].get.side_effect = _config_get

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
            partition_columns=[],
            merge_columns=["id"],
            tags={},
            schema=[Mock(name="id_field")],
        )
        table_ref = DeltaTableReference(
            table_name="default_catalog.default_db.default_table",
            table_path="Tables/default/path",
            access_mode="storage",
        )

        provider._ensure_table_exists = MagicMock()

        provider._ensure_destination_for_write(entity, table_ref)

        provider._ensure_table_exists.assert_not_called()

    def test_ensure_for_name_with_existing_location_skips_physical_table_creation(
        self, mock_dependencies
    ):
        """catalog ensure should rely on catalog semantics and never create physical path tables."""
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
            partition_columns=[],
            merge_columns=["id"],
            tags={},
            schema=None,
        )
        table_ref = DeltaTableReference(
            table_name="default_catalog.default_db.default_table",
            table_path="abfss://managed/location",
            access_mode="catalog",
        )

        provider._check_catalog_table_exists = MagicMock(return_value=True)
        provider._create_physical_table = MagicMock()
        provider._ensure_schema_applied = MagicMock()

        provider._ensure_table_exists(entity, table_ref)

        provider._create_physical_table.assert_not_called()

    def test_ensure_schema_for_name_uses_table_read_even_when_location_is_known(
        self, mock_dependencies
    ):
        """catalog schema checks should read by table name, not by managed location path."""
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
            partition_columns=[],
            merge_columns=["id"],
            tags={},
            schema=None,
        )
        table_ref = DeltaTableReference(
            table_name="default_catalog.default_db.default_table",
            table_path="abfss://managed/location",
            access_mode="catalog",
        )

        existing_df = MagicMock()
        existing_df.schema.fields = [MagicMock(name="id_field")]
        provider.spark.read.table.return_value = existing_df

        provider._ensure_schema_applied(entity, table_ref)

        provider.spark.read.table.assert_called_once_with(
            "default_catalog.default_db.default_table"
        )
        provider.spark.read.format.return_value.load.assert_not_called()
