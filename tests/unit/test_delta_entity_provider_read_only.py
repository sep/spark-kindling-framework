"""
Unit tests for read_only entity support in DeltaEntityProvider.

A `read_only` tag changes `ensure_entity_table` from "create or write" to
"register in catalog if not already registered," and makes all write paths
(`write_to_entity`, `merge_to_entity`, `append_to_entity`, `append_as_stream`)
raise immediately instead of touching the underlying table.
"""

from unittest.mock import MagicMock

import pytest

from kindling.data_entities import EntityMetadata, EntityNameMapper, EntityPathLocator
from kindling.entity_provider_delta import (
    DeltaEntityProvider,
    DeltaTableReference,
    EntityPathConflictError,
    ReadOnlyEntityError,
)
from kindling.signaling import SignalProvider
from kindling.spark_config import ConfigService
from kindling.spark_log_provider import PythonLoggerProvider


class TestDeltaEntityProviderReadOnly:
    """Unit tests for the read_only tag guard on DeltaEntityProvider."""

    @pytest.fixture(autouse=True)
    def mock_spark_session(self, monkeypatch):
        spark = MagicMock()
        monkeypatch.setattr(
            "kindling.entity_provider_delta.get_or_create_spark_session", lambda: spark
        )
        return spark

    @pytest.fixture
    def mock_dependencies(self):
        config = MagicMock(spec=ConfigService)

        def _config_get(key, default=None):
            if key == "kindling.delta.access_mode":
                return "catalog"
            return default

        config.get.side_effect = _config_get

        entity_name_mapper = MagicMock(spec=EntityNameMapper)
        entity_name_mapper.get_table_name.return_value = "default_catalog.default_db.default_table"

        path_locator = MagicMock(spec=EntityPathLocator)
        path_locator.get_table_path.return_value = "abfss://curated@storage/reference/data"

        logger_provider = MagicMock(spec=PythonLoggerProvider)
        logger_provider.get_logger.return_value = MagicMock()

        return {
            "config": config,
            "entity_name_mapper": entity_name_mapper,
            "path_locator": path_locator,
            "logger_provider": logger_provider,
            "signal_provider": MagicMock(spec=SignalProvider),
        }

    def _make_provider(self, mock_dependencies):
        return DeltaEntityProvider(
            config=mock_dependencies["config"],
            entity_name_mapper=mock_dependencies["entity_name_mapper"],
            path_locator=mock_dependencies["path_locator"],
            tp=mock_dependencies["logger_provider"],
            signal_provider=mock_dependencies["signal_provider"],
        )

    def _make_entity(self, tags):
        return EntityMetadata(
            entityid="shared.reference_data",
            name="reference_data",
            partition_columns=[],
            merge_columns=[],
            tags=tags,
            schema=None,
        )

    # -- write guards --------------------------------------------------

    def test_write_to_entity_raises_for_read_only_entity(self, mock_dependencies):
        provider = self._make_provider(mock_dependencies)
        entity = self._make_entity({"read_only": "true"})

        with pytest.raises(ReadOnlyEntityError):
            provider.write_to_entity(MagicMock(), entity)

        provider.spark.sql.assert_not_called()

    def test_merge_to_entity_raises_for_read_only_entity(self, mock_dependencies):
        provider = self._make_provider(mock_dependencies)
        entity = self._make_entity({"read_only": "true"})

        with pytest.raises(ReadOnlyEntityError):
            provider.merge_to_entity(MagicMock(), entity)

        provider.spark.sql.assert_not_called()

    def test_append_to_entity_raises_for_read_only_entity(self, mock_dependencies):
        provider = self._make_provider(mock_dependencies)
        entity = self._make_entity({"read_only": "true"})

        with pytest.raises(ReadOnlyEntityError):
            provider.append_to_entity(MagicMock(), entity)

        provider.spark.sql.assert_not_called()

    def test_append_as_stream_raises_for_read_only_entity(self, mock_dependencies):
        provider = self._make_provider(mock_dependencies)
        entity = self._make_entity({"read_only": "true"})

        with pytest.raises(ReadOnlyEntityError):
            provider.append_as_stream(MagicMock(), entity, checkpointLocation="/chk")

    def test_write_is_allowed_when_read_only_tag_absent(self, mock_dependencies):
        """Sanity check: normal entities are unaffected by the new guard."""
        provider = self._make_provider(mock_dependencies)
        entity = self._make_entity({})

        provider._ensure_destination_for_write = MagicMock()
        provider._write_to_delta_table = MagicMock()

        provider.write_to_entity(MagicMock(), entity)

        provider._write_to_delta_table.assert_called_once()

    # -- ensure_table_exists / external registration --------------------

    def test_ensure_table_exists_registers_external_table_when_not_in_catalog(
        self, mock_dependencies
    ):
        provider = self._make_provider(mock_dependencies)
        entity = self._make_entity({"read_only": "true"})
        table_ref = DeltaTableReference(
            table_name="shared.reference_data",
            table_path="abfss://curated@storage/reference/data",
            access_mode="catalog",
        )

        provider._check_catalog_table_exists = MagicMock(return_value=False)
        provider._create_physical_table = MagicMock()
        provider._create_managed_table = MagicMock()

        provider._ensure_table_exists(entity, table_ref)

        provider._create_physical_table.assert_not_called()
        provider._create_managed_table.assert_not_called()
        provider.spark.sql.assert_called_once()
        sql = provider.spark.sql.call_args[0][0]
        assert "CREATE TABLE IF NOT EXISTS shared.reference_data" in sql
        assert "LOCATION 'abfss://curated@storage/reference/data'" in sql

    def test_ensure_table_exists_is_noop_when_already_registered_at_same_path(
        self, mock_dependencies
    ):
        provider = self._make_provider(mock_dependencies)
        entity = self._make_entity({"read_only": "true"})
        table_ref = DeltaTableReference(
            table_name="shared.reference_data",
            table_path="abfss://curated@storage/reference/data",
            access_mode="catalog",
        )

        provider._check_catalog_table_exists = MagicMock(return_value=True)
        provider._resolve_catalog_table_location = MagicMock(
            return_value="abfss://curated@storage/reference/data"
        )

        provider._ensure_table_exists(entity, table_ref)

        provider.spark.sql.assert_not_called()

    def test_ensure_table_exists_raises_on_path_conflict_by_default(self, mock_dependencies):
        provider = self._make_provider(mock_dependencies)
        entity = self._make_entity({"read_only": "true"})
        table_ref = DeltaTableReference(
            table_name="shared.reference_data",
            table_path="abfss://curated@storage/reference/data",
            access_mode="catalog",
        )

        provider._check_catalog_table_exists = MagicMock(return_value=True)
        provider._resolve_catalog_table_location = MagicMock(
            return_value="abfss://other@storage/other/path"
        )

        with pytest.raises(EntityPathConflictError):
            provider._ensure_table_exists(entity, table_ref)

        provider.spark.sql.assert_not_called()

    def test_ensure_table_exists_reregisters_on_path_conflict_when_opted_in(
        self, mock_dependencies
    ):
        provider = self._make_provider(mock_dependencies)
        entity = self._make_entity(
            {"read_only": "true", "read_only.on_path_conflict": "reregister"}
        )
        table_ref = DeltaTableReference(
            table_name="shared.reference_data",
            table_path="abfss://curated@storage/reference/data",
            access_mode="catalog",
        )

        provider._check_catalog_table_exists = MagicMock(return_value=True)
        provider._resolve_catalog_table_location = MagicMock(
            return_value="abfss://other@storage/other/path"
        )

        provider._ensure_table_exists(entity, table_ref)

        calls = [c.args[0] for c in provider.spark.sql.call_args_list]
        assert any("DROP TABLE IF EXISTS shared.reference_data" in c for c in calls)
        assert any("CREATE TABLE IF NOT EXISTS shared.reference_data" in c for c in calls)

    def test_ensure_table_exists_skips_registration_in_storage_mode(self, mock_dependencies):
        provider = self._make_provider(mock_dependencies)
        entity = self._make_entity({"read_only": "true"})
        table_ref = DeltaTableReference(
            table_name=None,
            table_path="abfss://curated@storage/reference/data",
            access_mode="storage",
        )

        provider._ensure_table_exists(entity, table_ref)

        provider.spark.sql.assert_not_called()

    # -- reads are unaffected --------------------------------------------

    def test_read_entity_is_not_blocked_for_read_only_entity(self, mock_dependencies):
        provider = self._make_provider(mock_dependencies)
        entity = self._make_entity({"read_only": "true"})

        provider._read_delta_table = MagicMock(return_value="a-dataframe")

        assert provider.read_entity(entity) == "a-dataframe"
