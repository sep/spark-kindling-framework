"""Unit tests for WatermarkManager._legacy_version_read's provider
capability check.

Regression coverage for: a provider using the newer split-interface design
(BaseEntityProvider et al., e.g. MemoryEntityProvider) is neither
IncrementalReadableEntityProvider nor the legacy EntityProvider interface
that defined get_entity_version/read_entity_since_version -- it has no
notion of a table "version" at all. _read_changes routed any non-
IncrementalReadableEntityProvider straight into _legacy_version_read, which
unconditionally called provider.get_entity_version(entity), crashing with
AttributeError for a memory-backed entity with watermarking enabled.
"""

from unittest.mock import MagicMock

from kindling.watermarking import WatermarkManager


def _make_manager() -> WatermarkManager:
    manager = WatermarkManager.__new__(WatermarkManager)
    manager.logger = MagicMock()
    return manager


def test_legacy_version_read_falls_back_to_full_read_without_version_api():
    provider = MagicMock(spec=["read_entity", "check_entity_exists"])
    provider.read_entity.return_value = "the-dataframe"
    entity = MagicMock(entityid="memory.staging")
    manager = _make_manager()

    df, cursor = manager._legacy_version_read(provider, entity, cursor=None)

    assert df == "the-dataframe"
    assert cursor is None
    provider.read_entity.assert_called_once_with(entity)
    manager.logger.warning.assert_called_once()


def test_legacy_version_read_uses_version_api_when_present():
    """A provider that does implement get_entity_version must still go
    through the real version-comparison path (no data at the current
    watermark -> no-op), not the new full-read fallback."""
    provider = MagicMock()
    provider.get_entity_version.return_value = 3
    entity = MagicMock(entityid="delta.bronze", merge_columns=["id"])
    manager = _make_manager()

    df, cursor = manager._legacy_version_read(provider, entity, cursor="3")

    assert (df, cursor) == (None, None)
    provider.get_entity_version.assert_called_once_with(entity)
    provider.read_entity.assert_not_called()
