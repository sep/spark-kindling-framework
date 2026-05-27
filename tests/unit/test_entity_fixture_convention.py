"""
Unit tests for the tests/entities/ fixture auto-discovery convention.

Covers:
- CSV auto-discovered when present under tests/entities/
- Falls back to registered provider when no CSV is present
- Dotted entity ID maps to correct subfolder path
- Headers-only CSV raises a clear error
"""

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from kindling.data_entities import EntityMetadata
from kindling.entity_provider_csv import (
    FixtureCSVEntityProvider,
    _entity_id_to_fixture_path,
    resolve_fixture_csv_path,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_entity(entity_id: str) -> EntityMetadata:
    return EntityMetadata(
        entityid=entity_id,
        name=entity_id,
        partition_columns=[],
        merge_columns=[],
        tags={},
        schema=None,
    )


# ---------------------------------------------------------------------------
# Path mapping: dotted entity IDs -> subfolder paths
# ---------------------------------------------------------------------------


class TestEntityIdToFixturePath:
    """Test _entity_id_to_fixture_path path mapping helper."""

    def test_simple_entity_id_maps_to_root_csv(self, tmp_path):
        result = _entity_id_to_fixture_path("orders", tmp_path)
        assert result == tmp_path / "orders.csv"

    def test_dotted_entity_id_maps_to_subfolder(self, tmp_path):
        result = _entity_id_to_fixture_path("bronze.orders", tmp_path)
        assert result == tmp_path / "bronze" / "orders.csv"

    def test_triple_dotted_entity_id_maps_to_nested_subfolders(self, tmp_path):
        result = _entity_id_to_fixture_path("catalog.bronze.orders", tmp_path)
        assert result == tmp_path / "catalog" / "bronze" / "orders.csv"

    def test_entity_id_with_multiple_dots(self, tmp_path):
        result = _entity_id_to_fixture_path("a.b.c.d", tmp_path)
        assert result == tmp_path / "a" / "b" / "c" / "d.csv"


class TestResolveFixtureCsvPath:
    """Test resolve_fixture_csv_path returns path or None depending on file presence."""

    def test_returns_none_when_file_absent(self, tmp_path):
        # tests/entities/ directory doesn't even exist
        result = resolve_fixture_csv_path("bronze.orders", tmp_path)
        assert result is None

    def test_returns_none_when_directory_exists_but_file_absent(self, tmp_path):
        (tmp_path / "tests" / "entities" / "bronze").mkdir(parents=True)
        result = resolve_fixture_csv_path("bronze.orders", tmp_path)
        assert result is None

    def test_returns_path_when_csv_present_flat(self, tmp_path):
        fixture_dir = tmp_path / "tests" / "entities"
        fixture_dir.mkdir(parents=True)
        csv_file = fixture_dir / "orders.csv"
        csv_file.write_text("id,name\n1,foo\n")

        result = resolve_fixture_csv_path("orders", tmp_path)
        assert result == csv_file

    def test_returns_path_when_csv_present_nested(self, tmp_path):
        fixture_dir = tmp_path / "tests" / "entities" / "bronze"
        fixture_dir.mkdir(parents=True)
        csv_file = fixture_dir / "orders.csv"
        csv_file.write_text("id,name\n1,foo\n")

        result = resolve_fixture_csv_path("bronze.orders", tmp_path)
        assert result == csv_file

    def test_returns_absolute_path(self, tmp_path):
        fixture_dir = tmp_path / "tests" / "entities"
        fixture_dir.mkdir(parents=True)
        csv_file = fixture_dir / "orders.csv"
        csv_file.write_text("id\n1\n")

        result = resolve_fixture_csv_path("orders", tmp_path)
        assert result is not None
        assert result.is_absolute()


# ---------------------------------------------------------------------------
# FixtureCSVEntityProvider
# ---------------------------------------------------------------------------


class TestFixtureCSVEntityProvider:
    """Test FixtureCSVEntityProvider reads data and raises on empty files."""

    def _make_mock_spark(self, row_count: int):
        """Create a mock Spark session whose CSV reader returns row_count rows."""
        mock_df = MagicMock()
        mock_df.count.return_value = row_count

        mock_reader = MagicMock()
        mock_reader.load.return_value = mock_df
        mock_reader.option.return_value = mock_reader

        mock_spark = MagicMock()
        mock_spark.read.format.return_value = mock_reader
        return mock_spark, mock_df

    def test_read_entity_returns_dataframe_when_rows_present(self, tmp_path):
        csv_file = tmp_path / "orders.csv"
        csv_file.write_text("id,name\n1,foo\n")

        mock_spark, mock_df = self._make_mock_spark(row_count=1)
        provider = FixtureCSVEntityProvider(csv_file)
        entity = _make_entity("orders")

        with patch(
            "kindling.entity_provider_csv.get_or_create_spark_session", return_value=mock_spark
        ):
            result = provider.read_entity(entity)

        assert result is mock_df

    def test_read_entity_raises_clear_error_on_headers_only_csv(self, tmp_path):
        csv_file = tmp_path / "orders.csv"
        csv_file.write_text("id,name\n")  # headers only, no data rows

        mock_spark, _mock_df = self._make_mock_spark(row_count=0)
        provider = FixtureCSVEntityProvider(csv_file)
        entity = _make_entity("orders")

        with patch(
            "kindling.entity_provider_csv.get_or_create_spark_session", return_value=mock_spark
        ):
            with pytest.raises(ValueError, match="has no data rows"):
                provider.read_entity(entity)

    def test_read_entity_error_message_includes_entity_id(self, tmp_path):
        csv_file = tmp_path / "orders.csv"
        csv_file.write_text("id,name\n")

        mock_spark, _mock_df = self._make_mock_spark(row_count=0)
        provider = FixtureCSVEntityProvider(csv_file)
        entity = _make_entity("bronze.orders")

        with patch(
            "kindling.entity_provider_csv.get_or_create_spark_session", return_value=mock_spark
        ):
            with pytest.raises(ValueError, match="bronze.orders"):
                provider.read_entity(entity)

    def test_check_entity_exists_true_when_file_present(self, tmp_path):
        csv_file = tmp_path / "orders.csv"
        csv_file.write_text("id\n1\n")
        provider = FixtureCSVEntityProvider(csv_file)
        entity = _make_entity("orders")

        assert provider.check_entity_exists(entity) is True

    def test_check_entity_exists_false_when_file_absent(self, tmp_path):
        csv_file = tmp_path / "missing.csv"
        provider = FixtureCSVEntityProvider(csv_file)
        entity = _make_entity("missing")

        assert provider.check_entity_exists(entity) is False


# ---------------------------------------------------------------------------
# Integration with create_pipe_entity_reader (fixture auto-discovery)
# ---------------------------------------------------------------------------


class TestCreatePipeEntityReaderFixtureConvention:
    """
    Test that create_pipe_entity_reader uses the fixture CSV when present
    and falls back to the registered provider when the CSV is absent.
    """

    def _make_strategy(self):
        """Construct a SimpleReadPersistStrategy with mocked dependencies."""
        from kindling.simple_read_persist_strategy import SimpleReadPersistStrategy

        wms = MagicMock()
        ep = MagicMock()
        der = MagicMock()
        tp = MagicMock()
        lp = MagicMock()
        lp.get_logger.return_value = MagicMock()
        provider_registry = MagicMock()
        signal_provider = MagicMock()

        strategy = SimpleReadPersistStrategy.__new__(SimpleReadPersistStrategy)
        strategy.wms = wms
        strategy.ep = ep
        strategy.der = der
        strategy.tp = tp
        strategy.logger = lp.get_logger("test")
        strategy.provider_registry = provider_registry
        strategy._signal_provider = signal_provider
        strategy._listeners = {}
        return strategy

    def test_fixture_csv_used_when_present_and_local(self, tmp_path):
        """CSV auto-discovered when present under tests/entities/ in local mode."""
        fixture_dir = tmp_path / "tests" / "entities" / "bronze"
        fixture_dir.mkdir(parents=True)
        csv_file = fixture_dir / "orders.csv"
        csv_file.write_text("id,name\n1,foo\n")

        mock_df = MagicMock()
        mock_df.count.return_value = 1
        mock_reader = MagicMock()
        mock_reader.load.return_value = mock_df
        mock_reader.option.return_value = mock_reader
        mock_spark = MagicMock()
        mock_spark.read.format.return_value = mock_reader

        strategy = self._make_strategy()
        entity = _make_entity("bronze.orders")

        with (
            patch("kindling.simple_read_persist_strategy._is_local_execution", return_value=True),
            patch("kindling.simple_read_persist_strategy.os.getcwd", return_value=str(tmp_path)),
            patch(
                "kindling.entity_provider_csv.get_or_create_spark_session", return_value=mock_spark
            ),
        ):
            reader = strategy.create_pipe_entity_reader(MagicMock(pipeid="my_pipe"))
            result = reader(entity, usewm=False)

        assert result is mock_df
        # Registry should NOT have been consulted
        strategy.provider_registry.get_provider_for_entity.assert_not_called()

    def test_falls_back_to_registry_when_no_fixture_csv(self, tmp_path):
        """Registered provider used when no fixture CSV is present."""
        # No tests/entities/ directory created in tmp_path

        mock_df = MagicMock()
        mock_provider = MagicMock()
        mock_provider.read_entity.return_value = mock_df

        strategy = self._make_strategy()
        strategy.provider_registry.get_provider_for_entity.return_value = mock_provider
        entity = _make_entity("bronze.orders")

        with (
            patch("kindling.simple_read_persist_strategy._is_local_execution", return_value=True),
            patch("kindling.simple_read_persist_strategy.os.getcwd", return_value=str(tmp_path)),
        ):
            reader = strategy.create_pipe_entity_reader(MagicMock(pipeid="my_pipe"))
            result = reader(entity, usewm=False)

        assert result is mock_df
        strategy.provider_registry.get_provider_for_entity.assert_called_once_with(entity)

    def test_falls_back_to_registry_on_non_local_execution(self, tmp_path):
        """Convention does not apply outside local/standalone execution."""
        fixture_dir = tmp_path / "tests" / "entities"
        fixture_dir.mkdir(parents=True)
        (fixture_dir / "orders.csv").write_text("id,name\n1,foo\n")

        mock_df = MagicMock()
        mock_provider = MagicMock()
        mock_provider.read_entity.return_value = mock_df

        strategy = self._make_strategy()
        strategy.provider_registry.get_provider_for_entity.return_value = mock_provider
        entity = _make_entity("orders")

        with (
            patch("kindling.simple_read_persist_strategy._is_local_execution", return_value=False),
            patch("kindling.simple_read_persist_strategy.os.getcwd", return_value=str(tmp_path)),
        ):
            reader = strategy.create_pipe_entity_reader(MagicMock(pipeid="my_pipe"))
            result = reader(entity, usewm=False)

        assert result is mock_df
        strategy.provider_registry.get_provider_for_entity.assert_called_once_with(entity)

    def test_dotted_entity_id_maps_to_correct_subfolder(self, tmp_path):
        """bronze.orders -> tests/entities/bronze/orders.csv"""
        fixture_dir = tmp_path / "tests" / "entities" / "bronze"
        fixture_dir.mkdir(parents=True)
        csv_file = fixture_dir / "orders.csv"
        csv_file.write_text("id\n1\n")

        mock_df = MagicMock()
        mock_df.count.return_value = 1
        mock_reader = MagicMock()
        mock_reader.load.return_value = mock_df
        mock_reader.option.return_value = mock_reader
        mock_spark = MagicMock()
        mock_spark.read.format.return_value = mock_reader

        strategy = self._make_strategy()
        entity = _make_entity("bronze.orders")

        with (
            patch("kindling.simple_read_persist_strategy._is_local_execution", return_value=True),
            patch("kindling.simple_read_persist_strategy.os.getcwd", return_value=str(tmp_path)),
            patch(
                "kindling.entity_provider_csv.get_or_create_spark_session", return_value=mock_spark
            ),
        ):
            reader = strategy.create_pipe_entity_reader(MagicMock(pipeid="pipe"))
            result = reader(entity, usewm=False)

        assert result is mock_df
        # Verify load was called with the nested path
        mock_reader.load.assert_called_once_with(str(csv_file))

    def test_watermark_skipped_on_local_execution(self, tmp_path):
        """usewm=True is a no-op on standalone — wms must never be called."""
        strategy = self._make_strategy()
        mock_provider = MagicMock()
        mock_provider.read_entity.return_value = MagicMock()
        strategy.provider_registry.get_provider_for_entity.return_value = mock_provider
        entity = _make_entity("bronze.records")

        with (
            patch("kindling.simple_read_persist_strategy._is_local_execution", return_value=True),
            patch("kindling.simple_read_persist_strategy.os.getcwd", return_value=str(tmp_path)),
        ):
            reader = strategy.create_pipe_entity_reader(MagicMock(pipeid="my_pipe"))
            reader(entity, usewm=True)

        strategy.wms.read_current_entity_changes.assert_not_called()

    def test_headers_only_csv_raises_clear_error(self, tmp_path):
        """Headers-only fixture raises ValueError with a helpful message."""
        fixture_dir = tmp_path / "tests" / "entities"
        fixture_dir.mkdir(parents=True)
        csv_file = fixture_dir / "orders.csv"
        csv_file.write_text("id,name\n")  # no data rows

        mock_df = MagicMock()
        mock_df.count.return_value = 0
        mock_reader = MagicMock()
        mock_reader.load.return_value = mock_df
        mock_reader.option.return_value = mock_reader
        mock_spark = MagicMock()
        mock_spark.read.format.return_value = mock_reader

        strategy = self._make_strategy()
        entity = _make_entity("orders")

        with (
            patch("kindling.simple_read_persist_strategy._is_local_execution", return_value=True),
            patch("kindling.simple_read_persist_strategy.os.getcwd", return_value=str(tmp_path)),
            patch(
                "kindling.entity_provider_csv.get_or_create_spark_session", return_value=mock_spark
            ),
        ):
            reader = strategy.create_pipe_entity_reader(MagicMock(pipeid="pipe"))
            with pytest.raises(ValueError, match="has no data rows"):
                reader(entity, usewm=False)
