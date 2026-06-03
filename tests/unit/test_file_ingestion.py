"""Unit tests for file_ingestion module."""

from dataclasses import fields
from unittest.mock import MagicMock, patch

import pytest

# ── FileIngestionMetadata ────────────────────────────────────────────────────


def test_metadata_static_values_defaults_to_none():
    from kindling.file_ingestion import FileIngestionMetadata

    m = FileIngestionMetadata(
        entry_id="e1",
        name="test",
        patterns=[".*\\.csv"],
        dest_entity_id="my_entity",
        tags={},
    )
    assert m.static_values is None


def test_metadata_accepts_static_values_dict():
    from kindling.file_ingestion import FileIngestionMetadata

    m = FileIngestionMetadata(
        entry_id="e1",
        name="test",
        patterns=[".*\\.csv"],
        dest_entity_id="my_entity",
        tags={},
        static_values={"source": "system_a", "region": "us-east-1"},
    )
    assert m.static_values == {"source": "system_a", "region": "us-east-1"}


def test_metadata_field_is_optional():
    """static_values must have a default so it is not in the required-fields set."""
    from kindling.file_ingestion import FileIngestionMetadata

    optional_field_names = {
        f.name for f in fields(FileIngestionMetadata) if f.default is not f.default_factory
    }
    assert "static_values" in optional_field_names


# ── _build_df_plan static_values injection ───────────────────────────────────


def _make_processor(registry_entries):
    """Build a ParallelizingFileIngestionProcessor with a mocked DI environment."""
    from kindling.file_ingestion import ParallelizingFileIngestionProcessor

    mock_fir = MagicMock()
    mock_fir.get_entry_ids.return_value = list(registry_entries.keys())
    mock_fir.get_entry_definition.side_effect = registry_entries.get

    proc = object.__new__(ParallelizingFileIngestionProcessor)
    proc.fir = mock_fir
    proc.logger = MagicMock()
    return proc


def _make_entry(patterns, dest_entity_id="target_entity", static_values=None):
    from kindling.file_ingestion import FileIngestionMetadata

    return FileIngestionMetadata(
        entry_id="e1",
        name="test entry",
        patterns=patterns,
        dest_entity_id=dest_entity_id,
        tags={},
        static_values=static_values,
    )


def _captured_columns(df_mock):
    """Return the column names added via withColumn on the mock DataFrame."""
    return [call.args[0] for call in df_mock.withColumn.call_args_list]


def test_build_df_plan_applies_static_values_as_literal_columns():
    entry = _make_entry(
        patterns=[r"(?P<filetype>csv)_data\.csv"],
        static_values={"source_system": "erp", "load_type": "full"},
    )
    proc = _make_processor({"e1": entry})

    mock_df = MagicMock()
    mock_df.withColumn.return_value = mock_df  # chaining

    mock_spark = MagicMock()
    mock_spark.read.format.return_value.option.return_value.option.return_value.load.return_value = (
        mock_df
    )
    proc.spark = mock_spark

    with patch("kindling.file_ingestion.lit", side_effect=lambda v: f"LIT({v})"):
        with patch("kindling.file_ingestion.current_timestamp", return_value="NOW"):
            result = proc._build_df_plan("csv_data.csv", "/data")

    assert result is not None
    added_cols = _captured_columns(mock_df)
    assert "source_system" in added_cols
    assert "load_type" in added_cols


def test_build_df_plan_no_static_values_does_not_add_extra_columns():
    entry = _make_entry(patterns=[r"(?P<filetype>csv)_data\.csv"])
    proc = _make_processor({"e1": entry})

    mock_df = MagicMock()
    mock_df.withColumn.return_value = mock_df
    mock_spark = MagicMock()
    mock_spark.read.format.return_value.option.return_value.option.return_value.load.return_value = (
        mock_df
    )
    proc.spark = mock_spark

    with patch("kindling.file_ingestion.lit", side_effect=lambda v: f"LIT({v})"):
        with patch("kindling.file_ingestion.current_timestamp", return_value="NOW"):
            result = proc._build_df_plan("csv_data.csv", "/data")

    assert result is not None
    added_cols = _captured_columns(mock_df)
    # Only the named group (filetype) and ingestion_timestamp should be present
    assert "source_system" not in added_cols
    assert "load_type" not in added_cols
    assert "ingestion_timestamp" in added_cols


def test_build_df_plan_static_values_added_after_named_groups():
    """static_values columns must come after regex named-group columns."""
    entry = _make_entry(
        patterns=[r"(?P<filetype>csv)_data\.csv"],
        static_values={"region": "eu"},
    )
    proc = _make_processor({"e1": entry})

    mock_df = MagicMock()
    mock_df.withColumn.return_value = mock_df
    mock_spark = MagicMock()
    mock_spark.read.format.return_value.option.return_value.option.return_value.load.return_value = (
        mock_df
    )
    proc.spark = mock_spark

    with patch("kindling.file_ingestion.lit", side_effect=lambda v: f"LIT({v})"):
        with patch("kindling.file_ingestion.current_timestamp", return_value="NOW"):
            proc._build_df_plan("csv_data.csv", "/data")

    added_cols = _captured_columns(mock_df)
    filetype_idx = added_cols.index("filetype")
    region_idx = added_cols.index("region")
    assert region_idx > filetype_idx, "static_values should be added after named groups"


def test_build_df_plan_no_match_returns_none():
    entry = _make_entry(patterns=[r"report_\d{8}\.csv"])
    proc = _make_processor({"e1": entry})
    proc.spark = MagicMock()

    result = proc._build_df_plan("completely_different.parquet", "/data")
    assert result is None


# ── FileIngestionEntries.entry() ─────────────────────────────────────────────


def test_entry_decorator_accepts_static_values_without_error():
    from kindling.file_ingestion import FileIngestionManager, FileIngestionMetadata

    mgr = FileIngestionManager.__new__(FileIngestionManager)
    mgr.logger = MagicMock()
    mgr.registry = {}

    mgr.register_entry(
        "e1",
        name="my entry",
        patterns=[".*\\.csv"],
        dest_entity_id="my_entity",
        tags={},
        filetype="csv",
        static_values={"env": "prod"},
    )

    entry = mgr.get_entry_definition("e1")
    assert entry.static_values == {"env": "prod"}


def test_entry_decorator_omitting_static_values_stores_none():
    from kindling.file_ingestion import FileIngestionManager

    mgr = FileIngestionManager.__new__(FileIngestionManager)
    mgr.logger = MagicMock()
    mgr.registry = {}

    mgr.register_entry(
        "e2",
        name="another entry",
        patterns=[".*\\.parquet"],
        dest_entity_id="other_entity",
        tags={},
        filetype="parquet",
    )

    entry = mgr.get_entry_definition("e2")
    assert entry.static_values is None
