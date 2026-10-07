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


def test_file_ingestion_manager_get_entry_ids_returns_a_plain_list_snapshot():
    """get_entry_ids() must return a real list, not a dict.keys() view.

    A live view is exactly the antipattern that caused a production bug in
    the equivalent DataPipesRegistry.get_pipe_ids(): a signal handler
    mutating the returned collection in place (slice assignment) crashes on
    dict_keys, which doesn't support item assignment. The contract is a
    static snapshot at call time -- it must NOT reflect later registrations.
    """
    from kindling.file_ingestion import FileIngestionManager

    manager = FileIngestionManager(MagicMock())
    manager.register_entry(
        "entry1", name="E1", patterns=[".*\\.csv"], dest_entity_id="dest", tags={}
    )

    entry_ids = manager.get_entry_ids()

    assert isinstance(entry_ids, list)
    entry_ids[:] = entry_ids  # must support slice assignment

    manager.register_entry(
        "entry2", name="E2", patterns=[".*\\.json"], dest_entity_id="dest", tags={}
    )

    assert entry_ids == ["entry1"]


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


# ── enrich_file_dataframe (standalone helper) ────────────────────────────────


def test_enrich_file_dataframe_isolated_from_spark_read_path():
    """No SparkSession, no .read, no processor instance -- just a DataFrame-like
    object and plain dicts, proving the helper is reusable from a future
    foreachBatch callback."""
    from kindling.file_ingestion import enrich_file_dataframe

    mock_df = MagicMock()
    mock_df.withColumn.return_value = mock_df

    with patch("kindling.file_ingestion.lit", side_effect=lambda v: f"LIT({v})"):
        with patch("kindling.file_ingestion.current_timestamp", return_value="NOW"):
            result = enrich_file_dataframe(
                mock_df,
                named_groups={"filetype": "csv"},
                static_values={"region": "eu"},
            )

    assert result is mock_df
    assert _captured_columns(mock_df) == ["filetype", "region", "ingestion_timestamp"]


def test_enrich_file_dataframe_no_static_values_skips_that_step():
    from kindling.file_ingestion import enrich_file_dataframe

    mock_df = MagicMock()
    mock_df.withColumn.return_value = mock_df

    with patch("kindling.file_ingestion.lit", side_effect=lambda v: f"LIT({v})"):
        with patch("kindling.file_ingestion.current_timestamp", return_value="NOW"):
            enrich_file_dataframe(mock_df, named_groups={"filetype": "csv"}, static_values=None)

    assert _captured_columns(mock_df) == ["filetype", "ingestion_timestamp"]


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


# ── FileIngestionEntries.entry() discovery-field validation ─────────────────
#
# Unlike the tests above (which call FileIngestionManager.register_entry
# directly), these exercise FileIngestionEntries.entry() itself -- the
# classmethod that actually validates the "discovery" field and its
# source_glob dependency before handing off to the registry.


def _entries_kwargs(**overrides):
    kwargs = dict(
        entry_id="e1",
        name="test entry",
        patterns=[r".*\.csv"],
        dest_entity_id="my_entity",
        tags={},
        filetype="csv",
    )
    kwargs.update(overrides)
    return kwargs


def test_entries_entry_rejects_invalid_discovery_value(monkeypatch):
    from kindling.file_ingestion import FileIngestionEntries

    monkeypatch.setattr(FileIngestionEntries, "deregistry", MagicMock())

    with pytest.raises(ValueError, match="invalid discovery"):
        FileIngestionEntries.entry(**_entries_kwargs(discovery="streaming"))


def test_entries_entry_autoloader_requires_source_glob(monkeypatch):
    from kindling.file_ingestion import FileIngestionEntries

    monkeypatch.setattr(FileIngestionEntries, "deregistry", MagicMock())

    with pytest.raises(ValueError, match="requires an explicit source_glob"):
        FileIngestionEntries.entry(**_entries_kwargs(discovery="autoloader"))


def test_entries_entry_autoloader_with_source_glob_registers_successfully(monkeypatch):
    from kindling.file_ingestion import FileIngestionEntries

    mock_registry = MagicMock()
    monkeypatch.setattr(FileIngestionEntries, "deregistry", mock_registry)

    FileIngestionEntries.entry(
        **_entries_kwargs(
            discovery="autoloader",
            source_glob="*.csv",
            schema_evolution_mode="addNewColumns",
        )
    )

    mock_registry.register_entry.assert_called_once()
    args, kwargs = mock_registry.register_entry.call_args
    assert args[0] == "e1"
    assert kwargs["dest_entity_id"] == "my_entity"
    assert kwargs["discovery"] == "autoloader"
    assert kwargs["source_glob"] == "*.csv"
    assert kwargs["schema_evolution_mode"] == "addNewColumns"
    assert "entry_id" not in kwargs


def test_entries_entry_omitting_discovery_defaults_to_batch_and_needs_no_source_glob(monkeypatch):
    """Existing (pre-autoloader) callers that never pass discovery/source_glob
    must keep registering exactly as they did before -- regression-safe
    default."""
    from kindling.file_ingestion import FileIngestionEntries

    mock_registry = MagicMock()
    monkeypatch.setattr(FileIngestionEntries, "deregistry", mock_registry)

    FileIngestionEntries.entry(**_entries_kwargs())

    args, kwargs = mock_registry.register_entry.call_args
    assert args[0] == "e1"
    assert kwargs["discovery"] == "batch"
    assert kwargs["source_glob"] is None
    assert kwargs["schema_evolution_mode"] is None


# ── schema_evolution_mode ─────────────────────────────────────────────────────


def test_metadata_schema_evolution_mode_defaults_to_none():
    from kindling.file_ingestion import FileIngestionMetadata

    m = FileIngestionMetadata(
        entry_id="e1",
        name="test",
        patterns=[".*\\.csv"],
        dest_entity_id="my_entity",
        tags={},
    )
    assert m.schema_evolution_mode is None


def test_entry_decorator_accepts_schema_evolution_mode_without_error():
    from kindling.file_ingestion import FileIngestionManager

    mgr = FileIngestionManager.__new__(FileIngestionManager)
    mgr.logger = MagicMock()
    mgr.registry = {}

    mgr.register_entry(
        "e1",
        name="my entry",
        patterns=[".*\\.csv"],
        dest_entity_id="my_entity",
        tags={},
        discovery="autoloader",
        source_glob="*.csv",
        schema_evolution_mode="addNewColumns",
    )

    entry = mgr.get_entry_definition("e1")
    assert entry.schema_evolution_mode == "addNewColumns"


def test_entry_decorator_omitting_schema_evolution_mode_stores_none():
    from kindling.file_ingestion import FileIngestionManager

    mgr = FileIngestionManager.__new__(FileIngestionManager)
    mgr.logger = MagicMock()
    mgr.registry = {}

    mgr.register_entry(
        "e2",
        name="another entry",
        patterns=[".*\\.csv"],
        dest_entity_id="other_entity",
        tags={},
        discovery="autoloader",
        source_glob="*.csv",
    )

    entry = mgr.get_entry_definition("e2")
    assert entry.schema_evolution_mode is None


def test_process_path_disabled_tracing_emits_no_spans():
    """kindling.telemetry.tracing.enabled=false suppresses the process span."""
    from kindling.file_ingestion import ParallelizingFileIngestionProcessor
    from kindling.trace_ops import TracingGates

    proc = object.__new__(ParallelizingFileIngestionProcessor)
    proc.logger = MagicMock()
    proc.tp = MagicMock()
    proc._trace_gates = TracingGates(False, "standard")
    proc.emit = MagicMock()
    proc.config = MagicMock()
    proc.config.get.return_value = 3
    proc.env = MagicMock()
    proc.env.list.return_value = []
    proc.fir = MagicMock()
    proc.fir.get_entry_ids.return_value = []

    proc.process_path("/data")

    proc.tp.span.assert_not_called()


# ── read options: filetype and infer_schema are honored ─────────────────────


def _make_read_processor(entry):
    """Processor whose spark.read chain returns a chaining mock DataFrame."""
    proc = _make_processor({entry.entry_id: entry})
    mock_df = MagicMock()
    mock_df.withColumn.return_value = mock_df
    mock_spark = MagicMock()
    reader = mock_spark.read.format.return_value
    reader.option.return_value.option.return_value.load.return_value = mock_df
    proc.spark = mock_spark
    return proc, mock_spark


def _read_options(mock_spark):
    """{option: value} passed to the reader in _build_df_plan."""
    first = mock_spark.read.format.return_value.option
    second = first.return_value.option
    return dict([first.call_args.args, second.call_args.args])


def _plan(proc, fn):
    with patch("kindling.file_ingestion.lit", side_effect=lambda v: f"LIT({v})"):
        with patch("kindling.file_ingestion.current_timestamp", return_value="NOW"):
            return proc._build_df_plan(fn, "/data")


def _entry(**overrides):
    from kindling.file_ingestion import FileIngestionMetadata

    kwargs = dict(
        entry_id="e1",
        name="test entry",
        patterns=[r"orders_\d+\.parquet"],
        dest_entity_id="target_entity",
        tags={},
    )
    kwargs.update(overrides)
    return FileIngestionMetadata(**kwargs)


def test_build_df_plan_reads_with_entry_filetype_when_pattern_has_no_filetype_group():
    proc, spark = _make_read_processor(_entry(filetype="parquet"))

    assert _plan(proc, "orders_1.parquet") is not None
    spark.read.format.assert_called_once_with("parquet")


def test_build_df_plan_filetype_group_overrides_entry_filetype_per_file():
    entry = _entry(patterns=[r"orders_\d+\.(?P<filetype>json|csv)"], filetype="parquet")
    proc, spark = _make_read_processor(entry)

    _plan(proc, "orders_1.json")
    spark.read.format.assert_called_once_with("json")


def test_build_df_plan_defaults_to_csv_without_filetype_argument_or_group():
    from kindling.file_ingestion import FileIngestionMetadata

    entry = FileIngestionMetadata(
        entry_id="e1", name="n", patterns=[r"a\.csv"], dest_entity_id="t", tags={}
    )
    proc, spark = _make_read_processor(entry)

    _plan(proc, "a.csv")
    spark.read.format.assert_called_once_with("csv")


def test_build_df_plan_infer_schema_true_enables_spark_inference():
    proc, spark = _make_read_processor(_entry(infer_schema=True))

    _plan(proc, "orders_1.parquet")
    assert _read_options(spark) == {"header": "true", "inferSchema": "true"}


def test_build_df_plan_infer_schema_defaults_off():
    proc, spark = _make_read_processor(_entry())

    _plan(proc, "orders_1.parquet")
    assert _read_options(spark) == {"header": "true", "inferSchema": "false"}


def test_entries_entry_infer_schema_defaults_false_and_explicit_value_passes_through(
    monkeypatch,
):
    from kindling.file_ingestion import FileIngestionEntries

    mock_registry = MagicMock()
    monkeypatch.setattr(FileIngestionEntries, "deregistry", mock_registry)

    FileIngestionEntries.entry(**_entries_kwargs())
    assert mock_registry.register_entry.call_args.kwargs["infer_schema"] is False

    FileIngestionEntries.entry(**_entries_kwargs(infer_schema=True))
    assert mock_registry.register_entry.call_args.kwargs["infer_schema"] is True


# ── patterns: every pattern is tried in order ───────────────────────────────


def test_build_df_plan_matches_a_later_pattern_when_earlier_ones_miss():
    entry = _entry(patterns=[r"sales_(?P<region>\w+)\.csv", r"orders_(?P<day>\d+)\.csv"])
    proc, _ = _make_read_processor(entry)

    result = _plan(proc, "orders_7.csv")

    assert result is not None
    dest_entity_id, df, file_info = result
    assert file_info["filename"] == "orders_7.csv"
    assert "day" in _captured_columns(df)


def test_build_df_plan_first_matching_pattern_wins():
    entry = _entry(
        patterns=[r"(?P<first>orders)_\d+\.csv", r"(?P<second>orders_\d+)\.csv"],
        dest_entity_id="t_{first}",
    )
    proc, _ = _make_read_processor(entry)

    dest_entity_id, df, _ = _plan(proc, "orders_7.csv")

    assert dest_entity_id == "t_orders"
    assert "second" not in _captured_columns(df)


def test_match_file_patterns_returns_none_when_nothing_matches():
    from kindling.file_ingestion import match_file_patterns

    assert match_file_patterns([r"a\.csv", r"b\.csv"], "c.csv") is None
    assert match_file_patterns([r"a\.csv", r"b\.csv"], "b.csv").group(0) == "b.csv"


@pytest.mark.parametrize(
    "patterns, message",
    [
        (r"orders_\d+\.csv", "non-empty list"),
        ([], "non-empty list"),
        ([r"ok\.csv", r"bad(\.csv"], "invalid pattern"),
    ],
)
def test_entries_entry_rejects_unusable_patterns(monkeypatch, patterns, message):
    from kindling.file_ingestion import FileIngestionEntries

    monkeypatch.setattr(FileIngestionEntries, "deregistry", MagicMock())

    with pytest.raises(ValueError, match=message):
        FileIngestionEntries.entry(**_entries_kwargs(patterns=patterns))


def test_autoloader_batch_matches_any_entry_pattern():
    entry = _entry(
        patterns=[r"sales_\w+\.csv", r"orders_(?P<day>\d+)\.csv"],
        discovery="autoloader",
        source_glob="*.csv",
    )
    proc = _make_processor({entry.entry_id: entry})
    proc.emit = MagicMock()
    proc._write_table_group = MagicMock()

    batch_df = MagicMock()
    batch_df.select.return_value.distinct.return_value.collect.return_value = [
        {"file_path": "/landing/orders_3.csv"},
        {"file_path": "/landing/notes.csv"},
    ]
    file_df = MagicMock()
    file_df.withColumn.return_value = file_df
    batch_df.filter.return_value = file_df

    with patch("kindling.file_ingestion.col", MagicMock()):
        with patch("kindling.file_ingestion.lit", side_effect=lambda v: f"LIT({v})"):
            with patch("kindling.file_ingestion.current_timestamp", return_value="NOW"):
                result = proc._process_autoloader_batch(entry, batch_df, "0", None, None)

    assert result == (1, 0, 1)
    dest_entity_id, df_list, _ = proc._write_table_group.call_args.args
    assert dest_entity_id == "target_entity"
    assert [info["filename"] for _, info in df_list] == ["orders_3.csv"]
    assert "day" in _captured_columns(file_df)


# ── persistence: destination entity's own provider ──────────────────────────


def _make_writer(entity, provider_registry):
    from kindling.file_ingestion import ParallelizingFileIngestionProcessor

    proc = object.__new__(ParallelizingFileIngestionProcessor)
    proc.logger = MagicMock()
    proc.emit = MagicMock()
    proc.env = MagicMock()
    proc.der = MagicMock()
    proc.der.get_entity_definition.return_value = entity
    proc.provider_registry = provider_registry
    return proc


def _registry_with(**instances):
    """A real EntityProviderRegistry with pre-built provider instances."""
    from kindling.entity_provider_registry import EntityProviderRegistry

    registry = EntityProviderRegistry(MagicMock())
    registry._provider_instances.update(instances)
    return registry


def test_write_table_group_appends_through_entity_provider_type():
    from types import SimpleNamespace

    csv_provider, delta_provider = MagicMock(), MagicMock()
    entity = SimpleNamespace(entityid="bronze.raw", tags={"provider_type": "csv"})
    proc = _make_writer(entity, _registry_with(csv=csv_provider, delta=delta_provider))
    df = MagicMock()

    proc._write_table_group("bronze.raw", [(df, {"source_path": "/p/a.csv", "filename": "a.csv"})])

    csv_provider.append_to_entity.assert_called_once_with(df, entity)
    delta_provider.append_to_entity.assert_not_called()
    csv_provider.merge_to_entity.assert_not_called()
    csv_provider.write_to_entity.assert_not_called()


def test_write_table_group_defaults_to_delta_provider_without_provider_type():
    from types import SimpleNamespace

    csv_provider, delta_provider = MagicMock(), MagicMock()
    entity = SimpleNamespace(entityid="bronze.raw", tags={})
    proc = _make_writer(entity, _registry_with(csv=csv_provider, delta=delta_provider))
    df = MagicMock()

    proc._write_table_group("bronze.raw", [(df, {"source_path": "/p/a.csv", "filename": "a.csv"})])

    delta_provider.append_to_entity.assert_called_once_with(df, entity)
    csv_provider.append_to_entity.assert_not_called()


def test_write_table_group_rejects_provider_without_append():
    from types import SimpleNamespace

    read_only = MagicMock(spec=["read_entity", "check_entity_exists"])
    entity = SimpleNamespace(entityid="ref.lookup", tags={"provider_type": "sql"})
    proc = _make_writer(entity, _registry_with(sql=read_only))

    with pytest.raises(ValueError, match="does not support append"):
        proc._write_table_group("ref.lookup", [(MagicMock(), {"filename": "a.csv"})])

    proc.emit.assert_not_called()
