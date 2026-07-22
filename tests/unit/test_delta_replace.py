"""
Unit tests for DeltaEntityProvider.replace_entity — the derived-dataset
materialization: an atomic overwrite swap of the full table, or of only
the batch's slices when ``derived.replace_keys`` is declared (Delta
``replaceWhere`` computed from the batch's distinct key values).
"""

from datetime import date, datetime
from unittest.mock import MagicMock, patch

import pytest
from kindling.data_entities import EntityMetadata, EntityNameMapper, EntityPathLocator
from kindling.entity_provider import ReplaceWritableEntityProvider, is_replace_writable
from kindling.entity_provider_delta import DeltaEntityProvider, ReadOnlyEntityError
from kindling.signaling import SignalProvider
from kindling.spark_config import ConfigService
from kindling.spark_log_provider import PythonLoggerProvider


class TestDeltaReplaceEntity:
    @pytest.fixture(autouse=True)
    def mock_spark_session(self, monkeypatch):
        spark = MagicMock()
        monkeypatch.setattr(
            "kindling.entity_provider_delta.get_or_create_spark_session", lambda: spark
        )
        return spark

    @pytest.fixture
    def provider(self):
        config = MagicMock(spec=ConfigService)
        config.get.side_effect = lambda key, default=None: (
            "catalog" if key == "kindling.delta.access_mode" else default
        )
        entity_name_mapper = MagicMock(spec=EntityNameMapper)
        path_locator = MagicMock(spec=EntityPathLocator)
        logger_provider = MagicMock(spec=PythonLoggerProvider)
        logger_provider.get_logger.return_value = MagicMock()
        return DeltaEntityProvider(
            config=config,
            entity_name_mapper=entity_name_mapper,
            path_locator=path_locator,
            tp=logger_provider,
            signal_provider=MagicMock(spec=SignalProvider),
        )

    def _make_entity(self, tags=None):
        return EntityMetadata(
            entityid="gold.summary",
            name="summary",
            tags={"dataset.kind": "derived", **(tags or {})},
            merge_columns=None,
            schema=None,
        )

    def _writer_chain(self):
        """A df.write mock whose format/mode/option chain returns itself."""
        writer = MagicMock()
        writer.format.return_value = writer
        writer.mode.return_value = writer
        writer.option.return_value = writer
        writer.partitionBy.return_value = writer
        return writer

    def test_provider_is_replace_writable(self, provider):
        assert isinstance(provider, ReplaceWritableEntityProvider)
        assert is_replace_writable(provider)

    def test_read_only_entity_raises(self, provider):
        entity = self._make_entity(tags={"read_only": "true"})
        with pytest.raises(ReadOnlyEntityError):
            provider.replace_entity(MagicMock(), entity)

    def test_full_replace_overwrites_with_overwrite_schema(self, provider):
        entity = self._make_entity()
        df = MagicMock()
        df.write = self._writer_chain()
        table_ref = MagicMock(access_mode="catalog", table_name="gold_summary")

        with (
            patch.object(provider, "_get_table_reference", return_value=table_ref),
            patch.object(provider, "_check_table_exists", return_value=True),
            patch.object(provider, "_ensure_configured_table_schema_exists"),
            patch.object(provider, "_resolve_catalog_table_location", return_value=None),
        ):
            provider.replace_entity(df, entity)

        df.write.mode.assert_called_once_with("overwrite")
        df.write.option.assert_called_once_with("overwriteSchema", "true")
        df.write.saveAsTable.assert_called_once_with("gold_summary")

    def test_keyed_replace_uses_replace_where_not_overwrite_schema(self, provider):
        entity = self._make_entity(tags={"derived.replace_keys": "run_id"})
        df = MagicMock()
        df.write = self._writer_chain()
        df.select.return_value.distinct.return_value.collect.return_value = [
            {"run_id": "flight-1"},
            {"run_id": "flight-2"},
        ]
        table_ref = MagicMock(access_mode="catalog", table_name="gold_summary")

        with (
            patch.object(provider, "_get_table_reference", return_value=table_ref),
            patch.object(provider, "_check_table_exists", return_value=True),
            patch.object(provider, "_ensure_configured_table_schema_exists"),
            patch.object(provider, "_resolve_catalog_table_location", return_value=None),
        ):
            provider.replace_entity(df, entity)

        option_calls = {call.args[0]: call.args[1] for call in df.write.option.call_args_list}
        assert option_calls["replaceWhere"] == (
            "(`run_id` = 'flight-1') OR (`run_id` = 'flight-2')"
        )
        assert option_calls.get("mergeSchema") == "true"
        assert "overwriteSchema" not in option_calls
        df.write.mode.assert_called_once_with("overwrite")

    def test_keyed_replace_empty_batch_is_noop(self, provider):
        entity = self._make_entity(tags={"derived.replace_keys": "run_id"})
        df = MagicMock()
        df.write = self._writer_chain()
        df.select.return_value.distinct.return_value.collect.return_value = []
        table_ref = MagicMock(access_mode="catalog", table_name="gold_summary")

        with (
            patch.object(provider, "_get_table_reference", return_value=table_ref),
            patch.object(provider, "_check_table_exists", return_value=True),
        ):
            provider.replace_entity(df, entity)

        df.write.mode.assert_not_called()
        df.write.saveAsTable.assert_not_called()

    def test_missing_table_takes_create_path(self, provider):
        entity = self._make_entity()
        df = MagicMock()
        table_ref = MagicMock(access_mode="catalog", table_name="gold_summary")

        with (
            patch.object(provider, "_get_table_reference", return_value=table_ref),
            patch.object(provider, "_check_table_exists", return_value=False),
            patch.object(provider, "_ensure_destination_for_write") as ensure,
            patch.object(provider, "_write_to_delta_table") as write,
        ):
            provider.replace_entity(df, entity)

        ensure.assert_called_once()
        write.assert_called_once_with(df, entity, table_ref)


class TestReplacePredicate:
    def _predicate(self, rows, keys):
        df = MagicMock()
        df.select.return_value.distinct.return_value.collect.return_value = rows
        provider = MagicMock()
        provider.logger = MagicMock()
        provider._sql_literal = DeltaEntityProvider._sql_literal
        return DeltaEntityProvider._build_replace_predicate(provider, df, keys)

    def test_multi_key_slices_and_of_keys_or_of_slices(self):
        predicate = self._predicate(
            [{"run_id": "r1", "site": 7}, {"run_id": "r2", "site": None}],
            ["run_id", "site"],
        )
        assert predicate == (
            "(`run_id` = 'r1' AND `site` = 7) OR (`run_id` = 'r2' AND `site` IS NULL)"
        )

    def test_string_values_escape_quotes(self):
        predicate = self._predicate([{"run_id": "o'brien"}], ["run_id"])
        assert predicate == "(`run_id` = 'o''brien')"

    def test_temporal_and_bool_literals(self):
        predicate = self._predicate(
            [{"day": date(2026, 7, 1), "at": datetime(2026, 7, 1, 12, 30), "ok": True}],
            ["day", "at", "ok"],
        )
        assert predicate == (
            "(`day` = DATE '2026-07-01' AND `at` = TIMESTAMP '2026-07-01 12:30:00' "
            "AND `ok` = true)"
        )


class TestInsertOnlyStrategySelection:
    def test_registry_has_insert_only(self):
        from kindling.entity_provider_delta import DeltaMergeStrategies

        strategy = DeltaMergeStrategies.get("insert_only")
        builder = MagicMock()
        delta_table = MagicMock()
        delta_table.alias.return_value.merge.return_value = builder
        builder.withSchemaEvolution.return_value = builder
        builder.whenNotMatchedInsertAll.return_value = builder

        entity = EntityMetadata(
            entityid="silver.events",
            name="events",
            merge_columns=["event_id"],
            tags={"write.mode": "insert"},
            schema=None,
        )
        strategy.apply(delta_table, MagicMock(), entity, "old.`event_id` = new.`event_id`")

        builder.whenNotMatchedInsertAll.assert_called_once()
        builder.whenMatchedUpdateAll.assert_not_called()
        builder.execute.assert_called_once()


def test_sql_literal_decimal_is_unquoted():
    from decimal import Decimal

    assert DeltaEntityProvider._sql_literal(Decimal("12.50")) == "12.50"
