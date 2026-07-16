"""
Unit tests for DeltaEntityProvider.merge_as_stream.

merge_as_stream wraps the batch merge in a foreachBatch streaming sink:
each micro-batch is handed to merge_to_entity, so SCD1/SCD2 semantics are
identical to the batch path. These tests verify the writer wiring, the
guards (read_only, missing merge_columns), and option pass-through.
"""

from unittest.mock import MagicMock

import pytest
from kindling.data_entities import EntityMetadata, EntityNameMapper, EntityPathLocator
from kindling.entity_provider import StreamMergeableEntityProvider, is_stream_mergeable
from kindling.entity_provider_delta import DeltaEntityProvider, ReadOnlyEntityError
from kindling.signaling import SignalProvider
from kindling.spark_config import ConfigService
from kindling.spark_log_provider import PythonLoggerProvider


class TestDeltaMergeAsStream:
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

    def _make_entity(self, merge_columns=None, tags=None):
        return EntityMetadata(
            entityid="sales.orders",
            name="orders",
            merge_columns=["order_id"] if merge_columns is None else merge_columns,
            tags=tags or {},
            schema=None,
        )

    def test_provider_is_stream_mergeable(self, provider):
        assert isinstance(provider, StreamMergeableEntityProvider)
        assert is_stream_mergeable(provider)

    def test_read_only_entity_raises(self, provider):
        entity = self._make_entity(tags={"read_only": "true"})
        with pytest.raises(ReadOnlyEntityError):
            provider.merge_as_stream(MagicMock(), entity, "/chk")

    def test_missing_merge_columns_raises(self, provider):
        entity = self._make_entity(merge_columns=[])
        with pytest.raises(ValueError, match="merge_columns"):
            provider.merge_as_stream(MagicMock(), entity, "/chk")

    def test_declared_change_feed_without_sequence_by_raises(self, provider):
        entity = self._make_entity(tags={"scd.type": "2", "scd.source_kind": "change_feed"})
        with pytest.raises(ValueError, match="scd.sequence_by"):
            provider.merge_as_stream(MagicMock(), entity, "/chk")

    def test_declared_change_feed_with_sequence_by_starts(self, provider):
        entity = self._make_entity(
            tags={
                "scd.type": "2",
                "scd.source_kind": "change_feed",
                "scd.sequence_by": "updated_at",
            }
        )
        df = MagicMock()
        query = (
            df.writeStream.outputMode.return_value.option.return_value.foreachBatch.return_value.start.return_value
        )
        assert provider.merge_as_stream(df, entity, "/chk") is query

    def test_implicit_change_feed_default_is_not_rejected(self, provider):
        # Vanilla SCD2 (no source_kind tag) keeps arrival-time ordering with
        # the one-row-per-key-per-batch contract on the source, as in batch.
        entity = self._make_entity(tags={"scd.type": "2"})
        df = MagicMock()
        query = (
            df.writeStream.outputMode.return_value.option.return_value.foreachBatch.return_value.start.return_value
        )
        assert provider.merge_as_stream(df, entity, "/chk") is query

    def test_starts_foreach_batch_query_with_checkpoint(self, provider):
        entity = self._make_entity()
        df = MagicMock()
        writer = df.writeStream.outputMode.return_value.option.return_value
        query = writer.foreachBatch.return_value.start.return_value

        result = provider.merge_as_stream(df, entity, "/chk/orders")

        assert result is query
        df.writeStream.outputMode.assert_called_once_with("update")
        df.writeStream.outputMode.return_value.option.assert_called_once_with(
            "checkpointLocation", "/chk/orders"
        )
        writer.foreachBatch.return_value.start.assert_called_once_with()

    def test_each_micro_batch_runs_batch_merge(self, provider, monkeypatch):
        entity = self._make_entity()
        merge_calls = []
        monkeypatch.setattr(
            provider, "merge_to_entity", lambda df, ent: merge_calls.append((df, ent))
        )
        df = MagicMock()
        writer = df.writeStream.outputMode.return_value.option.return_value

        provider.merge_as_stream(df, entity, "/chk/orders")

        batch_fn = writer.foreachBatch.call_args[0][0]
        batch_df = MagicMock()
        batch_fn(batch_df, 42)

        assert merge_calls == [(batch_df, entity)]

    def test_trigger_and_query_name_options_are_applied(self, provider):
        entity = self._make_entity()
        df = MagicMock()
        after_batch = (
            df.writeStream.outputMode.return_value.option.return_value.foreachBatch.return_value
        )

        provider.merge_as_stream(
            df,
            entity,
            "/chk/orders",
            options={"trigger": {"availableNow": True}, "query_name": "orders-merge"},
        )

        after_batch.trigger.assert_called_once_with(availableNow=True)
        after_batch.trigger.return_value.queryName.assert_called_once_with("orders-merge")
        after_batch.trigger.return_value.queryName.return_value.start.assert_called_once_with()
