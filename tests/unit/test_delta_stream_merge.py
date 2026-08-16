"""
Unit tests for DeltaEntityProvider.merge_as_stream.

merge_as_stream wraps the batch merge in a foreachBatch streaming sink. The
per-batch callback (_merge_batch) applies the same SCD1/SCD2 merge semantics
as the batch path, but does not call merge_to_entity or touch GlobalInjector
at all: under Spark Connect, foreachBatch()'s callback runs in a freshly
spawned, isolated worker process (foreach_batch_worker.py) that never ran
initialize_framework()'s bootstrap sequence, so GlobalInjector has no
bindings there and any GlobalInjector.get(...) call fails with a bare
KeyError. Everything DI-dependent (the table reference, merge strategy name,
merge condition, schema-drift policy) is resolved once on the driver, inside
merge_as_stream itself, before the writer is built; only that plain,
picklable data is captured by the closure.

These tests verify the writer wiring, the guards (read_only, missing
merge_columns), option pass-through, that the closure never captures a live
provider/session/DI reference, and that the batch callback actually merges
using only that captured plain data plus batch_df.sparkSession.
"""

from unittest.mock import MagicMock

import pytest
from kindling.data_entities import EntityMetadata, EntityNameMapper, EntityPathLocator
from kindling.entity_provider import StreamMergeableEntityProvider, is_stream_mergeable
from kindling.entity_provider_delta import DeltaEntityProvider, ReadOnlyEntityError
from kindling.injection import GlobalInjector
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
        df.columns = ["order_id", "status", "updated_at"]
        query = (
            df.writeStream.outputMode.return_value.option.return_value.foreachBatch.return_value.start.return_value
        )
        assert provider.merge_as_stream(df, entity, "/chk") is query

    def test_sequence_by_missing_from_stream_raises_before_start(self, provider):
        # A missing sequence column would silently skip the per-batch
        # collapse and kill the query mid-stream; it must fail at query
        # start, before the writer is even built.
        entity = self._make_entity(tags={"scd.type": "2", "scd.sequence_by": "updated_at"})
        df = MagicMock()
        df.columns = ["order_id", "status"]

        with pytest.raises(ValueError, match="scd.sequence_by"):
            provider.merge_as_stream(df, entity, "/chk")

        df.writeStream.outputMode.assert_not_called()

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

    def test_each_micro_batch_merges_directly_without_di(self, provider, monkeypatch):
        """The batch callback must complete a merge using only its captured
        plain data (table_name/table_path/access_mode/entity/cfg/
        strategy_name/merge_condition) plus batch_df.sparkSession -- never
        GlobalInjector, never merge_to_entity, never any DeltaEntityProvider
        instance. Proven here by patching GlobalInjector.get to raise
        unconditionally: if the callback so much as touched it, this test
        would fail loudly instead of silently passing with a stubbed
        lookup.
        """
        entity = self._make_entity()
        df = MagicMock()
        writer = df.writeStream.outputMode.return_value.option.return_value

        provider.merge_as_stream(df, entity, "/chk/orders")
        batch_fn = writer.foreachBatch.call_args[0][0]

        def _must_not_be_called(*args, **kwargs):
            raise AssertionError(
                "GlobalInjector.get() was called from the foreachBatch callback -- "
                "it must never touch DI, since Spark Connect's isolated foreachBatch "
                "worker process never bootstraps the framework and has no bindings."
            )

        monkeypatch.setattr(GlobalInjector, "get", _must_not_be_called)

        mock_delta_table = MagicMock(name="delta_table")
        monkeypatch.setattr(
            "kindling.entity_provider_delta.DeltaTableReference.get_delta_table",
            lambda self: mock_delta_table,
        )

        apply_calls = []
        mock_strategy = MagicMock(name="strategy")
        mock_strategy.apply.side_effect = lambda *a, **kw: apply_calls.append((a, kw))
        monkeypatch.setattr(
            "kindling.entity_provider_delta.DeltaMergeStrategies.get",
            lambda name: mock_strategy,
        )

        batch_df = MagicMock()
        batch_df.sparkSession = MagicMock(name="batch_spark_session")

        # Must not raise -- and must not need GlobalInjector.get stubbed to
        # return anything sensible; it's stubbed above only to fail loudly
        # if called at all.
        batch_fn(batch_df, 42)

        assert len(apply_calls) == 1
        args, _kwargs = apply_calls[0]
        delta_table_arg, df_arg, entity_arg, merge_condition_arg = args
        assert delta_table_arg is mock_delta_table
        assert df_arg is batch_df
        assert entity_arg is entity
        assert merge_condition_arg == "old.`order_id` = new.`order_id`"

    def test_merge_batch_closure_does_not_capture_provider_or_spark(self, provider):
        """Spark-Connect regression: foreachBatch() cloudpickles its callback
        up front to ship it for micro-batch execution, and a captured
        SparkSession (reachable via a captured provider instance, e.g.
        `self`) fails with "TypeError: cannot pickle '_thread.RLock' object"
        (pyspark.errors.PySparkPicklingError:
        [STREAMING_CONNECT_SERIALIZATION_ERROR]) -- before any batch ever
        runs. `_merge_batch` must not close over the provider instance (or
        anything else holding a live SparkSession); it must use only the
        micro-batch's own bound session instead. Assert this at the
        bytecode level: neither the provider instance nor its session, nor
        GlobalInjector/EntityProvider/EntityNameMapper (the DI container and
        the interfaces the earlier, still-broken fix looked them up by) may
        appear among the callback's free variables or captured closure
        values -- the isolated foreachBatch worker process under Spark
        Connect never bootstraps the framework, so any of those would fail
        at pickling time (a live session) or at call time (a KeyError from
        an unbootstrapped GlobalInjector).
        """
        entity = self._make_entity()
        df = MagicMock()
        writer = df.writeStream.outputMode.return_value.option.return_value

        provider.merge_as_stream(df, entity, "/chk/orders")

        batch_fn = writer.foreachBatch.call_args[0][0]
        free_vars = batch_fn.__code__.co_freevars
        closure_values = [
            cell.cell_contents for cell in (batch_fn.__closure__ or ()) if cell.cell_contents
        ]

        assert "self" not in free_vars
        assert "GlobalInjector" not in free_vars
        assert "EntityProvider" not in free_vars
        assert "EntityNameMapper" not in free_vars
        assert provider not in closure_values
        assert provider.spark not in closure_values
        assert GlobalInjector not in closure_values

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
