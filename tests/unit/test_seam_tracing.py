"""Structural-seam span tests for gh#210 Phase 2.

Covers the persist reraise fix (data correctness), the consolidated
kindling.pipes read/persist span shapes, watermark manager spans, migration
service spans, and the config reload span — all asserted through
RecordingTraceProvider.
"""

from types import SimpleNamespace
from unittest.mock import MagicMock, Mock, patch

import pytest
from kindling.entity_provider import BaseEntityProvider, WritableEntityProvider
from kindling.injection import GlobalInjector
from kindling.simple_read_persist_strategy import SimpleReadPersistStrategy
from kindling.test_framework import RecordingTraceProvider
from kindling.trace_ops import COMPONENT_PIPES, TracingGates
from pyspark.sql import SparkSession

from tests.conftest import _sockets_permitted


@pytest.fixture(scope="module")
def spark_session():
    """Plain (non-Delta), module-scoped, self-contained SparkSession.

    Deliberately shadows conftest.py's shared, session-scoped fixture rather
    than requesting it: TestWatermarkSpans only needs an active SparkContext
    for pyspark.sql.functions.col(...) to build a Column against a mocked
    DataFrame, not a real Delta-capable session. Depending on the shared
    fixture would leave it (and conftest's get_local_spark_session Delta
    catalog config, which sets the DeltaCatalog class without loading the
    Delta JAR) active for the rest of the process -- silently corrupting any
    later plain-session fixture that reuses it via getOrCreate(). See
    test_entity_provider_memory_scd2.py's module docstring for the same
    hazard from the other direction.
    """
    if not _sockets_permitted():
        pytest.skip(
            "Sockets are not permitted in this environment; cannot start a real SparkSession."
        )
    spark = SparkSession.builder.appName("SeamTracingTests").master("local[2]").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    yield spark
    spark.stop()


class _MergeWritableProvider(BaseEntityProvider, WritableEntityProvider):
    """Spec class: batch-writable provider that also supports merge."""

    def merge_to_entity(self, df, entity): ...


def _make_strategy(dst_entity, out_provider, tp=None):
    der = Mock()
    src_entity = Mock(entityid="entity.src", tags={})
    der.get_entity_definition.side_effect = lambda eid: {
        "entity.src": src_entity,
        "entity.dst": dst_entity,
    }[eid]

    provider_registry = Mock()
    provider_registry.get_provider_for_entity.return_value = out_provider

    lp = Mock()
    lp.get_logger.return_value = Mock()

    strategy = SimpleReadPersistStrategy(
        ep=Mock(),
        der=der,
        tp=tp if tp is not None else RecordingTraceProvider(),
        lp=lp,
        provider_registry=provider_registry,
        signal_provider=None,
    )
    pipe = Mock(
        pipeid="pipe1",
        input_entity_ids=["entity.src"],
        output_entity_id="entity.dst",
    )
    return strategy, pipe


def _spy_signals(strategy, resolve_read_result=None):
    emitted = []

    def _spy(signal_name, **kwargs):
        emitted.append(signal_name)
        if signal_name == "read.resolve_read" and resolve_read_result is not None:
            return [("handler", resolve_read_result)]
        return []

    strategy.emit = _spy
    return emitted


class TestPersistReraiseFix:
    """A failed merge must never look like a successful persist (gh#210)."""

    def test_merge_failure_propagates_and_never_emits_after_persist(self):
        dst_entity = Mock(entityid="entity.dst", tags={})
        out_provider = Mock(spec=_MergeWritableProvider)
        out_provider.check_entity_exists.return_value = True
        out_provider.merge_to_entity.side_effect = RuntimeError("merge exploded")

        tp = RecordingTraceProvider()
        strategy, pipe = _make_strategy(dst_entity, out_provider, tp=tp)
        emitted = _spy_signals(strategy)
        persist = strategy.create_pipe_persist_activator(pipe)

        with pytest.raises(RuntimeError, match="merge exploded"):
            persist(Mock(name="df"))

        assert "persist.before_persist" in emitted
        assert "persist.persist_failed" in emitted
        assert "persist.after_persist" not in emitted, (
            "after_persist for unpersisted data advances the watermark past "
            "rows that were never written"
        )

        span = tp.find(component=COMPONENT_PIPES, operation="persist")[0]
        assert span.closed
        assert "merge exploded" in span.error

    def test_successful_persist_is_a_single_consolidated_span(self):
        dst_entity = Mock(entityid="entity.dst", tags={})
        out_provider = Mock(spec=_MergeWritableProvider)
        out_provider.check_entity_exists.return_value = True

        tp = RecordingTraceProvider()
        strategy, pipe = _make_strategy(dst_entity, out_provider, tp=tp)
        _spy_signals(strategy)
        persist = strategy.create_pipe_persist_activator(pipe)

        persist(Mock(name="df"))

        persist_spans = tp.find(component=COMPONENT_PIPES, operation="persist")
        assert len(persist_spans) == 1
        assert persist_spans[0].details["pipe_id"] == "pipe1"
        assert persist_spans[0].details["source_entity_id"] == "entity.src"
        assert persist_spans[0].details["output_entity_id"] == "entity.dst"
        assert "persist_id" in persist_spans[0].details

        legacy = [s for s in tp.spans if s.component == "data_utils"]
        assert legacy == [], "Inner data_utils spans were consolidated away"
        out_provider.merge_to_entity.assert_called_once()

    def test_persist_span_disabled_when_tracing_off(self):
        dst_entity = Mock(entityid="entity.dst", tags={})
        out_provider = Mock(spec=_MergeWritableProvider)
        out_provider.check_entity_exists.return_value = True

        tp = RecordingTraceProvider()
        strategy, pipe = _make_strategy(dst_entity, out_provider, tp=tp)
        strategy._trace_gates = TracingGates(enabled=False, level="standard")
        _spy_signals(strategy)

        strategy.create_pipe_persist_activator(pipe)(Mock(name="df"))

        assert tp.spans == []
        out_provider.merge_to_entity.assert_called_once()


class TestReadSpan:
    def test_resolved_read_marks_short_circuit(self):
        dst_entity = Mock(entityid="entity.dst", tags={})
        tp = RecordingTraceProvider()
        strategy, pipe = _make_strategy(dst_entity, Mock(), tp=tp)
        resolved = SimpleNamespace(df=Mock(name="resolved_df"))
        _spy_signals(strategy, resolve_read_result=resolved)
        reader = strategy.create_pipe_entity_reader(pipe)

        entity = Mock(entityid="entity.src", tags={})
        df = reader(entity, True)

        assert df is resolved.df
        span = tp.find(component=COMPONENT_PIPES, operation="read")[0]
        assert span.details["entity_id"] == "entity.src"
        assert span.details["pipe_id"] == "pipe1"
        assert span.details["watermarked"] is True
        assert span.details["resolved"] is True

    def test_provider_fallback_read_is_spanned(self):
        dst_entity = Mock(entityid="entity.dst", tags={})
        out_provider = Mock()
        out_provider.read_entity.return_value = Mock(name="df")
        tp = RecordingTraceProvider()
        strategy, pipe = _make_strategy(dst_entity, out_provider, tp=tp)
        _spy_signals(strategy)
        reader = strategy.create_pipe_entity_reader(pipe)

        entity = Mock(entityid="entity.src", tags={})
        with patch("kindling.simple_read_persist_strategy._is_local_execution", return_value=False):
            reader(entity, False)

        span = tp.find(component=COMPONENT_PIPES, operation="read")[0]
        assert span.details["resolved"] is False
        assert span.details["watermarked"] is False
        out_provider.read_entity.assert_called_once_with(entity)

    def test_read_failure_propagates_with_span_error(self):
        dst_entity = Mock(entityid="entity.dst", tags={})
        out_provider = Mock()
        out_provider.read_entity.side_effect = ValueError("storage offline")
        tp = RecordingTraceProvider()
        strategy, pipe = _make_strategy(dst_entity, out_provider, tp=tp)
        _spy_signals(strategy)
        reader = strategy.create_pipe_entity_reader(pipe)

        with patch("kindling.simple_read_persist_strategy._is_local_execution", return_value=False):
            with pytest.raises(ValueError, match="storage offline"):
                reader(Mock(entityid="entity.src", tags={}), False)

        span = tp.find(component=COMPONENT_PIPES, operation="read")[0]
        assert span.closed
        assert "storage offline" in span.error


class TestWatermarkSpans:
    def _manager(self, tp):
        from kindling.watermarking import WatermarkManager

        lp = Mock()
        lp.get_logger.return_value = Mock()
        with patch("kindling.watermarking.get_or_create_spark_session", return_value=MagicMock()):
            manager = WatermarkManager(
                ep=Mock(),
                wef=Mock(),
                lp=lp,
                signal_provider=None,
                provider_registry=None,
                tp=tp,
                config=None,
            )
        return manager

    def test_get_cursor_emits_span_with_ids(self, spark_session):
        # get_cursor() builds a real pyspark.sql.functions.col(...) predicate
        # against the (mocked) DataFrame, which needs an active SparkContext
        # regardless of the DataFrame itself being a MagicMock. Request the
        # shared fixture explicitly rather than relying on some earlier,
        # unrelated test in the suite happening to leave a session active.
        tp = RecordingTraceProvider()
        manager = self._manager(tp)
        df = MagicMock()
        df.isEmpty.return_value = True
        manager.ep.read_entity.return_value.filter.return_value.limit.return_value = df

        cursor = manager.get_cursor("entity.src", "pipe1")

        assert cursor is None
        span = tp.find(component="kindling.watermark", operation="get_cursor")[0]
        assert span.details == {"source_entity_id": "entity.src", "reader_id": "pipe1"}
        assert span.closed

    def test_save_cursor_emits_span_with_cursor_attr(self):
        tp = RecordingTraceProvider()
        manager = self._manager(tp)

        manager.save_cursor("entity.src", "pipe1", "42", "exec-1")

        span = tp.find(component="kindling.watermark", operation="save_cursor")[0]
        assert span.details["cursor"] == "42"
        manager.ep.merge_to_entity.assert_called_once()

    def test_save_cursor_failure_propagates_and_errors_span(self):
        tp = RecordingTraceProvider()
        manager = self._manager(tp)
        manager.ep.merge_to_entity.side_effect = RuntimeError("merge failed")

        with pytest.raises(RuntimeError, match="merge failed"):
            manager.save_cursor("entity.src", "pipe1", "42", "exec-1")

        span = tp.find(component="kindling.watermark", operation="save_cursor")[0]
        assert span.closed
        assert "merge failed" in span.error

    def test_no_spans_when_tp_absent(self, spark_session):
        # Same real-SparkContext dependency as test_get_cursor_emits_span_with_ids.
        manager = self._manager(None)
        df = MagicMock()
        df.isEmpty.return_value = True
        manager.ep.read_entity.return_value.filter.return_value.limit.return_value = df

        assert manager.get_cursor("entity.src", "pipe1") is None


class TestMigrationSpans:
    def _service_with_tracing(self, tp):
        import kindling.migration as migration_module

        class _Config:
            def get(self, key, default=None):
                return default

        class _Injector:
            @staticmethod
            def get(iface):
                from kindling.spark_config import ConfigService
                from kindling.spark_trace import SparkTraceProvider

                if iface is ConfigService:
                    return _Config()
                if iface is SparkTraceProvider:
                    return tp
                raise AssertionError(f"unexpected resolution: {iface}")

        service = migration_module.MigrationService(planner=Mock(), applier=Mock(), manager=Mock())
        return service, patch.object(migration_module, "GlobalInjector", _Injector)

    def test_plan_and_apply_emit_migration_spans(self):
        from kindling.migration import BackupStrategy

        tp = RecordingTraceProvider()
        service, injector_patch = self._service_with_tracing(tp)
        plan = Mock(statuses=[])
        service._planner.plan.return_value = plan

        with injector_patch:
            service.plan()
            service.apply(plan, allow_destructive=True, backup=BackupStrategy.SNAPSHOT)

        plan_span = tp.find(component="kindling.migration", operation="plan")[0]
        assert plan_span.details["entity_count"] == 0
        apply_span = tp.find(component="kindling.migration", operation="apply")[0]
        assert apply_span.details["allow_destructive"] is True
        assert apply_span.details["backup"] == "snapshot"

    def test_migration_spans_never_break_the_operation(self):
        """GlobalInjector resolution failure must degrade to a no-op span."""
        service = __import__("kindling.migration", fromlist=["MigrationService"]).MigrationService(
            planner=Mock(), applier=Mock(), manager=Mock()
        )
        service._planner.plan.return_value = Mock(statuses=[])

        with patch("kindling.migration.GlobalInjector") as broken:
            broken.get.side_effect = RuntimeError("no injector")
            plan = service.plan()

        assert plan is not None


class TestConfigReloadSpan:
    @patch("kindling.spark_config.Dynaconf")
    @patch("kindling.spark_config.get_or_create_spark_session")
    def test_reload_emits_config_span(self, mock_spark_fn, mock_dynaconf_class):
        from kindling.spark_config import DynaconfConfig
        from kindling.trace_ops import TracingGates

        mock_spark_fn.return_value = MagicMock()
        mock_dynaconf = MagicMock()
        mock_dynaconf.to_dict.return_value = {"test_value": "original"}
        mock_dynaconf_class.return_value = mock_dynaconf

        config = DynaconfConfig()
        config.initialize(reload_context=None)

        tp = RecordingTraceProvider()
        with patch(
            "kindling.trace_ops.tracing_gates",
            return_value=TracingGates(enabled=True, level="standard"),
        ):
            with patch.object(GlobalInjector, "get", return_value=tp):
                result = config.reload()

        assert result["status"] == "success"
        span = tp.find(component="kindling.config", operation="reload")[0]
        assert span.closed
