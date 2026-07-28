"""Unit tests for the JVM-free telemetry providers (kindling.plain_telemetry)."""

import json
import logging
import re
import uuid
from datetime import datetime
from unittest.mock import MagicMock, patch

import pytest
from kindling.plain_telemetry import (
    PlainPythonLogger,
    PlainPythonLoggerProvider,
    PlainPythonTraceProvider,
)
from kindling.spark_log_provider import PythonLoggerProvider
from kindling.spark_trace import SparkSpan, SparkTraceProvider


class _Config:
    def __init__(self, values=None):
        self.values = values or {}

    def get(self, key, default=None):
        return self.values.get(key, default)


class TestPlainPythonLogger:
    def test_logs_through_python_logging(self, caplog):
        logger = PlainPythonLogger("kindling.test.plain")

        with caplog.at_level(logging.DEBUG, logger="kindling.test.plain"):
            logger.info("hello")

        assert len(caplog.records) == 1
        assert caplog.records[0].levelno == logging.INFO
        assert "INFO: (kindling.test.plain) hello" in caplog.records[0].message

    def test_respects_configured_log_level(self, caplog):
        logger = PlainPythonLogger("kindling.test.level", config={"log_level": "error"})

        with caplog.at_level(logging.DEBUG, logger="kindling.test.level"):
            logger.info("suppressed")
            logger.error("emitted")

        assert len(caplog.records) == 1
        assert "emitted" in caplog.records[0].message

    def test_prints_when_print_logging_enabled(self):
        logger = PlainPythonLogger("kindling.test.print", config={"print_logging": True})

        with patch("builtins.print") as mock_print:
            logger.warning("watch out")

        assert "WARN: (kindling.test.print) watch out" in mock_print.call_args[0][0]

    def test_exception_appends_traceback(self, caplog):
        logger = PlainPythonLogger("kindling.test.exc")

        with caplog.at_level(logging.DEBUG, logger="kindling.test.exc"):
            try:
                raise ValueError("boom")
            except ValueError:
                logger.exception("failed")

        assert "ValueError: boom" in caplog.records[0].message

    def test_supports_sparklogger_method_surface(self):
        logger = PlainPythonLogger("kindling.test.surface")
        for method in ("debug", "info", "warn", "warning", "error", "exception"):
            getattr(logger, method)("msg")


class TestPlainPythonLoggerProvider:
    def test_is_python_logger_provider(self):
        provider = PlainPythonLoggerProvider(_Config())
        assert isinstance(provider, PythonLoggerProvider)

    def test_get_logger_returns_named_plain_logger(self):
        provider = PlainPythonLoggerProvider(_Config({"log_level": "debug"}))
        logger = provider.get_logger("Component", session=MagicMock())

        assert isinstance(logger, PlainPythonLogger)
        assert logger.name == "Component"
        assert logger.log_level == "debug"


def _trace_provider(config=None):
    logger_provider = MagicMock()
    logger = MagicMock()
    logger_provider.get_logger.return_value = logger
    return PlainPythonTraceProvider(logger_provider, config or _Config()), logger


class TestPlainPythonTraceProvider:
    def test_is_spark_trace_provider(self):
        trace, _ = _trace_provider()
        assert isinstance(trace, SparkTraceProvider)

    def test_span_emits_start_and_end(self):
        trace, logger = _trace_provider()

        with trace.span(operation="Op", component="Comp"):
            pass

        lines = [c[0][0] for c in logger.debug.call_args_list]
        assert any("Op: Op_START" in line for line in lines)
        assert any("Op: Op_END" in line for line in lines)
        assert all("Component: Comp" in line for line in lines)

    def test_span_swallows_exception_and_emits_error(self):
        trace, logger = _trace_provider()

        with trace.span(operation="Op", component="Comp", reraise=False):
            raise ValueError("boom")

        lines = [c[0][0] for c in logger.debug.call_args_list]
        assert any("Op_ERROR" in line for line in lines)
        assert any("Op_END" in line for line in lines)

    def test_span_reraises_when_requested(self):
        trace, logger = _trace_provider()

        with pytest.raises(ValueError, match="boom"):
            with trace.span(operation="Op", component="Comp", reraise=True):
                raise ValueError("boom")

        lines = [c[0][0] for c in logger.debug.call_args_list]
        assert any("Op_ERROR" in line for line in lines)

    def test_nested_spans_share_trace_id_and_inherit(self):
        trace, logger = _trace_provider()

        with trace.span(operation="Outer", component="Parent"):
            outer_trace_id = trace.current_span.traceId
            with trace.span():
                assert trace.current_span.traceId == outer_trace_id
                assert trace.current_span.component == "Parent"
                assert trace.current_span.operation == "Outer"

        assert trace.current_span is None

    def test_span_never_touches_spark(self):
        trace, _ = _trace_provider()

        with patch("kindling.spark_trace.get_or_create_spark_session") as mock_session:
            with trace.span(operation="Op", component="Comp"):
                pass

        mock_session.assert_not_called()

    def test_prints_when_print_trace_enabled(self):
        trace, _ = _trace_provider(_Config({"print_trace": True}))

        with patch("builtins.print") as mock_print:
            with trace.span(operation="Op", component="Comp"):
                pass

        printed = [c[0][0] for c in mock_print.call_args_list]
        assert any("Op_START" in line for line in printed)
        assert any("Op_END" in line for line in printed)

    def test_manual_span_lifecycle(self):
        trace, logger = _trace_provider()

        span = trace.start_span(operation="stream", component="Query")
        trace.add_event(span, "batch_progress", {"batch_id": 1})
        trace.end_span(span, error="Connection lost")

        assert isinstance(span, SparkSpan)
        assert span.end_time is not None
        lines = [c[0][0] for c in logger.debug.call_args_list]
        assert any("stream_START" in line for line in lines)
        assert any("stream_EVENT_batch_progress" in line for line in lines)
        assert any("stream_ERROR" in line for line in lines)
        assert any("stream_END" in line for line in lines)

    def test_start_span_inherits_trace_id_from_active_span(self):
        trace, _ = _trace_provider()

        with trace.span(operation="Outer", component="Parent"):
            manual = trace.start_span(operation="Inner", component="Child")
            assert manual.traceId == trace.current_span.traceId

        standalone = trace.start_span(operation="Solo", component="Comp")
        assert isinstance(standalone.traceId, uuid.UUID)


def _emitted(logger, event):
    """Return (line, details) pairs for a given emitted event name."""
    results = []
    for c in logger.debug.call_args_list:
        line = c[0][0]
        if f"Op: {event} " not in line:
            continue
        match = re.search(r"Msg: (\{.*\}|None) Id:", line)
        details = json.loads(match.group(1)) if match and match.group(1) != "None" else {}
        results.append((line, details))
    return results


class TestPlainTraceParenting:
    """Parent restore and parentSpanId emission (gh#210 Phase 0)."""

    def test_parent_restored_after_nested_exit(self):
        trace, _ = _trace_provider()

        with trace.span(operation="Outer", component="Comp"):
            outer_span = trace.current_span
            with trace.span(operation="Inner", component="Comp"):
                assert trace.current_span is not outer_span
            assert trace.current_span is outer_span

        assert trace.current_span is None

    def test_consecutive_top_level_spans_get_distinct_trace_ids(self):
        trace, _ = _trace_provider()

        with trace.span(operation="Op1", component="Comp"):
            first_trace_id = trace.current_span.traceId
        with trace.span(operation="Op2", component="Comp"):
            second_trace_id = trace.current_span.traceId

        assert first_trace_id != second_trace_id

    def test_child_events_carry_parent_span_id(self):
        trace, logger = _trace_provider()

        with trace.span(operation="Outer", component="Comp"):
            outer_id = trace.current_span.id
            with trace.span(operation="Inner", component="Comp"):
                pass

        for event in ("Inner_START", "Inner_END"):
            emitted = _emitted(logger, event)
            assert len(emitted) == 1
            assert emitted[0][1].get("parentSpanId") == outer_id

    def test_root_span_events_have_no_parent_span_id(self):
        trace, logger = _trace_provider()

        with trace.span(operation="Op", component="Comp"):
            pass

        for event in ("Op_START", "Op_END"):
            emitted = _emitted(logger, event)
            assert len(emitted) == 1
            assert "parentSpanId" not in emitted[0][1]

    def test_manual_span_events_carry_parent_span_id(self):
        trace, logger = _trace_provider()

        with trace.span(operation="Outer", component="Comp"):
            outer_id = trace.current_span.id
            manual = trace.start_span(operation="Inner", component="Comp")
            assert manual.parent_id == outer_id
            trace.end_span(manual)

        for event in ("Inner_START", "Inner_END"):
            emitted = _emitted(logger, event)
            assert len(emitted) == 1
            assert emitted[0][1].get("parentSpanId") == outer_id


class TestPlainRecordSpan:
    """record_span: retroactive spans with explicit timestamps (gh#210 Phase 0)."""

    def test_record_span_honors_given_timestamps(self):
        trace, logger = _trace_provider()
        start = datetime(2026, 1, 1, 12, 0, 0)
        end = datetime(2026, 1, 1, 12, 0, 2, 500000)

        trace.record_span("phase", "kindling.bootstrap", start, end, details={"k": "v"})

        start_emitted = _emitted(logger, "phase_START")
        end_emitted = _emitted(logger, "phase_END")
        assert len(start_emitted) == 1 and len(end_emitted) == 1
        assert start_emitted[0][1]["startTime"] == "2026-01-01 12:00:00.000"
        assert start_emitted[0][1]["k"] == "v"
        assert end_emitted[0][1]["endTime"] == "2026-01-01 12:00:02.500"
        assert end_emitted[0][1]["totalTime"] == "2.500"

    def test_record_span_with_error_emits_error_event(self):
        trace, logger = _trace_provider()
        start = datetime(2026, 1, 1, 12, 0, 0)
        end = datetime(2026, 1, 1, 12, 0, 1)

        trace.record_span("phase", "kindling.bootstrap", start, end, error="boom")

        error_emitted = _emitted(logger, "phase_ERROR")
        assert len(error_emitted) == 1
        assert error_emitted[0][1]["exception"] == "boom"
        assert error_emitted[0][1]["errorTime"] == "2026-01-01 12:00:01.000"

    def test_record_span_parents_to_current_span(self):
        trace, logger = _trace_provider()
        start = datetime(2026, 1, 1, 12, 0, 0)
        end = datetime(2026, 1, 1, 12, 0, 1)

        with trace.span(operation="Outer", component="Comp"):
            outer_id = trace.current_span.id
            expected_trace_id = str(trace.current_span.traceId)
            trace.record_span("phase", "kindling.bootstrap", start, end)

        for event in ("phase_START", "phase_END"):
            line, details = _emitted(logger, event)[0]
            assert details.get("parentSpanId") == outer_id
            assert f"trace_id:{expected_trace_id}" in line
