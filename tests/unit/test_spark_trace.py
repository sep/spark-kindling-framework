"""Unit tests for SparkTraceProvider and EventBasedSparkTrace."""

import json
import time
import uuid
from datetime import datetime
from unittest.mock import MagicMock, Mock, call, patch

import pytest
import kindling.spark_trace as spark_trace_module
from kindling.spark_trace import (
    AzureEventEmitter,
    CustomEventEmitter,
    EventBasedSparkTrace,
    SparkSpan,
    SparkTraceProvider,
    mdc_context,
)


@pytest.fixture(autouse=True)
def mock_spark_session_for_mdc(mock_spark_for_trace):
    """Automatically mock get_or_create_spark_session for all tests."""
    with patch(
        "kindling.spark_trace.get_or_create_spark_session", return_value=mock_spark_for_trace
    ):
        yield


@pytest.fixture(autouse=True)
def reset_mdc_block_flag():
    """Ensure MDC fallback state doesn't leak between tests."""
    spark_trace_module._mdc_api_blocked = False
    yield
    spark_trace_module._mdc_api_blocked = False


class TestCustomEventEmitter:
    """Test CustomEventEmitter ABC."""

    def test_custom_event_emitter_is_abstract(self):
        """CustomEventEmitter should be an abstract base class."""
        with pytest.raises(TypeError, match="Can't instantiate abstract class"):
            CustomEventEmitter()

    def test_custom_event_emitter_requires_emit_method(self):
        """Subclasses must implement emit_custom_event method."""

        class IncompleteEmitter(CustomEventEmitter):
            pass

        with pytest.raises(TypeError, match="Can't instantiate abstract class"):
            IncompleteEmitter()


class TestAzureEventEmitter:
    """Test AzureEventEmitter implementation."""

    def test_initialization(self, mock_logger_provider, mock_config):
        """AzureEventEmitter should initialize with logger and config."""
        emitter = AzureEventEmitter(mock_logger_provider, mock_config)

        assert emitter.logger is not None, "Logger should be initialized from provider"
        assert emitter.config == mock_config, "Config should be stored during initialization"
        mock_logger_provider.get_logger.assert_called_once_with(
            "EventEmitter"
        ), "Should request EventEmitter logger"

    def test_should_print_when_print_trace_enabled(self, mock_logger_provider, mock_config):
        """should_print() should return True when print_trace is enabled."""
        mock_config.get = Mock(
            side_effect=lambda key, default: True if key == "print_trace" else False
        )
        emitter = AzureEventEmitter(mock_logger_provider, mock_config)

        result = emitter.should_print()

        assert result is True, "should_print should return True when print_trace is enabled"

    def test_should_print_when_print_tracing_enabled(self, mock_logger_provider, mock_config):
        """should_print() should return True when print_tracing is enabled."""
        mock_config.get = Mock(
            side_effect=lambda key, default: True if key == "print_tracing" else False
        )
        emitter = AzureEventEmitter(mock_logger_provider, mock_config)

        result = emitter.should_print()

        assert result is True, "should_print should return True when print_tracing is enabled"

    def test_should_print_when_both_disabled(self, mock_logger_provider, mock_config):
        """should_print() should return False when both print flags are disabled."""
        mock_config.get = Mock(return_value=False)
        emitter = AzureEventEmitter(mock_logger_provider, mock_config)

        result = emitter.should_print()

        assert result is False, "should_print should return False when both flags are disabled"

    def test_emit_custom_event_creates_spark_event(
        self, mock_logger_provider, mock_config, mock_spark_for_trace
    ):
        """emit_custom_event should create and post ComponentSparkEvent."""
        emitter = AzureEventEmitter(mock_logger_provider, mock_config)
        test_trace_id = uuid.uuid4()
        details = {"key": "value", "count": 42}

        with patch(
            "kindling.spark_trace.get_or_create_spark_session", return_value=mock_spark_for_trace
        ):
            emitter.emit_custom_event(
                "TestComponent", "TestOperation", details, "event-123", test_trace_id
            )

        # Verify ComponentSparkEvent was created with correct parameters
        mock_spark_for_trace._jvm.com.microsoft.spark.metricevents.ComponentSparkEvent.assert_called_once()
        call_args = (
            mock_spark_for_trace._jvm.com.microsoft.spark.metricevents.ComponentSparkEvent.call_args
        )

        assert call_args[0][0] == "TestApp", "App name should be from Spark context"
        assert call_args[0][1] == "TestComponent", "Component should be passed correctly"
        assert call_args[0][2] == "TestOperation", "Operation should be passed correctly"

    def test_emit_custom_event_posts_to_listener_bus(
        self, mock_logger_provider, mock_config, mock_spark_for_trace
    ):
        """emit_custom_event should post event to Spark listener bus."""
        emitter = AzureEventEmitter(mock_logger_provider, mock_config)
        test_trace_id = uuid.uuid4()

        with patch(
            "kindling.spark_trace.get_or_create_spark_session", return_value=mock_spark_for_trace
        ):
            emitter.emit_custom_event("TestComp", "TestOp", {}, "event-1", test_trace_id)

        # Verify event was posted to listener bus
        mock_listener_bus = mock_spark_for_trace.sparkContext._jsc.sc().listenerBus()
        mock_listener_bus.post.assert_called_once(), "Event should be posted to listener bus"

    def test_emit_custom_event_serializes_details_to_json(
        self, mock_logger_provider, mock_config, mock_spark_for_trace
    ):
        """emit_custom_event should serialize details dict to JSON."""
        emitter = AzureEventEmitter(mock_logger_provider, mock_config)
        test_trace_id = uuid.uuid4()
        details = {"nested": {"data": "value"}, "list": [1, 2, 3]}

        with patch(
            "kindling.spark_trace.get_or_create_spark_session", return_value=mock_spark_for_trace
        ):
            emitter.emit_custom_event("TestComp", "TestOp", details, "event-1", test_trace_id)

        # Verify details were serialized to JSON
        call_args = (
            mock_spark_for_trace._jvm.com.microsoft.spark.metricevents.ComponentSparkEvent.call_args
        )
        custom_message_option = call_args[0][6]  # customMessage parameter

        # The custom message should be wrapped in Scala Option
        assert custom_message_option is not None, "Custom message should be provided"

    def test_emit_custom_event_prints_when_enabled(
        self, mock_logger_provider, mock_config, mock_spark_for_trace
    ):
        """emit_custom_event should print trace when print_trace is enabled."""
        mock_config.get = Mock(
            side_effect=lambda key, default: True if key == "print_trace" else False
        )
        emitter = AzureEventEmitter(mock_logger_provider, mock_config)
        test_trace_id = uuid.uuid4()
        details = {"test": "data"}

        with patch(
            "kindling.spark_trace.get_or_create_spark_session", return_value=mock_spark_for_trace
        ):
            with patch("builtins.print") as mock_print:
                emitter.emit_custom_event("TestComp", "TestOp", details, "event-1", test_trace_id)

        mock_print.assert_called_once(), "Should print trace when print_trace is enabled"
        printed_msg = mock_print.call_args[0][0]
        assert "TRACE:" in printed_msg, "Printed message should contain TRACE prefix"
        assert "TestComp" in printed_msg, "Printed message should contain component"
        assert "TestOp" in printed_msg, "Printed message should contain operation"

    def test_emit_custom_event_does_not_print_when_disabled(
        self, mock_logger_provider, mock_config, mock_spark_for_trace
    ):
        """emit_custom_event should not print when print_trace is disabled."""
        mock_config.get = Mock(return_value=False)
        emitter = AzureEventEmitter(mock_logger_provider, mock_config)
        test_trace_id = uuid.uuid4()

        with patch(
            "kindling.spark_trace.get_or_create_spark_session", return_value=mock_spark_for_trace
        ):
            with patch("builtins.print") as mock_print:
                emitter.emit_custom_event("TestComp", "TestOp", {}, "event-1", test_trace_id)

        mock_print.assert_not_called(), "Should not print trace when print_trace is disabled"

    def test_emit_custom_event_handles_none_details(
        self, mock_logger_provider, mock_config, mock_spark_for_trace
    ):
        """emit_custom_event should handle None details gracefully."""
        emitter = AzureEventEmitter(mock_logger_provider, mock_config)
        test_trace_id = uuid.uuid4()

        with patch(
            "kindling.spark_trace.get_or_create_spark_session", return_value=mock_spark_for_trace
        ):
            emitter.emit_custom_event("TestComp", "TestOp", None, "event-1", test_trace_id)

        # Should not raise exception
        mock_spark_for_trace._jvm.com.microsoft.spark.metricevents.ComponentSparkEvent.assert_called_once()


class TestMdcContext:
    """Test MDC context manager."""

    def test_mdc_context_sets_spark_local_properties(self, mock_spark_for_trace):
        """mdc_context should set Spark local properties for each key."""
        with patch(
            "kindling.spark_trace.get_or_create_spark_session", return_value=mock_spark_for_trace
        ):
            with mdc_context(trace_id="trace-123", span_id="span-456", component="TestComp"):
                pass

        # Verify setLocalProperty was called for each MDC key
        set_calls = [
            call for call in mock_spark_for_trace.sparkContext.setLocalProperty.call_args_list
        ]
        assert len(set_calls) >= 6, "Should set and clear local properties for each key"

        # Check that properties were set with mdc. prefix
        set_with_values = [c for c in set_calls if c[0][1] and c[0][1] != ""]
        assert any(
            "mdc.trace_id" in str(c) for c in set_with_values
        ), "Should set mdc.trace_id property"
        assert any(
            "mdc.span_id" in str(c) for c in set_with_values
        ), "Should set mdc.span_id property"
        assert any(
            "mdc.component" in str(c) for c in set_with_values
        ), "Should set mdc.component property"

    def test_mdc_context_sets_log4j_mdc(self, mock_spark_for_trace):
        """mdc_context should set Log4j MDC values."""
        mock_mdc = mock_spark_for_trace._jvm.org.apache.log4j.MDC

        with patch(
            "kindling.spark_trace.get_or_create_spark_session", return_value=mock_spark_for_trace
        ):
            with mdc_context(trace_id="trace-123", span_id="span-456"):
                pass

        # Verify MDC.put was called for each key
        put_calls = [call for call in mock_mdc.put.call_args_list]
        assert len(put_calls) >= 2, "Should call MDC.put for each key"

        # Verify the keys and values
        call_dict = {c[0][0]: c[0][1] for c in put_calls}
        assert call_dict.get("trace_id") == "trace-123", "Should set trace_id in MDC"
        assert call_dict.get("span_id") == "span-456", "Should set span_id in MDC"

    def test_mdc_context_clears_on_exit(self, mock_spark_for_trace):
        """mdc_context should clear MDC values on exit."""
        mock_mdc = mock_spark_for_trace._jvm.org.apache.log4j.MDC

        with patch(
            "kindling.spark_trace.get_or_create_spark_session", return_value=mock_spark_for_trace
        ):
            with mdc_context(trace_id="trace-123", operation="TestOp"):
                pass

        # Verify MDC.remove was called for each key
        remove_calls = [call for call in mock_mdc.remove.call_args_list]
        assert len(remove_calls) >= 2, "Should call MDC.remove for each key"

        removed_keys = [c[0][0] for c in remove_calls]
        assert "trace_id" in removed_keys, "Should remove trace_id from MDC"
        assert "operation" in removed_keys, "Should remove operation from MDC"

    def test_mdc_context_clears_spark_properties_on_exit(self, mock_spark_for_trace):
        """mdc_context should clear Spark local properties on exit."""
        with patch(
            "kindling.spark_trace.get_or_create_spark_session", return_value=mock_spark_for_trace
        ):
            with mdc_context(trace_id="trace-123"):
                pass

        # Verify setLocalProperty was called to clear (set to empty string)
        clear_calls = [
            c
            for c in mock_spark_for_trace.sparkContext.setLocalProperty.call_args_list
            if c[0][1] == ""
        ]
        assert len(clear_calls) >= 1, "Should clear local properties on exit"

    def test_mdc_context_clears_even_on_exception(self, mock_spark_for_trace):
        """mdc_context should clear MDC even when exception occurs."""
        mock_mdc = mock_spark_for_trace._jvm.org.apache.log4j.MDC

        with patch(
            "kindling.spark_trace.get_or_create_spark_session", return_value=mock_spark_for_trace
        ):
            try:
                with mdc_context(trace_id="trace-123"):
                    raise ValueError("Test error")
            except ValueError:
                pass

        # Verify MDC.remove was still called
        remove_calls = [call for call in mock_mdc.remove.call_args_list]
        assert len(remove_calls) >= 1, "Should clear MDC even when exception occurs"

    def test_mdc_context_reraises_non_whitelist_mdc_errors(self, mock_spark_for_trace):
        """mdc_context should still raise unexpected MDC failures."""
        mock_mdc = mock_spark_for_trace._jvm.org.apache.log4j.MDC
        mock_mdc.put.side_effect = RuntimeError("Unexpected MDC failure")

        with pytest.raises(RuntimeError, match="Unexpected MDC failure"):
            with mdc_context(trace_id="trace-123", span_id="span-456"):
                pass

    def test_mdc_context_disables_mdc_after_py4j_whitelist_error(self, mock_spark_for_trace):
        """Once blocked, mdc_context should skip direct log4j MDC calls."""
        mock_mdc = mock_spark_for_trace._jvm.org.apache.log4j.MDC

        class FakePy4JError(Exception):
            pass

        with patch("kindling.spark_trace.Py4JError", FakePy4JError):
            mock_mdc.put.side_effect = FakePy4JError(
                "Py4JSecurityException: Method public static void org.apache.log4j.MDC.put "
                "(java.lang.String,java.lang.String) is not whitelisted"
            )
            with mdc_context(trace_id="trace-123"):
                pass

            assert spark_trace_module._mdc_api_blocked is True

            mock_mdc.put.reset_mock()
            mock_mdc.remove.reset_mock()

            with mdc_context(trace_id="trace-456"):
                pass

        mock_mdc.put.assert_not_called()
        mock_mdc.remove.assert_not_called()


class TestSparkSpan:
    """Test SparkSpan dataclass."""

    def test_spark_span_initialization(self):
        """SparkSpan should initialize with required fields."""
        test_trace_id = uuid.uuid4()
        span = SparkSpan(
            id="1",
            component="TestComp",
            operation="TestOp",
            attributes={"key": "value"},
            traceId=test_trace_id,
            reraise=True,
        )

        assert span.id == "1", "Span ID should be set correctly"
        assert span.component == "TestComp", "Component should be set correctly"
        assert span.operation == "TestOp", "Operation should be set correctly"
        assert span.attributes == {"key": "value"}, "Attributes should be set correctly"
        assert span.traceId == test_trace_id, "Trace ID should be set correctly"
        assert span.reraise is True, "Reraise flag should be set correctly"

    def test_spark_span_optional_timestamps(self):
        """SparkSpan timestamps should default to None."""
        span = SparkSpan(
            id="1",
            component="TestComp",
            operation="TestOp",
            attributes={},
            traceId=uuid.uuid4(),
            reraise=False,
        )

        assert span.start_time is None, "Start time should default to None"
        assert span.end_time is None, "End time should default to None"

    def test_spark_span_with_timestamps(self):
        """SparkSpan should accept timestamp values."""
        start = datetime(2025, 1, 1, 12, 0, 0)
        end = datetime(2025, 1, 1, 12, 0, 1)

        span = SparkSpan(
            id="1",
            component="TestComp",
            operation="TestOp",
            attributes={},
            traceId=uuid.uuid4(),
            reraise=False,
            start_time=start,
            end_time=end,
        )

        assert span.start_time == start, "Start time should be set correctly"
        assert span.end_time == end, "End time should be set correctly"


class TestEventBasedSparkTrace:
    """Test EventBasedSparkTrace implementation."""

    def test_initialization(self, mock_emitter):
        """EventBasedSparkTrace should initialize with emitter."""
        trace = EventBasedSparkTrace(mock_emitter)

        assert trace.emitter == mock_emitter, "Emitter should be stored during initialization"
        assert trace.current_span is None, "Current span should be None initially"
        assert trace.activity_counter == 1, "Activity counter should start at 1"

    def test_increment_activity_increases_counter(self, mock_emitter):
        """_increment_activity should increase counter and return new value."""
        trace = EventBasedSparkTrace(mock_emitter)

        result1 = trace._increment_activity()
        result2 = trace._increment_activity()
        result3 = trace._increment_activity()

        assert result1 == 2, "First increment should return 2"
        assert result2 == 3, "Second increment should return 3"
        assert result3 == 4, "Third increment should return 4"
        assert trace.activity_counter == 4, "Counter should be at 4 after three increments"

    def test_add_timestamp_to_dict(self, mock_emitter):
        """_add_timestamp_to_dict should add formatted timestamp to dictionary."""
        trace = EventBasedSparkTrace(mock_emitter)
        test_dict = {"existing": "value"}
        test_time = datetime(2025, 10, 15, 14, 30, 45, 123456)

        result = trace._add_timestamp_to_dict(test_dict, "timestamp", test_time)

        assert "existing" in result, "Existing keys should be preserved"
        assert "timestamp" in result, "New timestamp key should be added"
        assert (
            result["timestamp"] == "2025-10-15 14:30:45.123"
        ), "Timestamp should be formatted correctly"

    def test_add_timestamp_handles_none_dict(self, mock_emitter):
        """_add_timestamp_to_dict should handle None input dict."""
        trace = EventBasedSparkTrace(mock_emitter)
        test_time = datetime(2025, 10, 15, 14, 30, 45)

        result = trace._add_timestamp_to_dict(None, "ts", test_time)

        assert result is not None, "Should return dict even when input is None"
        assert "ts" in result, "Timestamp key should be in result"

    def test_merge_dict(self, mock_emitter):
        """_merge_dict should merge two dictionaries."""
        trace = EventBasedSparkTrace(mock_emitter)
        dict1 = {"a": 1, "b": 2}
        dict2 = {"c": 3, "d": 4}

        result = trace._merge_dict(dict1, dict2)

        assert result == {"a": 1, "b": 2, "c": 3, "d": 4}, "Dictionaries should be merged"

    def test_merge_dict_overwrites_duplicate_keys(self, mock_emitter):
        """_merge_dict should let second dict overwrite first dict keys."""
        trace = EventBasedSparkTrace(mock_emitter)
        dict1 = {"a": 1, "b": 2}
        dict2 = {"b": 99, "c": 3}

        result = trace._merge_dict(dict1, dict2)

        assert result["b"] == 99, "Second dict should overwrite duplicate keys"

    def test_calculate_time_diff(self, mock_emitter):
        """_calculate_time_diff should calculate time difference in seconds."""
        trace = EventBasedSparkTrace(mock_emitter)
        dt1 = datetime(2025, 1, 1, 12, 0, 0)
        dt2 = datetime(2025, 1, 1, 12, 0, 1, 500000)  # 1.5 seconds later

        result = trace._calculate_time_diff(dt1, dt2)

        assert result == "1.500", "Time diff should be formatted to 3 decimal places"

    def test_calculate_time_diff_with_milliseconds(self, mock_emitter):
        """_calculate_time_diff should handle sub-second precision."""
        trace = EventBasedSparkTrace(mock_emitter)
        dt1 = datetime(2025, 1, 1, 12, 0, 0, 0)
        dt2 = datetime(2025, 1, 1, 12, 0, 0, 123456)  # 0.123456 seconds

        result = trace._calculate_time_diff(dt1, dt2)

        assert result == "0.123", "Should format with 3 decimal places"

    def test_span_creates_start_event(self, mock_emitter):
        """span context should emit START event on entry."""
        trace = EventBasedSparkTrace(mock_emitter)

        with trace.span(operation="TestOp", component="TestComp", details={"test": "data"}):
            pass

        # Verify START event was emitted
        start_calls = [
            c for c in mock_emitter.emit_custom_event.call_args_list if "START" in c[0][1]
        ]
        assert len(start_calls) == 1, "Should emit one START event"

        start_call = start_calls[0]
        assert start_call[0][0] == "TestComp", "Component should be correct in START event"
        assert start_call[0][1] == "TestOp_START", "Operation should have _START suffix"
        assert "startTime" in start_call[0][2], "START event should include startTime"

    def test_span_creates_end_event(self, mock_emitter):
        """span context should emit END event on exit."""
        trace = EventBasedSparkTrace(mock_emitter)

        with trace.span(operation="TestOp", component="TestComp"):
            pass

        # Verify END event was emitted
        end_calls = [c for c in mock_emitter.emit_custom_event.call_args_list if "END" in c[0][1]]
        assert len(end_calls) == 1, "Should emit one END event"

        end_call = end_calls[0]
        assert end_call[0][0] == "TestComp", "Component should be correct in END event"
        assert end_call[0][1] == "TestOp_END", "Operation should have _END suffix"
        assert "endTime" in end_call[0][2], "END event should include endTime"
        assert "totalTime" in end_call[0][2], "END event should include totalTime"

    def test_span_emits_events_in_order(self, mock_emitter):
        """span should emit START, then END events in correct order."""
        trace = EventBasedSparkTrace(mock_emitter)

        with trace.span(operation="TestOp", component="TestComp"):
            pass

        calls = mock_emitter.emit_custom_event.call_args_list
        assert len(calls) >= 2, "Should emit at least START and END events"

        operations = [c[0][1] for c in calls]
        assert operations[0].endswith("_START"), "First event should be START"
        assert operations[-1].endswith("_END"), "Last event should be END"

    def test_span_creates_error_event_on_exception(self, mock_emitter):
        """span should emit ERROR event when exception occurs."""
        trace = EventBasedSparkTrace(mock_emitter)

        try:
            with trace.span(operation="TestOp", component="TestComp", reraise=False):
                raise ValueError("Test error")
        except ValueError:
            pytest.fail("Exception should be caught when reraise=False")

        # Verify ERROR event was emitted
        error_calls = [
            c for c in mock_emitter.emit_custom_event.call_args_list if "ERROR" in c[0][1]
        ]
        assert len(error_calls) == 1, "Should emit one ERROR event"

        error_call = error_calls[0]
        assert error_call[0][1] == "TestOp_ERROR", "Operation should have _ERROR suffix"
        assert "exception" in error_call[0][2], "ERROR event should include exception details"

    def test_span_reraises_exception_when_reraise_true(self, mock_emitter):
        """span should reraise exception when reraise=True."""
        trace = EventBasedSparkTrace(mock_emitter)

        with pytest.raises(ValueError, match="Test error"):
            with trace.span(operation="TestOp", component="TestComp", reraise=True):
                raise ValueError("Test error")

        # ERROR event should still be emitted
        error_calls = [
            c for c in mock_emitter.emit_custom_event.call_args_list if "ERROR" in c[0][1]
        ]
        assert len(error_calls) == 1, "Should emit ERROR event even when reraising"

    def test_span_does_not_reraise_when_reraise_false(self, mock_emitter):
        """span should swallow exception when reraise=False."""
        trace = EventBasedSparkTrace(mock_emitter)

        # Should not raise
        with trace.span(operation="TestOp", component="TestComp", reraise=False):
            raise ValueError("Test error")

        # Verify execution continued after exception
        calls = mock_emitter.emit_custom_event.call_args_list
        operations = [c[0][1] for c in calls]
        assert any("END" in op for op in operations), "Should emit END event even after exception"

    def test_span_emits_end_after_error(self, mock_emitter):
        """span should emit END event even after ERROR event."""
        trace = EventBasedSparkTrace(mock_emitter)

        with trace.span(operation="TestOp", component="TestComp", reraise=False):
            raise ValueError("Test error")

        operations = [c[0][1] for c in mock_emitter.emit_custom_event.call_args_list]
        assert "TestOp_START" in operations, "Should have START event"
        assert "TestOp_ERROR" in operations, "Should have ERROR event"
        assert "TestOp_END" in operations, "Should have END event"

        # Verify order: START, ERROR, END
        start_idx = operations.index("TestOp_START")
        error_idx = operations.index("TestOp_ERROR")
        end_idx = operations.index("TestOp_END")
        assert start_idx < error_idx < end_idx, "Events should be in order: START, ERROR, END"

    def test_span_generates_unique_span_ids(self, mock_emitter):
        """Each span should get a unique span ID."""
        trace = EventBasedSparkTrace(mock_emitter)

        with trace.span(operation="Op1", component="Comp1"):
            pass

        with trace.span(operation="Op2", component="Comp2"):
            pass

        with trace.span(operation="Op3", component="Comp3"):
            pass

        # Extract all span IDs
        calls = mock_emitter.emit_custom_event.call_args_list
        span_ids = [c[0][3] for c in calls]  # eventId parameter

        # Each span produces 2 events (START and END) with the same span ID
        # So we should have 3 unique span IDs appearing twice each
        unique_span_ids = set(span_ids)
        assert len(unique_span_ids) == 3, "Should have 3 unique span IDs"
        assert len(span_ids) == 6, "Should have 6 events total (2 per span)"

    def test_span_generates_trace_id_for_first_span(self, mock_emitter):
        """First span should generate a new trace ID."""
        trace = EventBasedSparkTrace(mock_emitter)

        with trace.span(operation="TestOp", component="TestComp"):
            pass

        calls = mock_emitter.emit_custom_event.call_args_list
        trace_id = calls[0][0][4]  # traceId parameter from first call

        assert isinstance(trace_id, uuid.UUID), "Trace ID should be a UUID"

    def test_nested_spans_share_trace_id(self, mock_emitter):
        """Nested spans should share the same trace ID."""
        trace = EventBasedSparkTrace(mock_emitter)

        with trace.span(operation="OuterOp", component="OuterComp"):
            outer_trace_id = trace.current_span.traceId

            with trace.span(operation="InnerOp", component="InnerComp"):
                inner_trace_id = trace.current_span.traceId

        assert outer_trace_id == inner_trace_id, "Nested spans should share trace ID"

        # Verify all emitted events have same trace ID
        calls = mock_emitter.emit_custom_event.call_args_list
        trace_ids = [c[0][4] for c in calls]
        assert len(set(trace_ids)) == 1, "All events should have same trace ID"

    def test_span_inherits_component_from_parent(self, mock_emitter):
        """Child span should inherit component from parent if not specified."""
        trace = EventBasedSparkTrace(mock_emitter)

        with trace.span(operation="OuterOp", component="ParentComp"):
            with trace.span(operation="InnerOp"):  # No component specified
                pass

        # Check events to see if component was inherited
        calls = mock_emitter.emit_custom_event.call_args_list
        inner_calls = [c for c in calls if "InnerOp" in c[0][1]]

        assert len(inner_calls) >= 2, "Should have events for inner span"
        assert inner_calls[0][0][0] == "ParentComp", "Should inherit component from parent"

    def test_span_inherits_operation_from_parent(self, mock_emitter):
        """Child span should inherit operation from parent if not specified."""
        trace = EventBasedSparkTrace(mock_emitter)

        with trace.span(operation="ParentOp", component="TestComp"):
            with trace.span(component="ChildComp"):  # No operation specified
                pass

        # Check that operation was inherited
        calls = mock_emitter.emit_custom_event.call_args_list
        child_calls = [c for c in calls if c[0][0] == "ChildComp"]

        assert len(child_calls) >= 2, "Should have events for child span"
        # Operation name from events should contain ParentOp
        assert any(
            "ParentOp" in c[0][1] for c in child_calls
        ), "Should inherit operation from parent"

    def test_span_calculates_timing_correctly(self, mock_emitter):
        """span should calculate elapsed time between start and end."""
        trace = EventBasedSparkTrace(mock_emitter)

        with trace.span(operation="TestOp", component="TestComp"):
            time.sleep(0.1)  # Sleep for ~100ms

        # Find END event
        calls = mock_emitter.emit_custom_event.call_args_list
        end_calls = [c for c in calls if "END" in c[0][1]]
        assert len(end_calls) >= 1, "Should have END event"

        end_details = end_calls[0][0][2]
        total_time = float(end_details["totalTime"])

        assert total_time >= 0.1, "Total time should be at least 100ms"
        assert total_time < 1.0, "Total time should be less than 1 second"

    def test_span_includes_custom_details(self, mock_emitter):
        """span should include custom details in events."""
        trace = EventBasedSparkTrace(mock_emitter)
        custom_details = {"user": "test_user", "batch": "batch_123"}

        with trace.span(operation="TestOp", component="TestComp", details=custom_details):
            pass

        # Verify START event includes custom details
        calls = mock_emitter.emit_custom_event.call_args_list
        start_call = [c for c in calls if "START" in c[0][1]][0]
        start_details = start_call[0][2]

        assert "user" in start_details, "Custom details should be in START event"
        assert start_details["user"] == "test_user", "Custom detail values should be preserved"

    def test_span_uses_mdc_context(self, mock_emitter, mock_spark_for_trace):
        """span should set up MDC context during execution."""
        trace = EventBasedSparkTrace(mock_emitter)

        with patch(
            "kindling.spark_trace.get_or_create_spark_session", return_value=mock_spark_for_trace
        ):
            with trace.span(operation="TestOp", component="TestComp"):
                # MDC should be set during span execution
                pass

        # Verify MDC was set
        mock_mdc = mock_spark_for_trace._jvm.org.apache.log4j.MDC
        put_calls = mock_mdc.put.call_args_list

        assert len(put_calls) >= 4, "Should set MDC for trace_id, span_id, component, operation"

        # Verify MDC keys
        mdc_keys = [c[0][0] for c in put_calls]
        assert "trace_id" in mdc_keys, "Should set trace_id in MDC"
        assert "span_id" in mdc_keys, "Should set span_id in MDC"
        assert "component" in mdc_keys, "Should set component in MDC"
        assert "operation" in mdc_keys, "Should set operation in MDC"


class TestSparkTraceProvider:
    """Test SparkTraceProvider ABC."""

    def test_spark_trace_provider_is_abstract(self):
        """SparkTraceProvider should be an abstract base class."""
        with pytest.raises(TypeError, match="Can't instantiate abstract class"):
            SparkTraceProvider()

    def test_spark_trace_provider_requires_span_method(self):
        """Subclasses must implement span method."""

        class IncompleteTrace(SparkTraceProvider):
            pass

        with pytest.raises(TypeError, match="Can't instantiate abstract class"):
            IncompleteTrace()


class TestEdgeCases:
    """Test edge cases and error handling."""

    def test_span_with_all_none_parameters(self, mock_emitter):
        """span should handle all None parameters gracefully."""
        trace = EventBasedSparkTrace(mock_emitter)

        # First span to establish trace
        with trace.span(operation="InitOp", component="InitComp"):
            # Nested span with all None params should inherit
            with trace.span():
                pass

        calls = mock_emitter.emit_custom_event.call_args_list
        assert len(calls) >= 4, "Should emit events for both spans"

    def test_span_with_empty_details_dict(self, mock_emitter):
        """span should handle empty details dict."""
        trace = EventBasedSparkTrace(mock_emitter)

        with trace.span(operation="TestOp", component="TestComp", details={}):
            pass

        calls = mock_emitter.emit_custom_event.call_args_list
        assert len(calls) >= 2, "Should emit START and END events"

    def test_multiple_sequential_spans(self, mock_emitter):
        """Multiple sequential spans should work correctly."""
        trace = EventBasedSparkTrace(mock_emitter)

        with trace.span(operation="Op1", component="Comp1"):
            pass

        with trace.span(operation="Op2", component="Comp2"):
            pass

        with trace.span(operation="Op3", component="Comp3"):
            pass

        calls = mock_emitter.emit_custom_event.call_args_list
        # Should have START and END for each span
        assert len(calls) >= 6, "Should have 2 events per span (3 spans)"

        start_calls = [c for c in calls if "START" in c[0][1]]
        end_calls = [c for c in calls if "END" in c[0][1]]
        assert len(start_calls) == 3, "Should have 3 START events"
        assert len(end_calls) == 3, "Should have 3 END events"

    def test_deeply_nested_spans(self, mock_emitter):
        """Deeply nested spans should maintain correct trace ID."""
        trace = EventBasedSparkTrace(mock_emitter)

        with trace.span(operation="Level1", component="Comp"):
            trace_id_1 = trace.current_span.traceId

            with trace.span(operation="Level2", component="Comp"):
                trace_id_2 = trace.current_span.traceId

                with trace.span(operation="Level3", component="Comp"):
                    trace_id_3 = trace.current_span.traceId

        assert trace_id_1 == trace_id_2 == trace_id_3, "All nested spans should share trace ID"

    def test_span_with_long_running_operation(self, mock_emitter):
        """span should handle longer operations correctly."""
        trace = EventBasedSparkTrace(mock_emitter)

        with trace.span(operation="LongOp", component="TestComp"):
            time.sleep(0.2)  # 200ms operation

        calls = mock_emitter.emit_custom_event.call_args_list
        end_call = [c for c in calls if "END" in c[0][1]][0]
        total_time = float(end_call[0][2]["totalTime"])

        assert total_time >= 0.2, "Should measure at least 200ms"

    def test_emitter_emit_called_with_correct_types(self, mock_emitter):
        """emit_custom_event should be called with correct parameter types."""
        trace = EventBasedSparkTrace(mock_emitter)

        with trace.span(operation="TestOp", component="TestComp", details={"key": "value"}):
            pass

        calls = mock_emitter.emit_custom_event.call_args_list
        for call_obj in calls:
            args = call_obj[0]
            assert isinstance(args[0], str), "Component should be string"
            assert isinstance(args[1], str), "Operation should be string"
            assert isinstance(args[2], dict), "Details should be dict"
            assert isinstance(args[3], str), "Event ID should be string"
            assert isinstance(args[4], uuid.UUID), "Trace ID should be UUID"
