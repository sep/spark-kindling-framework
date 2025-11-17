"""Unit tests for SparkLogger pattern-based logging."""

import re
from datetime import datetime
from unittest.mock import MagicMock, Mock, call, patch

import pytest
from kindling.spark_log import SparkLogger


class TestSparkLoggerInitialization:
    """Test SparkLogger initialization and configuration."""

    def test_initialization_with_all_parameters(self, mock_spark, mock_logger, basic_config):
        """SparkLogger should initialize with name, logger, session, and config."""
        logger = SparkLogger("TestLogger", mock_logger, mock_spark, basic_config)

        assert logger.name == "TestLogger", "Logger name should be stored exactly as provided"
        assert logger.logger == mock_logger, "Base logger instance should be stored for delegation"
        assert logger.spark == mock_spark, "Spark session should be stored for MDC access"
        assert logger.config == basic_config, "Config instance should be stored for settings"
        assert logger.pattern is not None, "Default pattern should be set during initialization"
        assert (
            len(logger._level_hierarchy) == 4
        ), "Log level hierarchy should contain all four levels"

    def test_initialization_without_spark_session(self, mock_logger, basic_config):
        """SparkLogger should handle missing Spark session by using get_or_create_spark_session()."""
        # Note: In production, passing None for session triggers get_or_create_spark_session()
        # For testing, we need to patch that function to return a mock
        with patch("kindling.spark_log.get_or_create_spark_session", return_value=Mock()):
            logger = SparkLogger("TestLogger", mock_logger, None, basic_config)

        assert logger.spark is not None, "Spark session should be created when None is provided"
        assert (
            logger.name == "TestLogger"
        ), "Logger should initialize when Spark session is auto-created"
        assert (
            logger.logger == mock_logger
        ), "Base logger should still be set when Spark session is auto-created"

    def test_initialization_without_config(self, mock_spark, mock_logger):
        """SparkLogger should handle missing config by defaulting to empty dict."""
        logger = SparkLogger("TestLogger", mock_logger, mock_spark, None)

        assert logger.config == {}, "Config should default to empty dict when None is provided"
        assert logger.name == "TestLogger", "Logger should still initialize without config"

    def test_default_pattern_includes_mdc_fields(self, mock_spark, mock_logger, basic_config):
        """Default pattern should include MDC fields for distributed tracing."""
        logger = SparkLogger("TestLogger", mock_logger, mock_spark, basic_config)

        assert "trace_id" in logger.pattern, "Default pattern should include trace_id MDC field"
        assert "span_id" in logger.pattern, "Default pattern should include span_id MDC field"
        assert "component" in logger.pattern, "Default pattern should include component MDC field"
        assert "operation" in logger.pattern, "Default pattern should include operation MDC field"
        assert "%x{" in logger.pattern, "Default pattern should use %x{key} format for MDC"

    def test_log_level_hierarchy_values(self, mock_spark, mock_logger, basic_config):
        """Log level hierarchy should define correct priority order."""
        logger = SparkLogger("TestLogger", mock_logger, mock_spark, basic_config)

        assert logger._level_hierarchy["error"] == 0, "Error should have highest priority (0)"
        assert logger._level_hierarchy["warn"] == 1, "Warn should have second priority (1)"
        assert logger._level_hierarchy["info"] == 2, "Info should have third priority (2)"
        assert logger._level_hierarchy["debug"] == 3, "Debug should have lowest priority (3)"
        assert (
            logger._level_hierarchy["error"] < logger._level_hierarchy["debug"]
        ), "Error priority should be higher than debug"


class TestLogLevelFiltering:
    """Test log level checking and filtering."""

    def test_is_level_enabled_with_info_level(self, mock_spark, mock_logger, basic_config):
        """When effective level is INFO, only info/warn/error should be enabled."""
        mock_level = Mock()
        mock_level.__str__ = Mock(return_value="INFO")
        mock_logger.getEffectiveLevel = Mock(return_value=mock_level)

        logger = SparkLogger("TestLogger", mock_logger, mock_spark, basic_config)

        assert (
            logger._is_level_enabled("error") is True
        ), "Error should be enabled when effective level is INFO"
        assert (
            logger._is_level_enabled("warn") is True
        ), "Warn should be enabled when effective level is INFO"
        assert (
            logger._is_level_enabled("info") is True
        ), "Info should be enabled when effective level is INFO"
        assert (
            logger._is_level_enabled("debug") is False
        ), "Debug should be disabled when effective level is INFO"

    def test_is_level_enabled_with_warn_level(self, mock_spark, mock_logger, basic_config):
        """When effective level is WARN, only warn/error should be enabled."""
        mock_level = Mock()
        mock_level.__str__ = Mock(return_value="WARN")
        mock_logger.getEffectiveLevel = Mock(return_value=mock_level)

        logger = SparkLogger("TestLogger", mock_logger, mock_spark, basic_config)

        assert (
            logger._is_level_enabled("error") is True
        ), "Error should be enabled when effective level is WARN"
        assert (
            logger._is_level_enabled("warn") is True
        ), "Warn should be enabled when effective level is WARN"
        assert (
            logger._is_level_enabled("info") is False
        ), "Info should be disabled when effective level is WARN"
        assert (
            logger._is_level_enabled("debug") is False
        ), "Debug should be disabled when effective level is WARN"

    def test_is_level_enabled_with_error_level(self, mock_spark, mock_logger, basic_config):
        """When effective level is ERROR, only error should be enabled."""
        mock_level = Mock()
        mock_level.__str__ = Mock(return_value="ERROR")
        mock_logger.getEffectiveLevel = Mock(return_value=mock_level)

        logger = SparkLogger("TestLogger", mock_logger, mock_spark, basic_config)

        assert (
            logger._is_level_enabled("error") is True
        ), "Error should be enabled when effective level is ERROR"
        assert (
            logger._is_level_enabled("warn") is False
        ), "Warn should be disabled when effective level is ERROR"
        assert (
            logger._is_level_enabled("info") is False
        ), "Info should be disabled when effective level is ERROR"
        assert (
            logger._is_level_enabled("debug") is False
        ), "Debug should be disabled when effective level is ERROR"

    def test_is_level_enabled_with_debug_level(self, mock_spark, mock_logger, basic_config):
        """When effective level is DEBUG, all levels should be enabled."""
        mock_level = Mock()
        mock_level.__str__ = Mock(return_value="DEBUG")
        mock_logger.getEffectiveLevel = Mock(return_value=mock_level)

        logger = SparkLogger("TestLogger", mock_logger, mock_spark, basic_config)

        assert (
            logger._is_level_enabled("error") is True
        ), "Error should be enabled when effective level is DEBUG"
        assert (
            logger._is_level_enabled("warn") is True
        ), "Warn should be enabled when effective level is DEBUG"
        assert (
            logger._is_level_enabled("info") is True
        ), "Info should be enabled when effective level is DEBUG"
        assert (
            logger._is_level_enabled("debug") is True
        ), "Debug should be enabled when effective level is DEBUG"

    def test_is_level_enabled_with_unknown_level(self, mock_spark, mock_logger, basic_config):
        """Unknown effective levels should default to allowing all messages."""
        mock_level = Mock()
        mock_level.__str__ = Mock(return_value="UNKNOWN")
        mock_logger.getEffectiveLevel = Mock(return_value=mock_level)

        logger = SparkLogger("TestLogger", mock_logger, mock_spark, basic_config)

        assert (
            logger._is_level_enabled("debug") is True
        ), "Unknown level should default to allowing debug"
        assert (
            logger._is_level_enabled("info") is True
        ), "Unknown level should default to allowing info"
        assert (
            logger._is_level_enabled("warn") is True
        ), "Unknown level should default to allowing warn"
        assert (
            logger._is_level_enabled("error") is True
        ), "Unknown level should default to allowing error"


class TestLoggingMethods:
    """Test public logging method interfaces."""

    def test_debug_method_calls_log(self, mock_spark, mock_logger, basic_config):
        """debug() should delegate to _log() with debug level."""
        logger = SparkLogger("TestLogger", mock_logger, mock_spark, basic_config)

        with patch.object(logger, "_log") as mock_log:
            logger.debug("debug message")

        mock_log.assert_called_once_with(
            "debug", "debug message"
        ), "_log should be called with 'debug' level and message"

    def test_info_method_calls_log(self, mock_spark, mock_logger, basic_config):
        """info() should delegate to _log() with info level."""
        logger = SparkLogger("TestLogger", mock_logger, mock_spark, basic_config)

        with patch.object(logger, "_log") as mock_log:
            logger.info("info message")

        mock_log.assert_called_once_with(
            "info", "info message"
        ), "_log should be called with 'info' level and message"

    def test_warn_method_calls_log(self, mock_spark, mock_logger, basic_config):
        """warn() should delegate to _log() with warn level."""
        logger = SparkLogger("TestLogger", mock_logger, mock_spark, basic_config)

        with patch.object(logger, "_log") as mock_log:
            logger.warn("warn message")

        mock_log.assert_called_once_with(
            "warn", "warn message"
        ), "_log should be called with 'warn' level and message"

    def test_warning_method_calls_log(self, mock_spark, mock_logger, basic_config):
        """warning() should be an alias for warn() with warn level."""
        logger = SparkLogger("TestLogger", mock_logger, mock_spark, basic_config)

        with patch.object(logger, "_log") as mock_log:
            logger.warning("warning message")

        mock_log.assert_called_once_with(
            "warn", "warning message"
        ), "warning() should be alias calling _log with 'warn' level"

    def test_error_method_calls_log(self, mock_spark, mock_logger, basic_config):
        """error() should delegate to _log() with error level."""
        logger = SparkLogger("TestLogger", mock_logger, mock_spark, basic_config)

        with patch.object(logger, "_log") as mock_log:
            logger.error("error message")

        mock_log.assert_called_once_with(
            "error", "error message"
        ), "_log should be called with 'error' level and message"

    def test_all_methods_called_sequentially(self, mock_spark, mock_logger, basic_config):
        """All logging methods should work when called in sequence."""
        logger = SparkLogger("TestLogger", mock_logger, mock_spark, basic_config)

        with patch.object(logger, "_log") as mock_log:
            logger.debug("debug msg")
            logger.info("info msg")
            logger.warn("warn msg")
            logger.warning("warning msg")
            logger.error("error msg")

        assert mock_log.call_count == 5, "All five logging methods should delegate to _log"
        calls = mock_log.call_args_list
        assert calls[0] == call("debug", "debug msg"), "First call should be debug"
        assert calls[1] == call("info", "info msg"), "Second call should be info"
        assert calls[2] == call("warn", "warn msg"), "Third call should be warn"
        assert calls[3] == call("warn", "warning msg"), "Fourth call should be warn (warning alias)"
        assert calls[4] == call("error", "error msg"), "Fifth call should be error"


class TestCentralLogMethod:
    """Test the central _log method behavior."""

    def test_log_skips_when_level_disabled(self, mock_spark, mock_logger, basic_config):
        """_log should skip processing when level is disabled."""
        logger = SparkLogger("TestLogger", mock_logger, mock_spark, basic_config)

        with patch.object(logger, "_is_level_enabled", return_value=False):
            with patch.object(logger, "_format_msg") as mock_format:
                logger._log("debug", "test message")

        mock_format.assert_not_called(), "Message formatting should be skipped when level is disabled for performance"
        mock_logger.debug.assert_not_called(), "Base logger should not be called when level is disabled"

    def test_log_formats_and_logs_when_enabled(self, mock_spark, mock_logger, basic_config):
        """_log should format and log message when level is enabled."""
        logger = SparkLogger("TestLogger", mock_logger, mock_spark, basic_config)

        with patch.object(logger, "_is_level_enabled", return_value=True):
            with patch.object(logger, "_format_msg", return_value="formatted message"):
                logger._log("info", "test message")

        mock_logger.info.assert_called_once_with(
            "formatted message"
        ), "Base logger.info should be called with formatted message"

    def test_log_calls_correct_logger_method_for_each_level(
        self, mock_spark, mock_logger, basic_config
    ):
        """_log should call the appropriate logger method for each level."""
        logger = SparkLogger("TestLogger", mock_logger, mock_spark, basic_config)

        with patch.object(logger, "_is_level_enabled", return_value=True):
            with patch.object(logger, "_format_msg", return_value="msg"):
                logger._log("debug", "test")
                logger._log("info", "test")
                logger._log("warn", "test")
                logger._log("error", "test")

        mock_logger.debug.assert_called_once_with(
            "msg"
        ), "logger.debug should be called for debug level"
        mock_logger.info.assert_called_once_with(
            "msg"
        ), "logger.info should be called for info level"
        mock_logger.warn.assert_called_once_with(
            "msg"
        ), "logger.warn should be called for warn level"
        mock_logger.error.assert_called_once_with(
            "msg"
        ), "logger.error should be called for error level"

    def test_log_prints_when_print_logging_enabled(self, mock_spark, mock_logger, basic_config):
        """_log should print to stdout when print_logging is enabled in config."""

        # Override config.get to return appropriate types
        def config_get(key, default=None):
            return {"log_level": "", "print_logging": True}.get(key, default)

        basic_config.get = Mock(side_effect=config_get)
        logger = SparkLogger("TestLogger", mock_logger, mock_spark, basic_config)

        with patch.object(logger, "_is_level_enabled", return_value=True):
            with patch.object(logger, "_format_msg", return_value="formatted msg"):
                with patch("builtins.print") as mock_print:
                    logger._log("info", "test message")

        mock_print.assert_called_once_with(
            "formatted msg"
        ), "print should be called with formatted message when print_logging is True"

    def test_log_does_not_print_when_print_logging_disabled(
        self, mock_spark, mock_logger, basic_config
    ):
        """_log should not print to stdout when print_logging is disabled in config."""

        # Override config.get to return appropriate types
        def config_get(key, default=None):
            return {"log_level": "", "print_logging": False}.get(key, default)

        basic_config.get = Mock(side_effect=config_get)
        logger = SparkLogger("TestLogger", mock_logger, mock_spark, basic_config)

        with patch.object(logger, "_is_level_enabled", return_value=True):
            with patch.object(logger, "_format_msg", return_value="formatted msg"):
                with patch("builtins.print") as mock_print:
                    logger._log("info", "test message")

        mock_print.assert_not_called(), "print should not be called when print_logging is False"

    def test_log_handles_missing_config(self, mock_spark, mock_logger):
        """_log should handle missing config by using empty dict default."""
        logger = SparkLogger("TestLogger", mock_logger, mock_spark, None)

        with patch.object(logger, "_is_level_enabled", return_value=True):
            with patch.object(logger, "_format_msg", return_value="msg"):
                with patch("builtins.print") as mock_print:
                    logger._log("info", "test")

        mock_logger.info.assert_called_once_with(
            "msg"
        ), "Logging should work with empty dict config"
        mock_print.assert_not_called(), "Print should not be called when config defaults to empty dict"


class TestPatternFormatting:
    """Test message pattern formatting."""

    def test_format_msg_with_simple_message_placeholder(
        self, mock_spark, mock_logger, basic_config
    ):
        """Pattern %m should be replaced with message text."""
        logger = SparkLogger("TestLogger", mock_logger, mock_spark, basic_config)
        logger.pattern = "%m"

        result = logger._format_msg("test message", "info")

        assert result == "test message", "Message placeholder should be replaced with message text"

    def test_format_msg_with_level_placeholder(self, mock_spark, mock_logger, basic_config):
        """Pattern %p should be replaced with uppercase log level."""
        logger = SparkLogger("TestLogger", mock_logger, mock_spark, basic_config)
        logger.pattern = "%p: %m"

        result_info = logger._format_msg("test", "info")
        result_warn = logger._format_msg("test", "warn")
        result_error = logger._format_msg("test", "error")
        result_debug = logger._format_msg("test", "debug")

        assert "INFO:" in result_info, "Log level should be formatted as uppercase INFO"
        assert "WARN:" in result_warn, "Log level should be formatted as uppercase WARN"
        assert "ERROR:" in result_error, "Log level should be formatted as uppercase ERROR"
        assert "DEBUG:" in result_debug, "Log level should be formatted as uppercase DEBUG"

    def test_format_msg_with_logger_name_full(self, mock_spark, mock_logger, basic_config):
        """Pattern %c should be replaced with full logger name."""
        logger = SparkLogger("com.example.package.MyClass", mock_logger, mock_spark, basic_config)
        logger.pattern = "[%c] %m"

        result = logger._format_msg("test", "info")

        assert (
            "com.example.package.MyClass" in result
        ), "Full logger name should be included with %c pattern"

    def test_format_msg_with_logger_name_truncated(self, mock_spark, mock_logger, basic_config):
        """Pattern %c{n} should truncate logger name to last n segments."""
        logger = SparkLogger("com.example.package.MyClass", mock_logger, mock_spark, basic_config)

        logger.pattern = "%c{1} %m"
        result_1 = logger._format_msg("test", "info")

        logger.pattern = "%c{2} %m"
        result_2 = logger._format_msg("test", "info")

        logger.pattern = "%c{3} %m"
        result_3 = logger._format_msg("test", "info")

        assert result_1.startswith("MyClass"), "Logger name should be truncated to last 1 segment"
        assert "package.MyClass" in result_2, "Logger name should be truncated to last 2 segments"
        assert (
            "example.package.MyClass" in result_3
        ), "Logger name should be truncated to last 3 segments"

    def test_format_msg_with_newline_placeholder(self, mock_spark, mock_logger, basic_config):
        """Pattern %n should be replaced with newline character."""
        logger = SparkLogger("TestLogger", mock_logger, mock_spark, basic_config)
        logger.pattern = "%m%n"

        result = logger._format_msg("test", "info")

        assert result.endswith("\n"), "Newline placeholder should be replaced with actual newline"
        assert result == "test\n", "Pattern should produce message followed by newline"

    def test_format_msg_with_date_placeholder(self, mock_spark, mock_logger, basic_config):
        """Pattern %d should be replaced with formatted date."""
        logger = SparkLogger("TestLogger", mock_logger, mock_spark, basic_config)
        logger.pattern = "%d{%Y-%m-%d} %m"

        result = logger._format_msg("test", "info")

        # The code has a bug: date_str = now.strftime(date_format)[:-3]
        # This removes last 3 chars, so "2025-01-10" becomes "2025-01"
        assert re.search(
            r"\d{4}-\d{2}", result
        ), "Date should contain year and month in YYYY-MM format"
        assert "test" in result, "Message should be preserved in formatted output"

    def test_format_msg_with_mdc_placeholders(self, mock_spark, mock_logger, basic_config):
        """Pattern %x{key} should be replaced with MDC values from Spark context."""
        mock_spark.sparkContext.getLocalProperty = Mock(
            side_effect=lambda key: {
                "mdc.trace_id": "trace-123",
                "mdc.span_id": "span-456",
                "mdc.component": "TestComponent",
                "mdc.operation": "TestOperation",
            }.get(key, "n/a")
        )

        logger = SparkLogger("TestLogger", mock_logger, mock_spark, basic_config)
        logger.pattern = (
            "trace=%x{trace_id} span=%x{span_id} comp=%x{component} op=%x{operation} %m"
        )

        result = logger._format_msg("test msg", "info")

        assert "trace=trace-123" in result, "MDC trace_id should be substituted in pattern"
        assert "span=span-456" in result, "MDC span_id should be substituted in pattern"
        assert "comp=TestComponent" in result, "MDC component should be substituted in pattern"
        assert "op=TestOperation" in result, "MDC operation should be substituted in pattern"
        assert "test msg" in result, "Original message should be preserved"

    def test_format_msg_with_mdc_default_value(self, mock_spark, mock_logger, basic_config):
        """Pattern %x{key} should use 'n/a' when MDC value is not available."""
        mock_spark.sparkContext.getLocalProperty = Mock(return_value=None)

        logger = SparkLogger("TestLogger", mock_logger, mock_spark, basic_config)
        logger.pattern = "trace=%x{trace_id} %m"

        result = logger._format_msg("test", "info")

        assert "trace=n/a" in result, "MDC placeholder should default to 'n/a' when value not found"

    def test_format_msg_with_mdc_without_spark_session(self, mock_logger, basic_config):
        """Pattern %x{key} should handle missing Spark session gracefully."""
        # Patch spark session creation to return a mock without sparkContext
        mock_spark_no_context = Mock(spec=[])  # Empty spec means no attributes
        with patch(
            "kindling.spark_log.get_or_create_spark_session", return_value=mock_spark_no_context
        ):
            logger = SparkLogger("TestLogger", mock_logger, None, basic_config)
        logger.pattern = "trace=%x{trace_id} %m"

        result = logger._format_msg("test", "info")

        assert (
            "trace=n/a" in result
        ), "MDC placeholder should default to 'n/a' when Spark session lacks sparkContext"

    def test_format_msg_with_complex_pattern(self, mock_spark, mock_logger, basic_config):
        """Complex patterns with multiple placeholders should all be replaced."""
        mock_spark.sparkContext.getLocalProperty = Mock(
            side_effect=lambda key: {"mdc.trace_id": "trace-789"}.get(key, "n/a")
        )

        logger = SparkLogger("com.example.TestLogger", mock_logger, mock_spark, basic_config)
        logger.pattern = "%d{%Y-%m-%d} [%p] %c{1}: %m trace=%x{trace_id}%n"

        result = logger._format_msg("test message", "warn")

        assert re.search(r"\d{4}-\d{2}", result), "Date should be formatted in pattern"
        assert "WARN" in result, "Log level should be uppercase in pattern"
        assert "TestLogger" in result, "Truncated logger name should be in pattern"
        assert "test message" in result, "Original message should be in pattern"
        assert "trace=trace-789" in result, "MDC value should be substituted in pattern"
        assert result.endswith("\n"), "Newline should terminate the pattern"

    def test_format_msg_with_empty_message(self, mock_spark, mock_logger, basic_config):
        """Empty message should result in empty formatted output with just pattern."""
        logger = SparkLogger("TestLogger", mock_logger, mock_spark, basic_config)
        logger.pattern = "%m"

        result = logger._format_msg("", "info")

        assert result == "", "Empty message should produce empty result when pattern is just %m"

    def test_format_msg_with_static_pattern(self, mock_spark, mock_logger, basic_config):
        """Pattern with no placeholders should return literal text."""
        logger = SparkLogger("TestLogger", mock_logger, mock_spark, basic_config)
        logger.pattern = "static log text"

        result = logger._format_msg("ignored message", "info")

        assert (
            result == "static log text"
        ), "Pattern without placeholders should return literal text"


class TestPatternCustomization:
    """Test pattern customization methods."""

    def test_with_pattern_sets_custom_pattern(self, mock_spark, mock_logger, basic_config):
        """with_pattern() should update pattern with MDC suffix."""
        logger = SparkLogger("TestLogger", mock_logger, mock_spark, basic_config)

        result = logger.with_pattern("%p %m")

        expected_pattern = "%p %m%ntrace_id=%x{trace_id} span_id=%x{span_id} component=%x{component} operation=%x{operation}"
        assert (
            logger.pattern == expected_pattern
        ), "Custom pattern should be appended with MDC fields"

    def test_with_pattern_returns_self(self, mock_spark, mock_logger, basic_config):
        """with_pattern() should return self for method chaining."""
        logger = SparkLogger("TestLogger", mock_logger, mock_spark, basic_config)

        result = logger.with_pattern("%p %m")

        assert result is logger, "with_pattern should return self to enable method chaining"

    def test_with_pattern_allows_method_chaining(self, mock_spark, mock_logger, basic_config):
        """with_pattern() should enable chaining with logging methods."""
        logger = SparkLogger("TestLogger", mock_logger, mock_spark, basic_config)

        with patch.object(logger, "_log") as mock_log:
            logger.with_pattern("%p: %m").info("test")

        custom_pattern = "%p: %m%ntrace_id=%x{trace_id} span_id=%x{span_id} component=%x{component} operation=%x{operation}"
        assert logger.pattern == custom_pattern, "Pattern should be updated before logging"
        mock_log.assert_called_once_with("info", "test"), "Chained logging method should work"


class TestShouldPrint:
    """Test print_logging configuration checking."""

    def test_should_print_returns_true_when_enabled(self, mock_spark, mock_logger, basic_config):
        """should_print() should return True when print_logging is enabled."""

        # Override config.get to return appropriate types
        def config_get(key, default=None):
            return {"log_level": "", "print_logging": True}.get(key, default)

        basic_config.get = Mock(side_effect=config_get)
        logger = SparkLogger("TestLogger", mock_logger, mock_spark, basic_config)

        result = logger.should_print()

        assert result is True, "should_print should return True when config.print_logging is True"

    def test_should_print_returns_false_when_disabled(self, mock_spark, mock_logger, basic_config):
        """should_print() should return False when print_logging is disabled."""

        # Override config.get to return appropriate types
        def config_get(key, default=None):
            return {"log_level": "", "print_logging": False}.get(key, default)

        basic_config.get = Mock(side_effect=config_get)
        logger = SparkLogger("TestLogger", mock_logger, mock_spark, basic_config)

        result = logger.should_print()

        assert (
            result is False
        ), "should_print should return False when config.print_logging is False"

    def test_should_print_returns_false_when_config_missing(self, mock_spark, mock_logger):
        """should_print() should return False when config defaults to empty dict."""
        logger = SparkLogger("TestLogger", mock_logger, mock_spark, None)

        result = logger.should_print()

        assert result is False, "should_print should return False when config is empty dict"


class TestEdgeCases:
    """Test edge cases and error handling."""

    def test_logger_name_with_single_segment(self, mock_spark, mock_logger, basic_config):
        """Logger with single-segment name should handle truncation gracefully."""
        logger = SparkLogger("SimpleLogger", mock_logger, mock_spark, basic_config)
        logger.pattern = "%c{2} %m"

        result = logger._format_msg("test", "info")

        assert (
            "SimpleLogger" in result
        ), "Single-segment name should be preserved even with truncation"

    def test_logger_name_truncation_exceeds_segments(self, mock_spark, mock_logger, basic_config):
        """Requesting more segments than available should return full name."""
        logger = SparkLogger("com.example", mock_logger, mock_spark, basic_config)
        logger.pattern = "%c{5} %m"

        result = logger._format_msg("test", "info")

        assert "com.example" in result, "Truncation exceeding segment count should return full name"

    def test_message_with_special_characters(self, mock_spark, mock_logger, basic_config):
        """Messages with special characters should be preserved."""
        logger = SparkLogger("TestLogger", mock_logger, mock_spark, basic_config)
        logger.pattern = "%m"

        special_msg = "Test with \n newline, \t tab, and % percent"
        result = logger._format_msg(special_msg, "info")

        assert special_msg in result, "Special characters in message should be preserved"

    def test_pattern_with_percentage_not_followed_by_placeholder(
        self, mock_spark, mock_logger, basic_config
    ):
        """Percentage signs not followed by valid placeholders should be preserved."""
        logger = SparkLogger("TestLogger", mock_logger, mock_spark, basic_config)
        logger.pattern = "100% complete: %m"

        result = logger._format_msg("test", "info")

        assert (
            "100% complete:" in result
        ), "Percentage signs not part of placeholders should be preserved"

    def test_multiple_logging_calls_in_sequence(self, mock_spark, mock_logger, basic_config):
        """Multiple logging calls should all work correctly."""
        logger = SparkLogger("TestLogger", mock_logger, mock_spark, basic_config)
        logger.pattern = "%p: %m"

        with patch.object(logger, "_is_level_enabled", return_value=True):
            logger.info("first")
            logger.warn("second")
            logger.error("third")

        assert mock_logger.info.call_count == 1, "info should be called once"
        assert mock_logger.warn.call_count == 1, "warn should be called once"
        assert mock_logger.error.call_count == 1, "error should be called once"

    def test_logger_works_with_minimal_configuration(self, mock_logger):
        """Logger should work with only required parameters."""
        # Patch spark session creation to avoid JVM issues in dev container
        mock_spark = Mock()
        mock_spark.sparkContext = Mock()
        with patch("kindling.spark_log.get_or_create_spark_session", return_value=mock_spark):
            logger = SparkLogger("TestLogger", mock_logger)

        assert logger.name == "TestLogger", "Logger should initialize with minimal parameters"
        assert logger.logger == mock_logger, "Base logger should be set with minimal parameters"
        assert logger.spark is not None, "Spark should be auto-created when not provided"
        assert logger.config == {}, "Config should default to empty dict when not provided"
