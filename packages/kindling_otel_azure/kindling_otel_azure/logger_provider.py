"""Azure Monitor OpenTelemetry logger provider implementation."""

import logging
from typing import Optional

from injector import inject
from kindling.injection import GlobalInjector
from kindling.spark_config import ConfigService
from kindling.spark_log_provider import SparkLoggerProvider
from kindling.spark_session import get_or_create_spark_session
from opentelemetry import trace

from .config import AzureMonitorConfig


class AzureMonitorLogger:
    """Logger that sends to Azure Monitor via OpenTelemetry."""

    def __init__(self, name: str, logger: logging.Logger, tracer: Optional[trace.Tracer] = None):
        self.name = name
        self._logger = logger
        self._tracer = tracer

    def _add_trace_context(self, message: str) -> str:
        """Add trace context to log message if available."""
        if self._tracer:
            span = trace.get_current_span()
            if span and span.is_recording():
                ctx = span.get_span_context()
                return f"[trace_id={ctx.trace_id:032x} span_id={ctx.span_id:016x}] {message}"
        return message

    def _log_message(self, level: str, message: str, **kwargs):
        """Write to Python logger (goes to Azure Monitor via OpenTelemetry)"""
        contextual_message = self._add_trace_context(message)
        getattr(self._logger, level)(contextual_message, extra=kwargs)

    def debug(self, message: str, **kwargs):
        """Log debug message."""
        self._log_message("debug", message, **kwargs)

    def info(self, message: str, **kwargs):
        """Log info message."""
        self._log_message("info", message, **kwargs)

    def warn(self, message: str, **kwargs):
        """Log warning message."""
        self._log_message("warning", message, **kwargs)

    def warning(self, message: str, **kwargs):
        """Log warning message."""
        self.warn(message, **kwargs)

    def error(self, message: str, **kwargs):
        """Log error message."""
        self._log_message("error", message, **kwargs)

    def critical(self, message: str, **kwargs):
        """Log critical message."""
        self._log_message("critical", message, **kwargs)

    def exception(self, message: str, **kwargs):
        """Log exception with traceback."""
        contextual_message = self._add_trace_context(message)
        self._logger.exception(contextual_message, extra=kwargs)


class AzureMonitorLoggerProvider(SparkLoggerProvider):
    """Logger provider that uses Azure Monitor OpenTelemetry."""

    @inject
    def __init__(self, config: ConfigService):
        # Don't call super().__init__ to avoid double initialization
        self.config = config
        self._initialize_azure_monitor()

    def _initialize_azure_monitor(self):
        """Initialize Azure Monitor OpenTelemetry if configured."""
        if AzureMonitorConfig.is_configured():
            print("🔍 Azure Monitor already configured")
            return

        # Check if Azure Monitor is enabled
        enable_logging = self.config.get("kindling.telemetry.azure_monitor.enable_logging", False)
        enable_tracing = self.config.get("kindling.telemetry.azure_monitor.enable_tracing", False)

        print(f"🔍 Azure Monitor config check:")
        print(f"   enable_logging: {enable_logging}")
        print(f"   enable_tracing: {enable_tracing}")

        enabled = enable_logging or enable_tracing
        if not enabled:
            print("⚠️  Azure Monitor not enabled in config")
            return

        # Get connection string from config
        connection_string = self.config.get("kindling.telemetry.azure_monitor.connection_string")
        print(f"🔍 Connection string: {'SET' if connection_string else 'NOT SET'}")

        if not connection_string:
            print("⚠️  No Azure Monitor connection string in config")
            return

        # Initialize Azure Monitor (once for both logging and tracing)
        print(f"🚀 Initializing Azure Monitor with connection string...")
        AzureMonitorConfig.initialize(connection_string)
        print(f"✅ Azure Monitor initialized: {AzureMonitorConfig.is_configured()}")

    def get_logger(self, name: str, session=None):
        """Get logger instance."""
        # Get Python standard logger
        logger = logging.getLogger(name)

        # Set log level from config
        log_level = self.config.get("log_level", "INFO").upper()
        logger.setLevel(getattr(logging, log_level, logging.INFO))

        # Get tracer for trace context if available
        tracer = None
        tracer_provider = AzureMonitorConfig.get_tracer_provider()
        if tracer_provider:
            tracer = tracer_provider.get_tracer(name)

        return AzureMonitorLogger(name, logger, tracer)
