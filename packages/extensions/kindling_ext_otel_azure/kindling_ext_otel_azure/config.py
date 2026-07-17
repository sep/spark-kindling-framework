"""Shared Azure Monitor OpenTelemetry configuration."""

from typing import Optional

from azure.monitor.opentelemetry import configure_azure_monitor
from opentelemetry import trace


class AzureMonitorConfig:
    """Singleton for Azure Monitor OpenTelemetry configuration."""

    _configured = False
    _tracer_provider = None
    _error: Optional[Exception] = None

    @classmethod
    def initialize(cls, connection_string: str, **kwargs):
        """Initialize Azure Monitor OpenTelemetry.

        Args:
            connection_string: Application Insights connection string
            **kwargs: Additional arguments to pass to configure_azure_monitor
        """
        if cls._configured:
            return

        if not connection_string:
            return

        try:
            # Configure Azure Monitor - sets up both logging and tracing
            configure_azure_monitor(connection_string=connection_string, **kwargs)

            # Get tracer provider for later use
            cls._tracer_provider = trace.get_tracer_provider()
            cls._configured = True

        except Exception as e:
            cls._error = e
            # Don't raise - allow graceful fallback
            print(f"Failed to configure Azure Monitor OpenTelemetry: {e}")

    @classmethod
    def is_configured(cls) -> bool:
        """Check if Azure Monitor is configured."""
        return cls._configured

    @classmethod
    def get_tracer_provider(cls):
        """Get the tracer provider if configured."""
        return cls._tracer_provider

    @classmethod
    def get_error(cls) -> Optional[Exception]:
        """Get configuration error if any."""
        return cls._error

    @classmethod
    def force_flush(cls, timeout_millis: int = 30000) -> bool:
        """Force flush all pending spans.

        Args:
            timeout_millis: Maximum time to wait for flush in milliseconds

        Returns:
            True if flush succeeded within timeout, False otherwise
        """
        if cls._tracer_provider:
            return cls._tracer_provider.force_flush(timeout_millis)
        return True

    @classmethod
    def shutdown(cls):
        """Shutdown the tracer provider and ensure all spans are exported."""
        if cls._tracer_provider:
            cls._tracer_provider.shutdown()
