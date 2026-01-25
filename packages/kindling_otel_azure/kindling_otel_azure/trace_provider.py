"""Azure Monitor OpenTelemetry trace provider implementation."""

from contextlib import contextmanager
from typing import Any, Dict, Optional

from injector import inject
from kindling.injection import GlobalInjector
from kindling.spark_config import ConfigService
from kindling.spark_trace import SparkTraceProvider
from opentelemetry import trace
from opentelemetry.trace import Status, StatusCode

from .config import AzureMonitorConfig


class AzureMonitorTraceProvider(SparkTraceProvider):
    """Trace provider that uses Azure Monitor OpenTelemetry."""

    @inject
    def __init__(self, config: ConfigService):
        self.config = config
        self._tracer = None
        self._initialize_azure_monitor()

    def _initialize_azure_monitor(self):
        """Initialize Azure Monitor OpenTelemetry if configured."""
        if AzureMonitorConfig.is_configured():
            print("🔍 Azure Monitor already configured (trace provider)")
            self._tracer = trace.get_tracer(__name__)
            return

        # Check if Azure Monitor is enabled
        enable_logging = self.config.get("kindling.telemetry.azure_monitor.enable_logging", False)
        enable_tracing = self.config.get("kindling.telemetry.azure_monitor.enable_tracing", False)

        print(f"🔍 Azure Monitor trace config check:")
        print(f"   enable_logging: {enable_logging}")
        print(f"   enable_tracing: {enable_tracing}")

        enabled = enable_logging or enable_tracing
        if not enabled:
            print("⚠️  Azure Monitor not enabled in config (trace provider)")
            return

        # Get connection string from config
        connection_string = self.config.get("kindling.telemetry.azure_monitor.connection_string")
        print(f"🔍 Trace - Connection string: {'SET' if connection_string else 'NOT SET'}")

        if not connection_string:
            print("⚠️  No Azure Monitor connection string in config (trace provider)")
            return

        # Initialize Azure Monitor (once for both logging and tracing)
        print(f"🚀 Trace provider initializing Azure Monitor...")
        AzureMonitorConfig.initialize(connection_string)

        # Get tracer if configuration succeeded
        if AzureMonitorConfig.is_configured():
            self._tracer = trace.get_tracer(__name__)
            print(
                f"✅ Trace provider: Azure Monitor initialized, tracer: {self._tracer is not None}"
            )

    def get_tracer(self, name: str):
        """Get OpenTelemetry tracer instance.

        Args:
            name: Name for the tracer

        Returns:
            OpenTelemetry Tracer instance
        """
        if not self._tracer:
            return trace.get_tracer(name)
        return self._tracer

    @contextmanager
    def span(
        self,
        operation: str = None,
        component: str = None,
        details: dict = None,
        reraise: bool = False,
    ):
        """Create a tracing span.

        Args:
            operation: Operation name (e.g., "process_data")
            component: Component name (e.g., "data_pipeline")
            details: Additional attributes to attach to span
            reraise: Whether to reraise exceptions after recording them
        """
        if not self._tracer:
            # Azure Monitor not configured, just yield without tracing
            yield self
            return

        # Construct span name from component and operation
        span_name = (
            f"{component}.{operation}"
            if component and operation
            else (operation or component or "span")
        )

        # Start span
        with self._tracer.start_as_current_span(span_name) as span:
            try:
                # Add component as attribute if provided
                if component:
                    span.set_attribute("component", component)

                # Add operation as attribute if provided
                if operation:
                    span.set_attribute("operation", operation)

                # Add custom attributes from details
                if details:
                    for key, value in details.items():
                        # Convert value to string if not a basic type
                        if isinstance(value, (str, int, float, bool)):
                            span.set_attribute(key, value)
                        else:
                            span.set_attribute(key, str(value))

                yield self

            except Exception as e:
                # Record exception in span
                span.record_exception(e)
                span.set_status(Status(StatusCode.ERROR, str(e)))

                if reraise:
                    raise
            else:
                # Mark span as successful
                span.set_status(Status(StatusCode.OK))
