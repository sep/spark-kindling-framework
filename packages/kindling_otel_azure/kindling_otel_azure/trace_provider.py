"""Azure Monitor OpenTelemetry trace provider implementation."""

from contextlib import contextmanager
from datetime import datetime
from typing import Any, Dict, Optional

from injector import inject
from kindling.injection import GlobalInjector
from kindling.spark_config import ConfigService
from kindling.spark_trace import SparkSpan, SparkTraceProvider
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

    @staticmethod
    def _set_span_attributes(span, details: Optional[Dict[str, Any]]):
        """Set attributes on an OpenTelemetry span from details dict."""
        if not details:
            return
        for key, value in details.items():
            if isinstance(value, (str, int, float, bool)):
                span.set_attribute(key, value)
            else:
                span.set_attribute(key, str(value))

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
                self._set_span_attributes(span, details)

                yield self

            except Exception as e:
                # Apply any late-mutated attributes before closing the span.
                self._set_span_attributes(span, details)
                # Record exception in span
                span.record_exception(e)
                span.set_status(Status(StatusCode.ERROR, str(e)))

                if reraise:
                    raise
            else:
                # Apply any late-mutated attributes before marking success.
                self._set_span_attributes(span, details)
                # Mark span as successful
                span.set_status(Status(StatusCode.OK))

    def start_span(
        self,
        operation: str,
        component: str,
        details: dict = None,
    ) -> SparkSpan:
        """Start a manual span backed by an OTEL span when the tracer is available."""
        otel_span = None
        if self._tracer:
            span_name = (
                f"{component}.{operation}"
                if component and operation
                else (operation or component or "span")
            )
            otel_span = self._tracer.start_span(span_name)
            if component:
                otel_span.set_attribute("component", component)
            if operation:
                otel_span.set_attribute("operation", operation)
            self._set_span_attributes(otel_span, details)

        spark_span = SparkSpan(
            id=str(id(otel_span)) if otel_span else "noop",
            component=component or "",
            operation=operation or "",
            attributes=details or {},
            start_time=datetime.now(),
            reraise=False,
        )
        # Stash the OTEL span so add_event/end_span can reference it
        spark_span._otel_span = otel_span
        return spark_span

    def add_event(
        self,
        span: SparkSpan,
        name: str,
        attributes: dict = None,
    ) -> None:
        """Add a timestamped event to a manual span."""
        otel_span = getattr(span, "_otel_span", None)
        if otel_span:
            otel_attrs = {}
            if attributes:
                for k, v in attributes.items():
                    otel_attrs[k] = v if isinstance(v, (str, int, float, bool)) else str(v)
            otel_span.add_event(name, attributes=otel_attrs)

    def end_span(
        self,
        span: SparkSpan,
        error: Optional[str] = None,
    ) -> None:
        """End a manual span, optionally recording an error."""
        span.end_time = datetime.now()
        otel_span = getattr(span, "_otel_span", None)
        if otel_span:
            if error:
                otel_span.set_status(Status(StatusCode.ERROR, error))
                otel_span.add_event("error", attributes={"exception.message": error})
            else:
                otel_span.set_status(Status(StatusCode.OK))
            otel_span.end()
