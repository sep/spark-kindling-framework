"""Unit tests for AzureMonitorTraceProvider (kindling_ext_otel_azure).

Skipped when the opentelemetry API is not importable (the extension's test
environment installs it; the core unit environment may not).

The extension package ``__init__`` force-rebinds the GlobalInjector telemetry
providers at import time, which must not leak into the unit suite. The
fixture below loads ``trace_provider`` as a package submodule without
executing the package ``__init__``, and stubs ``azure.monitor.opentelemetry``
when absent — only ``AzureMonitorConfig.initialize`` touches it, and these
tests never call that.
"""

import importlib
import sys
import types
import uuid
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

opentelemetry = pytest.importorskip("opentelemetry")
from opentelemetry.trace import StatusCode  # noqa: E402

EXTENSION_PACKAGE_ROOT = (
    Path(__file__).resolve().parents[2] / "packages" / "extensions" / "kindling_ext_otel_azure"
)

_PACKAGE_NAME = "kindling_ext_otel_azure"


@pytest.fixture(scope="module")
def trace_provider_module():
    """Import kindling_ext_otel_azure.trace_provider without package side effects."""
    if "azure.monitor.opentelemetry" not in sys.modules:
        try:
            importlib.import_module("azure.monitor.opentelemetry")
        except ImportError:
            azure_pkg = sys.modules.setdefault("azure", types.ModuleType("azure"))
            monitor_pkg = types.ModuleType("azure.monitor")
            stub = types.ModuleType("azure.monitor.opentelemetry")
            stub.configure_azure_monitor = lambda **kwargs: None
            azure_pkg.monitor = monitor_pkg
            monitor_pkg.opentelemetry = stub
            sys.modules["azure.monitor"] = monitor_pkg
            sys.modules["azure.monitor.opentelemetry"] = stub

    if _PACKAGE_NAME not in sys.modules:
        package_stub = types.ModuleType(_PACKAGE_NAME)
        package_stub.__path__ = [str(EXTENSION_PACKAGE_ROOT / _PACKAGE_NAME)]
        sys.modules[_PACKAGE_NAME] = package_stub

    return importlib.import_module(f"{_PACKAGE_NAME}.trace_provider")


class _Config:
    def get(self, key, default=None):
        return default


def _provider(trace_provider_module, tracer=None):
    provider = trace_provider_module.AzureMonitorTraceProvider.__new__(
        trace_provider_module.AzureMonitorTraceProvider
    )
    provider.config = _Config()
    provider._tracer = tracer
    return provider


def _fake_otel_span(trace_id=0):
    span = MagicMock()
    span.get_span_context.return_value = SimpleNamespace(trace_id=trace_id)
    return span


class TestStartSpan:
    def test_start_span_without_tracer_returns_span_with_trace_id(self, trace_provider_module):
        """Regression: SparkSpan construction previously omitted required traceId."""
        provider = _provider(trace_provider_module, tracer=None)

        span = provider.start_span(operation="op", component="comp")

        assert span.id == "noop"
        assert isinstance(span.traceId, uuid.UUID)

    def test_start_span_adopts_otel_trace_id(self, trace_provider_module):
        tracer = MagicMock()
        otel_span = _fake_otel_span(trace_id=12345)
        tracer.start_span.return_value = otel_span
        provider = _provider(trace_provider_module, tracer=tracer)

        span = provider.start_span(operation="op", component="comp", details={"k": "v"})

        assert span.traceId == uuid.UUID(int=12345)
        assert span._otel_span is otel_span
        tracer.start_span.assert_called_once_with("comp.op")

    def test_start_span_mints_trace_id_when_context_has_none(self, trace_provider_module):
        tracer = MagicMock()
        tracer.start_span.return_value = _fake_otel_span(trace_id=0)
        provider = _provider(trace_provider_module, tracer=tracer)

        span = provider.start_span(operation="op", component="comp")

        assert isinstance(span.traceId, uuid.UUID)


class TestRecordSpan:
    def test_record_span_honors_given_timestamps(self, trace_provider_module):
        tracer = MagicMock()
        otel_span = _fake_otel_span()
        tracer.start_span.return_value = otel_span
        provider = _provider(trace_provider_module, tracer=tracer)
        start = datetime(2026, 1, 1, 12, 0, 0)
        end = datetime(2026, 1, 1, 12, 0, 2)

        provider.record_span("phase", "kindling.bootstrap", start, end, details={"k": "v"})

        tracer.start_span.assert_called_once_with(
            "kindling.bootstrap.phase", start_time=int(start.timestamp() * 1_000_000_000)
        )
        otel_span.end.assert_called_once_with(end_time=int(end.timestamp() * 1_000_000_000))
        otel_span.set_attribute.assert_any_call("k", "v")
        status = otel_span.set_status.call_args[0][0]
        assert status.status_code == StatusCode.OK

    def test_record_span_with_error_sets_error_status(self, trace_provider_module):
        tracer = MagicMock()
        otel_span = _fake_otel_span()
        tracer.start_span.return_value = otel_span
        provider = _provider(trace_provider_module, tracer=tracer)
        start = datetime(2026, 1, 1, 12, 0, 0)
        end = datetime(2026, 1, 1, 12, 0, 1)

        provider.record_span("phase", "kindling.bootstrap", start, end, error="boom")

        status = otel_span.set_status.call_args[0][0]
        assert status.status_code == StatusCode.ERROR
        otel_span.add_event.assert_called_once_with(
            "error", attributes={"exception.message": "boom"}
        )
        otel_span.end.assert_called_once()

    def test_record_span_without_tracer_is_noop(self, trace_provider_module):
        provider = _provider(trace_provider_module, tracer=None)

        provider.record_span(
            "phase",
            "kindling.bootstrap",
            datetime(2026, 1, 1),
            datetime(2026, 1, 2),
        )
