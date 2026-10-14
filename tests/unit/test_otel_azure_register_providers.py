"""Provider registration of kindling_ext_otel_azure (``_register_providers``).

The Kindling bootstrap resolves ``PythonLoggerProvider`` before it imports
extensions, and injector caches singletons per interface key, so the
extension's rebind must replace providers that were already resolved.

The package ``__init__`` is executed against stub ``logger_provider`` /
``trace_provider`` submodules (the real ones need the Azure Monitor
OpenTelemetry distro) and a private injector, so nothing leaks into the
global DI container or ``sys.modules``.
"""

import importlib.util
import sys
import types
from pathlib import Path
from unittest.mock import MagicMock

import pytest
from injector import Injector, singleton
from kindling.injection import GlobalInjector
from kindling.spark_log_provider import PythonLoggerProvider, SparkLoggerProvider
from kindling.spark_trace import SparkTraceProvider

_PACKAGE_NAME = "kindling_ext_otel_azure"
_PACKAGE_INIT = (
    Path(__file__).resolve().parents[2]
    / "packages"
    / "extensions"
    / _PACKAGE_NAME
    / _PACKAGE_NAME
    / "__init__.py"
)


class _DefaultLoggerProvider(PythonLoggerProvider):
    """Stands in for the core default (SparkLoggerProvider), which needs Spark."""

    def get_logger(self, name, session=None):
        return MagicMock()


class _DefaultTraceProvider(SparkTraceProvider):
    pass


class _StubAzureMonitorLoggerProvider(SparkLoggerProvider):
    def __init__(self):
        pass


class _StubAzureMonitorTraceProvider(SparkTraceProvider):
    pass


# Instantiable without implementing the tracing API, which these tests never call.
_DefaultTraceProvider.__abstractmethods__ = frozenset()
_StubAzureMonitorTraceProvider.__abstractmethods__ = frozenset()


@pytest.fixture
def injector(monkeypatch):
    injector = Injector(auto_bind=True)
    # Singleton defaults, as kindling's own singleton_autobind registers them.
    injector.binder.bind(PythonLoggerProvider, to=_DefaultLoggerProvider, scope=singleton)
    injector.binder.bind(SparkTraceProvider, to=_DefaultTraceProvider, scope=singleton)
    monkeypatch.setattr(GlobalInjector, "get_injector", classmethod(lambda cls: injector))
    return injector


def _import_extension(monkeypatch):
    """Execute the extension's ``__init__`` (which registers on import)."""
    logger_module = types.ModuleType(f"{_PACKAGE_NAME}.logger_provider")
    logger_module.AzureMonitorLoggerProvider = _StubAzureMonitorLoggerProvider
    trace_module = types.ModuleType(f"{_PACKAGE_NAME}.trace_provider")
    trace_module.AzureMonitorTraceProvider = _StubAzureMonitorTraceProvider
    monkeypatch.setitem(sys.modules, logger_module.__name__, logger_module)
    monkeypatch.setitem(sys.modules, trace_module.__name__, trace_module)

    spec = importlib.util.spec_from_file_location(
        _PACKAGE_NAME, _PACKAGE_INIT, submodule_search_locations=[]
    )
    module = importlib.util.module_from_spec(spec)
    monkeypatch.setitem(sys.modules, _PACKAGE_NAME, module)
    spec.loader.exec_module(module)
    return module


def test_registration_replaces_providers_resolved_before_import(injector, monkeypatch):
    # What the bootstrap does before it loads extensions.
    assert isinstance(injector.get(PythonLoggerProvider), _DefaultLoggerProvider)
    assert isinstance(injector.get(SparkTraceProvider), _DefaultTraceProvider)

    _import_extension(monkeypatch)

    assert isinstance(injector.get(PythonLoggerProvider), _StubAzureMonitorLoggerProvider)
    assert isinstance(injector.get(SparkLoggerProvider), _StubAzureMonitorLoggerProvider)
    assert isinstance(injector.get(SparkTraceProvider), _StubAzureMonitorTraceProvider)


def test_registration_falls_back_on_runtimes_without_rebind_singleton(injector, monkeypatch):
    # Older Kindling runtimes: plain rebind (does not evict earlier resolutions).
    monkeypatch.delattr(GlobalInjector, "rebind_singleton")

    _import_extension(monkeypatch)

    assert isinstance(injector.get(PythonLoggerProvider), _StubAzureMonitorLoggerProvider)
    assert isinstance(injector.get(SparkLoggerProvider), _StubAzureMonitorLoggerProvider)
    assert isinstance(injector.get(SparkTraceProvider), _StubAzureMonitorTraceProvider)
