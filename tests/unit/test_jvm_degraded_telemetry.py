"""Degraded-path tests for runtimes without a py4j JVM bridge.

Databricks UC shared/standard access mode clusters and Spark Connect raise
PySparkAttributeError (an AttributeError subclass) on ``spark._jvm`` and
``spark.sparkContext`` access. These tests lock in the graceful fallbacks and
the capability-based provider swap.
"""

from unittest.mock import MagicMock

import kindling.bootstrap as bootstrap_module
import kindling.notebook_framework as notebook_framework_module
import pytest
from injector import Injector
from kindling.injection import GlobalInjector
from kindling.plain_telemetry import PlainPythonLoggerProvider, PlainPythonTraceProvider
from kindling.spark_config import ConfigService
from kindling.spark_log_provider import PythonLoggerProvider, SparkLoggerProvider
from kindling.spark_trace import SparkTraceProvider


class _PySparkAttributeErrorStub(AttributeError):
    pass


class _JvmBlockedSpark:
    @property
    def _jvm(self):
        raise _PySparkAttributeErrorStub(
            "[JVM_ATTRIBUTE_NOT_SUPPORTED] Attribute `_jvm` is not supported."
        )

    @property
    def sparkContext(self):
        raise _PySparkAttributeErrorStub(
            "[JVM_ATTRIBUTE_NOT_SUPPORTED] Attribute `sparkContext` is not supported."
        )


class TestGetSparkLogLevelFallback:
    def test_bootstrap_copy_returns_none_without_jvm_bridge(self, monkeypatch):
        monkeypatch.setattr(
            bootstrap_module, "get_or_create_spark_session", lambda: _JvmBlockedSpark()
        )
        assert bootstrap_module.get_spark_log_level() is None

    def test_notebook_framework_copy_returns_none_without_jvm_bridge(self, monkeypatch):
        monkeypatch.setattr(
            notebook_framework_module, "get_or_create_spark_session", lambda: _JvmBlockedSpark()
        )
        assert notebook_framework_module.get_spark_log_level() is None

    def test_console_logger_defaults_to_info_without_jvm_bridge(self, monkeypatch):
        monkeypatch.setattr(
            bootstrap_module, "get_or_create_spark_session", lambda: _JvmBlockedSpark()
        )
        logger = bootstrap_module.create_console_logger({})

        logger.info("does not raise")
        logger.debug("does not raise either")


class TestPlainTelemetryProviderSwap:
    @pytest.fixture
    def injector(self, monkeypatch):
        injector = Injector(auto_bind=True)
        config = MagicMock()
        config.get.return_value = None
        injector.binder.bind(ConfigService, to=config)
        monkeypatch.setattr(GlobalInjector, "get_injector", classmethod(lambda cls: injector))
        return injector

    def test_bind_plain_telemetry_providers_rebinds_all_three(self, injector):
        bootstrap_module._bind_plain_telemetry_providers()

        assert isinstance(injector.get(PythonLoggerProvider), PlainPythonLoggerProvider)
        assert isinstance(injector.get(SparkLoggerProvider), PlainPythonLoggerProvider)
        assert isinstance(injector.get(SparkTraceProvider), PlainPythonTraceProvider)

    def test_rebind_replaces_providers_already_resolved(self, injector):
        # The bootstrap resolves PythonLoggerProvider (and may resolve the trace
        # provider) before the swap; injector caches singletons per interface.
        from injector import singleton

        class DefaultLoggerProvider(PythonLoggerProvider):
            def get_logger(self, name, session=None):
                return MagicMock()

        class DefaultTraceProvider(SparkTraceProvider):
            pass

        DefaultTraceProvider.__abstractmethods__ = frozenset()
        injector.binder.bind(PythonLoggerProvider, to=DefaultLoggerProvider, scope=singleton)
        injector.binder.bind(SparkTraceProvider, to=DefaultTraceProvider, scope=singleton)
        assert isinstance(injector.get(PythonLoggerProvider), DefaultLoggerProvider)
        assert isinstance(injector.get(SparkTraceProvider), DefaultTraceProvider)

        bootstrap_module._bind_plain_telemetry_providers()

        assert isinstance(injector.get(PythonLoggerProvider), PlainPythonLoggerProvider)
        assert isinstance(injector.get(SparkLoggerProvider), PlainPythonLoggerProvider)
        assert isinstance(injector.get(SparkTraceProvider), PlainPythonTraceProvider)

    def test_reinitialization_keeps_an_extension_override(self, injector):
        # A second initialize_framework runs the swap again after
        # kindling_ext_otel_azure registered its providers; the extension is
        # already imported, so nothing would register them again.
        class ExtensionLoggerProvider(PythonLoggerProvider):
            def get_logger(self, name, session=None):
                return MagicMock()

        class ExtensionTraceProvider(SparkTraceProvider):
            pass

        ExtensionTraceProvider.__abstractmethods__ = frozenset()
        bootstrap_module._bind_plain_telemetry_providers()
        GlobalInjector.rebind_singleton(PythonLoggerProvider, ExtensionLoggerProvider)
        GlobalInjector.rebind_singleton(SparkLoggerProvider, ExtensionLoggerProvider)
        GlobalInjector.rebind_singleton(SparkTraceProvider, ExtensionTraceProvider)
        resolved = injector.get(PythonLoggerProvider)

        bootstrap_module._bind_plain_telemetry_providers()

        assert injector.get(PythonLoggerProvider) is resolved
        assert isinstance(injector.get(SparkLoggerProvider), ExtensionLoggerProvider)
        assert isinstance(injector.get(SparkTraceProvider), ExtensionTraceProvider)

    def test_repeated_swap_keeps_the_plain_providers(self, injector):
        bootstrap_module._bind_plain_telemetry_providers()
        bootstrap_module._bind_plain_telemetry_providers()

        assert isinstance(injector.get(PythonLoggerProvider), PlainPythonLoggerProvider)
        assert isinstance(injector.get(SparkTraceProvider), PlainPythonTraceProvider)

    def test_rebound_providers_are_singletons(self, injector):
        bootstrap_module._bind_plain_telemetry_providers()

        assert injector.get(SparkTraceProvider) is injector.get(SparkTraceProvider)
        assert injector.get(PythonLoggerProvider) is injector.get(PythonLoggerProvider)
