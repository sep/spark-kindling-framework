"""Degraded-path tests for runtimes without a py4j JVM bridge.

Databricks UC shared/standard access mode clusters and Spark Connect raise
PySparkAttributeError (an AttributeError subclass) on ``spark._jvm`` and
``spark.sparkContext`` access. These tests lock in the graceful fallbacks and
the capability-based provider swap.
"""

from unittest.mock import MagicMock

import pytest
from injector import Injector

import kindling.bootstrap as bootstrap_module
import kindling.notebook_framework as notebook_framework_module
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

    def test_rebound_providers_are_singletons(self, injector):
        bootstrap_module._bind_plain_telemetry_providers()

        assert injector.get(SparkTraceProvider) is injector.get(SparkTraceProvider)
        assert injector.get(PythonLoggerProvider) is injector.get(PythonLoggerProvider)
