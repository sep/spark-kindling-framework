"""Azure Monitor OpenTelemetry integration for Kindling framework."""

from .logger_provider import AzureMonitorLoggerProvider
from .trace_provider import AzureMonitorTraceProvider

__all__ = [
    "AzureMonitorLoggerProvider",
    "AzureMonitorTraceProvider",
]

__version__ = "0.1.0"


def _register_providers():
    """Register Azure Monitor providers with the DI container.

    This is called automatically when the module is imported.
    It forcibly rebinds the log and trace providers to the Azure Monitor implementations.
    """
    from injector import singleton
    from kindling.injection import GlobalInjector
    from kindling.spark_log_provider import PythonLoggerProvider, SparkLoggerProvider
    from kindling.spark_trace import SparkTraceProvider

    # Get the injector
    injector = GlobalInjector.get_injector()

    # Rebind the providers by creating new bindings (injector will replace existing)
    # This ensures Azure Monitor providers override the defaults

    # Bind log provider to both base and concrete classes
    injector.binder.bind(
        PythonLoggerProvider, to=singleton(AzureMonitorLoggerProvider), scope=singleton
    )

    injector.binder.bind(
        SparkLoggerProvider, to=singleton(AzureMonitorLoggerProvider), scope=singleton
    )

    # Bind trace provider
    injector.binder.bind(
        SparkTraceProvider, to=singleton(AzureMonitorTraceProvider), scope=singleton
    )

    print("✅ Azure Monitor providers registered (overriding defaults)")


# Automatically register providers when module is imported
_register_providers()
