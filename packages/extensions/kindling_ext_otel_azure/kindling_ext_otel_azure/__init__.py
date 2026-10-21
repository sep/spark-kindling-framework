"""Azure Monitor OpenTelemetry integration for Kindling framework."""

from .logger_provider import AzureMonitorLoggerProvider
from .trace_provider import AzureMonitorTraceProvider

__all__ = [
    "AzureMonitorLoggerProvider",
    "AzureMonitorTraceProvider",
]

__version__ = "0.14.0rc3"


def _register_providers():
    """Register Azure Monitor providers with the DI container.

    This is called automatically when the module is imported.
    It forcibly rebinds the log and trace providers to the Azure Monitor implementations.

    The Kindling bootstrap resolves ``PythonLoggerProvider`` before it imports
    extensions, and injector caches singletons per interface, so a plain
    rebind would leave that interface returning the default provider.
    ``GlobalInjector.rebind_singleton`` also evicts the cached instance; the
    plain rebind is the fallback for Kindling runtimes without it. Loggers
    already handed out (e.g. the bootstrap's own) are not replaced.
    """
    from injector import singleton
    from kindling.injection import GlobalInjector
    from kindling.spark_log_provider import PythonLoggerProvider, SparkLoggerProvider
    from kindling.spark_trace import SparkTraceProvider

    rebind = getattr(GlobalInjector, "rebind_singleton", None)
    if rebind is None:
        binder = GlobalInjector.get_injector().binder

        def rebind(interface, implementation):
            binder.bind(interface, to=implementation, scope=singleton)

    # Bind log provider to both base and concrete classes
    rebind(PythonLoggerProvider, singleton(AzureMonitorLoggerProvider))
    rebind(SparkLoggerProvider, singleton(AzureMonitorLoggerProvider))

    # Bind trace provider
    rebind(SparkTraceProvider, singleton(AzureMonitorTraceProvider))

    print("✅ Azure Monitor providers registered (overriding defaults)")


# Automatically register providers when module is imported
_register_providers()
