"""Design-time CLI tracing (gh#210).

The CLI is a separate process with no injector or ConfigService — the SDK
deliberately has zero kindling-core imports, and the CLI populates the
injector only when it loads the user's app. Spans here come from a lazily
built PlainPythonTraceProvider over a tiny dict-config shim, gated by
environment variables since no ConfigService exists pre-app-load:

- ``KINDLING_TRACE=1`` enables CLI spans.
- ``KINDLING_KINDLING__TELEMETRY__TRACING__PRINT=true`` prints them (the
  same env override Dynaconf applies to kindling.telemetry.tracing.print
  once an app loads).

CLI migrate/pipeline commands get real tracing for free via the app's
initialize(); this module only brackets design-time operations (package/
app/runtime deploys, workspace uploads, run submission) with kindling.cli
spans. Tracing must never break a CLI command: any import or provider
failure degrades to a no-op.
"""

import functools
import os
from contextlib import nullcontext

# Hardcoded rather than imported from kindling.trace_ops: the constant is
# not worth importing the core (and transitively pyspark) for at CLI start.
CLI_COMPONENT = "kindling.cli"

_TRACE_ENV = "KINDLING_TRACE"
_PRINT_ENV = "KINDLING_KINDLING__TELEMETRY__TRACING__PRINT"

# Whitelisted command kwargs promoted to span attributes.
_DETAIL_KEYS = ("app_name", "platform", "env", "environment", "job_name", "package_name")

_provider = None


class _EnvConfigShim:
    """Just enough ConfigService surface for PlainPythonTraceProvider."""

    def __init__(self, values):
        self._values = values

    def get(self, key, default=None):
        return self._values.get(key, default)


def _truthy(value) -> bool:
    return str(value or "").strip().lower() in {"1", "true", "yes", "on"}


def cli_tracing_enabled() -> bool:
    return _truthy(os.environ.get(_TRACE_ENV))


def _get_provider():
    global _provider
    if _provider is None:
        from kindling.plain_telemetry import (
            PlainPythonLoggerProvider,
            PlainPythonTraceProvider,
        )

        values = {}
        if _truthy(os.environ.get(_PRINT_ENV)):
            values["print_trace"] = True
        shim = _EnvConfigShim(values)
        _provider = PlainPythonTraceProvider(PlainPythonLoggerProvider(shim), shim)
    return _provider


def cli_span(operation: str, details=None):
    """A kindling.cli span, or a no-op CM when KINDLING_TRACE is unset."""
    if not cli_tracing_enabled():
        return nullcontext()
    try:
        return _get_provider().span(
            operation=operation, component=CLI_COMPONENT, details=details, reraise=True
        )
    except Exception:
        return nullcontext()


def traced_command(operation: str):
    """Bracket a click command callback with a kindling.cli span.

    Place immediately above ``def`` (below the click decorators) so the
    span wraps the raw callback. Whitelisted kwargs become attributes.
    """

    def decorate(fn):
        @functools.wraps(fn)
        def wrapper(*args, **kwargs):
            details = {
                key: str(value)
                for key, value in kwargs.items()
                if key in _DETAIL_KEYS and value is not None
            }
            with cli_span(operation, details):
                return fn(*args, **kwargs)

        return wrapper

    return decorate
