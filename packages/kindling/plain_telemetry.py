"""JVM-free telemetry providers.

Used on runtimes without a py4j JVM bridge — Databricks UC shared/standard
access mode clusters and Spark Connect — where ``spark._jvm`` and
``spark.sparkContext`` raise PySparkAttributeError. Bootstrap rebinds the
telemetry providers to these implementations when the ``spark.jvm_bridge``
runtime feature is False (see ``kindling.features``); force either direction
with the static ``kindling.features.spark.jvm_bridge`` config override.

These classes never touch the Spark JVM bridge: logging goes through the
standard ``logging`` module and spans are emitted as log records.
"""

import json
import logging
import traceback
import uuid
from contextlib import contextmanager
from datetime import datetime
from typing import Optional

from injector import inject

from kindling.spark_config import ConfigService
from kindling.spark_log_provider import PythonLoggerProvider
from kindling.spark_trace import SparkSpan, SparkTraceProvider

_LEVEL_HIERARCHY = {"error": 0, "warn": 1, "warning": 1, "info": 2, "debug": 3}

_PYTHON_LEVELS = {
    "debug": logging.DEBUG,
    "info": logging.INFO,
    "warn": logging.WARNING,
    "warning": logging.WARNING,
    "error": logging.ERROR,
}


class PlainPythonLogger:
    """SparkLogger-compatible logger backed purely by python logging."""

    def __init__(self, name: str, config=None):
        self.name = name
        self.config = config or {}
        self.logger = logging.getLogger(name)
        self.log_level = str(self.config.get("log_level", "") or "").lower()

    def should_print(self):
        return self.config.get("print_logging", False)

    def debug(self, msg: str, include_traceback: bool = False):
        self._log("debug", msg, include_traceback=include_traceback)

    def info(self, msg: str, include_traceback: bool = False):
        self._log("info", msg, include_traceback=include_traceback)

    def warn(self, msg: str, include_traceback: bool = False):
        self._log("warn", msg, include_traceback=include_traceback)

    def warning(self, msg: str, include_traceback: bool = False):
        self._log("warn", msg, include_traceback=include_traceback)

    def error(self, msg: str, include_traceback: bool = False):
        self._log("error", msg, include_traceback=include_traceback)

    def exception(self, msg: str):
        self._log("error", msg, include_traceback=True)

    def _is_level_enabled(self, level: str) -> bool:
        if self.log_level not in _LEVEL_HIERARCHY:
            return True
        return _LEVEL_HIERARCHY[level] <= _LEVEL_HIERARCHY[self.log_level]

    def _log(self, level: str, msg: str, include_traceback: bool = False):
        if not self._is_level_enabled(level):
            return
        formatted = f"{level.upper()}: ({self.name}) {msg}"
        if include_traceback:
            formatted = f"{formatted}\n{traceback.format_exc()}"
        self.logger.log(_PYTHON_LEVELS[level], formatted)
        if self.should_print():
            print(formatted)


class PlainPythonLoggerProvider(PythonLoggerProvider):
    @inject
    def __init__(self, config: ConfigService):
        self.config = config

    def get_logger(self, name: str, session=None):
        return PlainPythonLogger(name, config=self.config)


class PlainPythonTraceProvider(SparkTraceProvider):
    """Span provider that emits span lifecycle events as log records.

    Mirrors EventBasedSparkTrace semantics (nested spans inherit component,
    operation, and trace id; START/ERROR/END events; reraise behavior) without
    MDC or the Spark listener bus.
    """

    @inject
    def __init__(self, logger_provider: PythonLoggerProvider, config: ConfigService):
        self.logger = logger_provider.get_logger("Trace")
        self.config = config
        self.current_span = None
        self.activity_counter = 1

    def should_print(self):
        return self.config.get("print_trace", False) or self.config.get("print_tracing", False)

    def _increment_activity(self) -> int:
        self.activity_counter = self.activity_counter + 1
        return self.activity_counter

    def _emit(self, span: SparkSpan, event: str, details: dict) -> None:
        message = json.dumps(details) if details else None
        line = (
            f"TRACE: Component: {span.component} Op: {event} Msg: {message} "
            f"Id: {span.id} trace_id:{span.traceId}"
        )
        self.logger.debug(line)
        if self.should_print():
            print(line)

    @staticmethod
    def _with_timestamp(details: dict, key: str, ts: datetime) -> dict:
        return {**(details or {}), key: ts.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]}

    @staticmethod
    def _total_time(start: datetime, end: datetime) -> str:
        return f"{(end - start).total_seconds():.3f}"

    @contextmanager
    def span(
        self,
        operation: str = None,
        component: str = None,
        details: dict = None,
        reraise: bool = False,
    ):
        live_details = details if details is not None else {}
        parent = self.current_span

        current_span = SparkSpan(
            id=str(self._increment_activity()),
            component=component or (parent.component if parent else None),
            operation=operation or (parent.operation if parent else None),
            attributes=(
                live_details if details is not None else (parent.attributes if parent else None)
            ),
            start_time=datetime.now(),
            traceId=parent.traceId if parent else uuid.uuid4(),
            reraise=reraise or (parent.reraise if parent else None),
        )
        self.current_span = current_span

        self._emit(
            current_span,
            f"{current_span.operation}_START",
            self._with_timestamp(live_details, "startTime", current_span.start_time),
        )
        try:
            yield self
        except Exception:
            error_details = self._with_timestamp(live_details, "startTime", current_span.start_time)
            error_details = self._with_timestamp(error_details, "errorTime", datetime.now())
            error_details["exception"] = traceback.format_exc()
            self._emit(current_span, f"{current_span.operation}_ERROR", error_details)
            if reraise:
                raise
        finally:
            current_span.end_time = datetime.now()
            end_details = self._with_timestamp(live_details, "startTime", current_span.start_time)
            end_details = self._with_timestamp(end_details, "endTime", current_span.end_time)
            end_details["totalTime"] = self._total_time(
                current_span.start_time, current_span.end_time
            )
            self._emit(current_span, f"{current_span.operation}_END", end_details)
            self.current_span = parent

    def start_span(
        self,
        operation: str,
        component: str,
        details: dict = None,
    ) -> SparkSpan:
        live_details = details if details is not None else {}
        span = SparkSpan(
            id=str(self._increment_activity()),
            component=component,
            operation=operation,
            attributes=live_details,
            start_time=datetime.now(),
            traceId=self.current_span.traceId if self.current_span else uuid.uuid4(),
            reraise=False,
        )
        self._emit(
            span,
            f"{span.operation}_START",
            self._with_timestamp(live_details, "startTime", span.start_time),
        )
        return span

    def add_event(
        self,
        span: SparkSpan,
        name: str,
        attributes: dict = None,
    ) -> None:
        event_details = dict(attributes) if attributes else {}
        event_details = self._with_timestamp(event_details, "eventTime", datetime.now())
        event_details = self._with_timestamp(event_details, "spanStartTime", span.start_time)
        self._emit(span, f"{span.operation}_EVENT_{name}", event_details)

    def end_span(
        self,
        span: SparkSpan,
        error: Optional[str] = None,
    ) -> None:
        span.end_time = datetime.now()

        if error:
            error_details = self._with_timestamp({}, "startTime", span.start_time)
            error_details = self._with_timestamp(error_details, "errorTime", span.end_time)
            error_details["exception"] = error
            self._emit(span, f"{span.operation}_ERROR", error_details)

        end_details = self._with_timestamp({}, "startTime", span.start_time)
        end_details = self._with_timestamp(end_details, "endTime", span.end_time)
        end_details["totalTime"] = self._total_time(span.start_time, span.end_time)
        self._emit(span, f"{span.operation}_END", end_details)
