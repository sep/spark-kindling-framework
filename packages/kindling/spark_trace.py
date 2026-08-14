import json
import time
import traceback
import uuid
from abc import ABC, abstractmethod
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Callable, Dict, Optional

from injector import Binder, Injector, inject, singleton
from kindling.injection import *
from kindling.spark_config import *
from kindling.spark_session import *
from py4j.java_gateway import JavaObject
from py4j.protocol import Py4JError

from .spark_log_provider import *

_mdc_api_blocked = False
_local_properties_blocked = False


def _is_mdc_api_blocked_exception(exc: Exception) -> bool:
    """Detect Databricks Py4J whitelist errors for org.apache.log4j.MDC calls."""
    if not isinstance(exc, Py4JError):
        return False
    message = str(exc)
    return "Py4JSecurityException" in message and "org.apache.log4j.MDC" in message


def _get_log4j_mdc(spark):
    """Return the log4j MDC handle, or None when the JVM bridge is unavailable.

    Databricks UC shared/standard access mode and Spark Connect raise
    PySparkAttributeError ([JVM_ATTRIBUTE_NOT_SUPPORTED]) on ``spark._jvm``
    access; it subclasses AttributeError.
    """
    global _mdc_api_blocked
    if _mdc_api_blocked:
        return None
    try:
        return spark._jvm.org.apache.log4j.MDC
    except (AttributeError, TypeError):
        _mdc_api_blocked = True
        return None


def _safe_app_name(spark) -> str:
    """appName without assuming sparkContext is accessible."""
    return getattr(getattr(spark, "sparkContext", None), "appName", None) or "unknown"


def _set_mdc_local_property(spark, key: str, value) -> None:
    """Set a Spark local property, no-op when sparkContext is unavailable."""
    global _local_properties_blocked
    if _local_properties_blocked:
        return
    try:
        spark.sparkContext.setLocalProperty(key, value)
    except (AttributeError, TypeError):
        _local_properties_blocked = True


class CustomEventEmitter(ABC):
    @abstractmethod
    def emit_custom_event(
        self, component: str, operation: str, details: dict, eventId: str, traceId: uuid
    ) -> None:
        pass


@GlobalInjector.singleton_autobind()
class AzureEventEmitter(CustomEventEmitter):
    @inject
    def __init__(self, plp: PythonLoggerProvider, cs: ConfigService):
        self.logger = plp.get_logger("EventEmitter")
        self.config = cs

    def should_print(self):
        should_print = self.config.get("print_trace", False) or self.config.get(
            "print_tracing", False
        )
        return should_print

    def emit_custom_event(
        self, component: str, operation: str, details: dict, eventId: str, traceId: uuid
    ) -> None:

        custom_message = json.dumps(details) if details else None

        spark = get_or_create_spark_session()

        # Check if Microsoft Spark metric events are available (Fabric/Synapse only)
        try:
            ComponentSparkEvent = spark._jvm.com.microsoft.spark.metricevents.ComponentSparkEvent
            # Verify it's actually a class, not just a JavaPackage
            if not callable(ComponentSparkEvent):
                raise TypeError("ComponentSparkEvent is not callable (JavaPackage)")
        except (AttributeError, TypeError):
            # Not on Fabric/Synapse - just log instead
            if self.should_print():
                print(
                    f"TRACE: ({_safe_app_name(spark)}) Component: {component} Op: {operation} Msg: {custom_message} Id: {eventId} trace_id:{str(traceId)}"
                )
            self.logger.debug(f"Trace event: {component}.{operation} - {custom_message}")
            return

        # ComponentSparkEvent is available and callable
        ScalaOption = spark._jvm.scala.Option  # Scala Option
        LogLevel = spark._jvm.org.slf4j.event.Level
        ScalaNone = ScalaOption.empty()
        JavaUUID = spark._jvm.java.util.UUID.fromString(str(traceId))

        event = spark._jvm.com.microsoft.spark.metricevents.ComponentSparkEvent(
            spark.sparkContext.appName,  # String appName
            component,  # String module
            operation,  # String activity
            ScalaOption.apply(eventId),  # Option<String> eventId
            ScalaOption.apply(JavaUUID),  # Option<UUID> traceId
            ScalaNone,  # Option<BlockInfo> blockInfo
            ScalaOption.apply(custom_message),  # Option<String> customMessage
            spark._jvm.org.slf4j.event.Level.INFO,  # Level logLevel
        )

        if self.should_print():
            print(
                f"TRACE: ({spark.sparkContext.appName}) Component: {component} Op: {operation} Msg: {custom_message} Id: {eventId} trace_id:{str(traceId)}"
            )

        # Get the SparkListener manager and post the event
        listener_bus = spark.sparkContext._jsc.sc().listenerBus()
        listener_bus.post(event)


@contextmanager
def mdc_context(**kwargs):
    global _mdc_api_blocked

    spark = get_or_create_spark_session()
    mdc = _get_log4j_mdc(spark)
    try:
        for key, value in kwargs.items():
            _set_mdc_local_property(spark, "mdc." + key, value)
            if mdc is None:
                continue
            try:
                mdc.put(key, value)
            except Exception as exc:
                if _is_mdc_api_blocked_exception(exc):
                    _mdc_api_blocked = True
                    mdc = None
                else:
                    raise
        yield
    finally:
        for key in kwargs:
            if mdc is not None:
                try:
                    mdc.remove(key)
                except Exception as exc:
                    if _is_mdc_api_blocked_exception(exc):
                        _mdc_api_blocked = True
                        mdc = None
                    else:
                        raise
            _set_mdc_local_property(spark, "mdc." + key, "")


@dataclass
class SparkSpan:
    id: str
    component: str
    operation: str
    attributes: Dict[str, str]
    traceId: uuid
    reraise: bool
    start_time: datetime = None
    end_time: datetime = None
    # Id of the enclosing span at open time (None for roots). Appended with a
    # default so existing positional constructions keep working.
    parent_id: Optional[str] = None


class SparkTraceProvider(ABC):
    @abstractmethod
    def span(
        self,
        operation: str = None,
        component: str = None,
        details: dict = None,
        reraise: bool = False,
    ):
        pass

    @abstractmethod
    def start_span(
        self,
        operation: str,
        component: str,
        details: dict = None,
    ) -> "SparkSpan":
        """Start a span manually. Returns the span for later add_event/end_span calls."""
        pass

    @abstractmethod
    def add_event(
        self,
        span: "SparkSpan",
        name: str,
        attributes: dict = None,
    ) -> None:
        """Add a timestamped event marker to an active span."""
        pass

    @abstractmethod
    def end_span(
        self,
        span: "SparkSpan",
        error: Optional[str] = None,
    ) -> None:
        """End a manually started span. Optionally record an error."""
        pass

    def record_span(
        self,
        operation: str,
        component: str,
        start_time: datetime,
        end_time: datetime,
        details: dict = None,
        error: Optional[str] = None,
    ) -> None:
        """Record an already-completed span with explicit timestamps.

        Concrete default so third-party/duck-typed providers keep working:
        routes through start_span/end_span and carries the true timestamps in
        the details. Built-in providers override to honor the given
        timestamps natively.
        """
        recorded_details = dict(details or {})
        recorded_details["recordedStartTime"] = start_time.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
        recorded_details["recordedEndTime"] = end_time.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
        span = self.start_span(operation, component, details=recorded_details)
        self.end_span(span, error=error)


@GlobalInjector.singleton_autobind()
class EventBasedSparkTrace(SparkTraceProvider):
    # Static instance to maintain session trace
    _instance = None

    @inject
    def __init__(self, emitter: CustomEventEmitter):
        self.emitter = emitter
        self.current_span = None
        self.activity_counter = 1

    def _add_timestamp_to_dict(
        self, dict: dict[str, str], key: str, ts: datetime
    ) -> dict[str, str]:
        d = dict or {}
        return {**d, **{key: ts.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]}}

    def _merge_dict(self, d1: dict[str, str], d2: dict[str, str]) -> dict[str, str]:
        return {**d1, **d2}

    def _calculate_time_diff(self, dt1: datetime, dt2: datetime) -> str:
        diff = (dt2 - dt1).total_seconds()
        return f"{diff:.3f}"

    def _increment_activity(self) -> int:
        self.activity_counter = self.activity_counter + 1
        return self.activity_counter

    @contextmanager
    def span(
        self,
        operation: str = None,
        component: str = None,
        details: dict = None,
        reraise: bool = False,
    ):
        id = str(self._increment_activity())
        live_details = details if details is not None else {}

        parent_span = self.current_span
        current_span = SparkSpan(
            id=id,
            component=component or (parent_span.component if parent_span else None),
            operation=operation or (parent_span.operation if parent_span else None),
            attributes=(
                live_details
                if details is not None
                else (parent_span.attributes if parent_span else None)
            ),
            start_time=datetime.now(),
            traceId=parent_span.traceId if parent_span else uuid.uuid4(),
            reraise=reraise or (parent_span.reraise if parent_span else None),
            parent_id=parent_span.id if parent_span else None,
        )

        self.current_span = current_span

        with mdc_context(
            trace_id=str(current_span.traceId),
            span_id=current_span.id,
            component=current_span.component,
            operation=current_span.operation,
        ):
            start_details = self._add_timestamp_to_dict(
                live_details, "startTime", current_span.start_time
            )
            if current_span.parent_id is not None:
                start_details["parentSpanId"] = current_span.parent_id
            try:
                self.emitter.emit_custom_event(
                    current_span.component,
                    f"{current_span.operation}_START",
                    start_details,
                    current_span.id,
                    current_span.traceId,
                )
                yield self

            except Exception as e:
                error_time = datetime.now()
                error_base = self._add_timestamp_to_dict(
                    live_details, "startTime", current_span.start_time
                )
                error_details = self._add_timestamp_to_dict(error_base, "errorTime", error_time)
                error_details["exception"] = traceback.format_exc()
                try:
                    self.emitter.emit_custom_event(
                        current_span.component,
                        f"{current_span.operation}_ERROR",
                        error_details,
                        current_span.id,
                        current_span.traceId,
                    )
                except Exception:
                    # A failing emitter must not replace the span's own
                    # exception on the reraise path.
                    pass
                if reraise:
                    raise

            finally:
                try:
                    current_span.end_time = datetime.now()
                    end_base = self._add_timestamp_to_dict(
                        live_details, "startTime", current_span.start_time
                    )
                    end_details = self._add_timestamp_to_dict(
                        end_base, "endTime", current_span.end_time
                    )
                    end_details["totalTime"] = self._calculate_time_diff(
                        current_span.start_time, current_span.end_time
                    )
                    if current_span.parent_id is not None:
                        end_details["parentSpanId"] = current_span.parent_id
                    self.emitter.emit_custom_event(
                        current_span.component,
                        f"{current_span.operation}_END",
                        end_details,
                        current_span.id,
                        current_span.traceId,
                    )
                except Exception:
                    # A failing END emission must not break caller control
                    # flow (or clobber an in-flight exception).
                    pass
                finally:
                    # Restore the enclosing span so sibling spans and
                    # subsequent runs in the same session do not blur into
                    # one tree — even when the END emission fails.
                    self.current_span = parent_span

    def start_span(
        self,
        operation: str,
        component: str,
        details: dict = None,
    ) -> SparkSpan:
        span_id = str(self._increment_activity())
        live_details = details if details is not None else {}

        span = SparkSpan(
            id=span_id,
            component=component,
            operation=operation,
            attributes=live_details,
            start_time=datetime.now(),
            traceId=self.current_span.traceId if self.current_span else uuid.uuid4(),
            reraise=False,
            parent_id=self.current_span.id if self.current_span else None,
        )

        start_details = self._add_timestamp_to_dict(live_details, "startTime", span.start_time)
        if span.parent_id is not None:
            start_details["parentSpanId"] = span.parent_id
        try:
            self.emitter.emit_custom_event(
                span.component,
                f"{span.operation}_START",
                start_details,
                span.id,
                span.traceId,
            )
        except Exception:
            # A failing emitter must not break the caller of a manual span
            # (e.g. the streaming listener's long-lived query span).
            pass

        return span

    def add_event(
        self,
        span: SparkSpan,
        name: str,
        attributes: dict = None,
    ) -> None:
        event_details = dict(attributes) if attributes else {}
        event_details = self._add_timestamp_to_dict(event_details, "eventTime", datetime.now())
        event_details = self._add_timestamp_to_dict(event_details, "spanStartTime", span.start_time)

        try:
            self.emitter.emit_custom_event(
                span.component,
                f"{span.operation}_EVENT_{name}",
                event_details,
                span.id,
                span.traceId,
            )
        except Exception:
            # A failing emitter must not break the caller (e.g. streaming
            # query progress updates degrade to a no-op instead of raising).
            pass

    def end_span(
        self,
        span: SparkSpan,
        error: Optional[str] = None,
    ) -> None:
        span.end_time = datetime.now()

        if error:
            error_details = self._add_timestamp_to_dict({}, "startTime", span.start_time)
            error_details = self._add_timestamp_to_dict(error_details, "errorTime", span.end_time)
            error_details["exception"] = error
            try:
                self.emitter.emit_custom_event(
                    span.component,
                    f"{span.operation}_ERROR",
                    error_details,
                    span.id,
                    span.traceId,
                )
            except Exception:
                # A failing emitter must not replace/suppress the caller's
                # own error handling for a manual span.
                pass

        end_details = self._add_timestamp_to_dict({}, "startTime", span.start_time)
        end_details = self._add_timestamp_to_dict(end_details, "endTime", span.end_time)
        end_details["totalTime"] = self._calculate_time_diff(span.start_time, span.end_time)
        if span.parent_id is not None:
            end_details["parentSpanId"] = span.parent_id
        try:
            self.emitter.emit_custom_event(
                span.component,
                f"{span.operation}_END",
                end_details,
                span.id,
                span.traceId,
            )
        except Exception:
            # A failing emitter must not break the caller of a manual span
            # (e.g. the streaming listener's long-lived query span).
            pass

    def record_span(
        self,
        operation: str,
        component: str,
        start_time: datetime,
        end_time: datetime,
        details: dict = None,
        error: Optional[str] = None,
    ) -> None:
        """Emit START/END (and ERROR) for a completed span with the given timestamps."""
        span_id = str(self._increment_activity())
        live_details = dict(details or {})

        span = SparkSpan(
            id=span_id,
            component=component,
            operation=operation,
            attributes=live_details,
            start_time=start_time,
            end_time=end_time,
            traceId=self.current_span.traceId if self.current_span else uuid.uuid4(),
            reraise=False,
            parent_id=self.current_span.id if self.current_span else None,
        )

        start_details = self._add_timestamp_to_dict(live_details, "startTime", start_time)
        if span.parent_id is not None:
            start_details["parentSpanId"] = span.parent_id
        try:
            self.emitter.emit_custom_event(
                span.component, f"{span.operation}_START", start_details, span.id, span.traceId
            )
        except Exception:
            # A failing emitter must not break the caller of a recorded span.
            pass

        if error:
            error_details = self._add_timestamp_to_dict(live_details, "startTime", start_time)
            error_details = self._add_timestamp_to_dict(error_details, "errorTime", end_time)
            error_details["exception"] = error
            try:
                self.emitter.emit_custom_event(
                    span.component, f"{span.operation}_ERROR", error_details, span.id, span.traceId
                )
            except Exception:
                pass

        end_details = self._add_timestamp_to_dict(live_details, "startTime", start_time)
        end_details = self._add_timestamp_to_dict(end_details, "endTime", end_time)
        end_details["totalTime"] = self._calculate_time_diff(start_time, end_time)
        if span.parent_id is not None:
            end_details["parentSpanId"] = span.parent_id
        try:
            self.emitter.emit_custom_event(
                span.component, f"{span.operation}_END", end_details, span.id, span.traceId
            )
        except Exception:
            pass
