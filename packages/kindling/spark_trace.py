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

from .spark_log_provider import *


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
                    f"TRACE: ({spark.sparkContext.appName}) Component: {component} Op: {operation} Msg: {custom_message} Id: {eventId} trace_id:{str(traceId)}"
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
    spark = get_or_create_spark_session()
    mdc = spark._jvm.org.apache.log4j.MDC
    try:
        for key, value in kwargs.items():
            spark.sparkContext.setLocalProperty("mdc." + key, value)
            mdc.put(key, value)
        yield
    finally:
        for key in kwargs:
            mdc.remove(key)
            spark.sparkContext.setLocalProperty("mdc." + key, "")


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

        current_span = SparkSpan(
            id=id,
            component=component or (self.current_span.component if self.current_span else None),
            operation=operation or (self.current_span.operation if self.current_span else None),
            attributes=details or (self.current_span.attributes if self.current_span else None),
            start_time=datetime.now(),
            traceId=self.current_span.traceId if self.current_span else uuid.uuid4(),
            reraise=reraise or (self.current_span.reraise if self.current_span else None),
        )

        self.current_span = current_span

        with mdc_context(
            trace_id=str(current_span.traceId),
            span_id=current_span.id,
            component=current_span.component,
            operation=current_span.operation,
        ):
            start_details = self._add_timestamp_to_dict(
                details or {}, "startTime", current_span.start_time
            )
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
                error_details = self._add_timestamp_to_dict(start_details, "errorTime", error_time)
                error_details["exception"] = traceback.format_exc()
                self.emitter.emit_custom_event(
                    current_span.component,
                    f"{current_span.operation}_ERROR",
                    error_details,
                    current_span.id,
                    current_span.traceId,
                )
                if reraise:
                    raise

            finally:
                current_span.end_time = datetime.now()
                end_details = self._add_timestamp_to_dict(
                    start_details, "endTime", current_span.end_time
                )
                end_details["totalTime"] = self._calculate_time_diff(
                    current_span.start_time, current_span.end_time
                )
                self.emitter.emit_custom_event(
                    current_span.component,
                    f"{current_span.operation}_END",
                    end_details,
                    current_span.id,
                    current_span.traceId,
                )
