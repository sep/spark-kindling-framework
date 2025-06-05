# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   }
# META }

# CELL ********************

from typing import Optional, Dict, Any, Callable
from py4j.java_gateway import JavaObject
import uuid
import json
from datetime import datetime
import traceback
from dataclasses import dataclass, field
from contextlib import contextmanager
import time

from abc import ABC, abstractmethod
from injector import Injector, inject, singleton, Binder

notebook_import(".spark_session")
notebook_import(".spark_log")
notebook_import(".injection")
notebook_import(".spark_config")

class CustomEventEmitter(ABC):
    @abstractmethod
    def emit_custom_event(
        component: str,
        operation: str,
        details: dict,
        eventId: str,
        traceId: uuid ) -> None:
            pass

@GlobalInjector.singleton_autobind()
class SynapseEventEmitter(BaseServiceProvider, CustomEventEmitter):

    @inject
    def __init__(self):
        self.logger = SparkLogger("spark_trace")

    # Keep the original helper functions
    def emit_custom_event(
        component: str,
        operation: str,
        details: dict,
        eventId: str,
        traceId: uuid ) -> None:

        custom_message = json.dumps(details) if details else None

        spark = get_or_create_spark_session()

        ComponentSparkEvent = spark._jvm.com.microsoft.spark.metricevents.ComponentSparkEvent

        ScalaOption = spark._jvm.scala.Option  # Scala Option
        LogLevel = spark._jvm.org.slf4j.event.Level
        ScalaNone = ScalaOption.empty()
        JavaUUID = spark._jvm.java.util.UUID.fromString(str(traceId))

        # Create the ComponentSparkEvent
        event = spark._jvm.com.microsoft.spark.metricevents.ComponentSparkEvent(
            spark.sparkContext.appName,                         # String appName
            component,                                          # String module
            operation,                                          # String activity
            ScalaOption.apply(eventId),                         # Option<String> eventId
            ScalaOption.apply(JavaUUID),                        # Option<UUID> traceId
            ScalaNone,                                          # Option<BlockInfo> blockInfo
            ScalaOption.apply(custom_message),                  # Option<String> customMessage
            spark._jvm.org.slf4j.event.Level.INFO               # Level logLevel
        )

        print(f"Tracer: {spark.sparkContext.appName} Component: {component} Op: {operation} Msg: {custom_message} trace_id:{str(traceId)}")

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
    name: str
    operation: str
    attributes: Dict[str, str] 
    traceId: uuid
    reraise: bool
    start_time: datetime = None
    end_time: datetime = None

class CustomEventPoster(ABC):
    @abstractmethod
    def emit_custom_event(
        component: str,
        operation: str,
        details: dict,
        eventId: str,
        traceId: uuid ) -> None:
            pass

class CustomTraceProvider(ABC):
    @abstractmethod
    def span(self, operation: str = None, component: str = None, details: dict = None, reraise: bool = False):
        pass  

@GlobalInjector.singleton_autobind()
class SparkTrace(BaseServiceProvider, CustomEventPoster):
    # Static instance to maintain session trace
    _instance = None
    
    @inject
    def __init__(self, emitter: CustomEventPoster):
        self.emitter = emitter 
        self.activity_counter = 1
    
    def _add_timestamp_to_dict(dict: dict[str,str], key: str, ts: datetime) -> dict[str,str]:
        d = dict or {}
        return {**d, **{ key: ts.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3] }}

    def _merge_dict(d1: dict[str,str], d2: dict[str,str]) -> dict[str,str]:
        return {**d1, **d2}

    def _calculate_time_diff(dt1: datetime, dt2: datetime) -> str:
        diff = (dt2 - dt1).total_seconds()
        return f"{diff:.3f}"

    def _increment_activity(self) -> int:
        self.activity_counter = self.activity_counter + 1
        return self.activity_counter

    @contextmanager
    def span(self, operation: str = None, component: str = None, details: dict = None, reraise: bool = False):
        id = str(self._increment_activity())        

        current_span = SparkSpan( 
            id = id, 
            name = component or self.current_span.name if self.current_span else None, 
            operation = operation or self.current_span.operation if self.current_span else None, 
            attributes = details or self.current_span.attributes if self.current_span else None, 
            start_time = datetime.now(),
            traceId = self.traceId, 
            reraise = reraise or self.current_span.reraise if self.current_span else None
        )

        self.current_span = current_span

        with mdc_context(trace_id=str(current_span.traceId), span_id=current_span.id, 
                        component=current_span.name, operation=current_span.operation):
            start_details = self._add_timestamp_to_dict(details or {}, "startTime", current_span.start_time)
            try:
                self.emitter(current_span.name, f"{current_span.operation}_START", 
                           start_details, current_span.id, current_span.traceId)
                yield self
                
            except Exception as e:
                error_time = datetime.now()
                error_details = self._add_timestamp_to_dict(start_details, "errorTime", error_time)
                error_details["exception"] = traceback.format_exc()
                self.emitter(current_span.name, f"{current_span.operation}_ERROR", 
                           error_details, current_span.id, current_span.traceId)
                if reraise:
                    raise

            finally: 
                current_span.end_time = datetime.now()
                end_details = self._add_timestamp_to_dict(start_details, "endTime", current_span.end_time)
                end_details["totalTime"] = self._calculate_time_diff(current_span.start_time, current_span.end_time)
                self.emitter(current_span.name, f"{current_span.operation}_END", 
                           end_details, current_span.id, current_span.traceId)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
