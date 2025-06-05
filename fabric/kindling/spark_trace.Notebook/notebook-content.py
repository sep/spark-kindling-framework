# Fabric notebook source


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
  
notebook_import(".spark_session")

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

def add_timestamp_to_dict(dict: dict[str,str], key: str, ts: datetime) -> dict[str,str]:
    d = dict or {}
    return {**d, **{ key: ts.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3] }}

def merge_dict(d1: dict[str,str], d2: dict[str,str]) -> dict[str,str]:
    return {**d1, **d2}

def calculate_time_diff(dt1: datetime, dt2: datetime) -> str:
    diff = (dt2 - dt1).total_seconds()
    return f"{diff:.3f}"

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

class SparkTrace:
    # Static instance to maintain session trace
    _instance = None
    
    @classmethod
    def current(cls) -> 'SparkTrace':
        """Get the current trace. If none exists, create a default one."""
        if cls._instance is None:
            cls._instance = cls("DEFAULT")
        return cls._instance
    
    @classmethod
    def create(cls, component: str, operation: str = "None", details: dict = None, 
             reraise: bool = False, emitter: Callable = None) -> 'SparkTrace':
        """Create a new trace while maintaining the session's trace ID."""
        # Get trace_id and counter from current instance if it exists
        trace_id = None
        activity_counter = 0
        
        if cls._instance is not None:
            trace_id = cls._instance.traceId
            activity_counter = cls._instance.activity_counter
            
        # Create new trace with existing trace ID
        trace = cls(component, operation, details, reraise, emitter, 
                  trace_id=trace_id, activity_counter=activity_counter)
        
        # Set as current trace
        cls._instance = trace
        return trace
    
    def __init__(self, component: str, operation: str = "None", details: dict = None, 
               reraise: bool = False, emitter: Callable = emit_custom_event, 
               trace_id: uuid = None, activity_counter: int = 0):
        self.name = component
        self.operation = operation
        self.start_time = None
        self.attributes: Dict[str, str] = details or {}
        self.activity_counter = activity_counter
        self.traceId = trace_id or uuid.uuid4()
        self.emitter = emitter 
        self.reraise = reraise
        
        # Register this trace as the current trace
        SparkTrace._instance = self
    
    def _increment_activity(self) -> int:
        self.activity_counter = self.activity_counter + 1
        return self.activity_counter

    @contextmanager
    def span(self, operation: str = None, component: str = None, details: dict = None, reraise: bool = False):
        id = str(self._increment_activity())        

        current_span = SparkSpan( 
            id = id, 
            name = component or self.name, 
            operation = operation or self.operation, 
            attributes = details or self.attributes,
            start_time = datetime.now(), 
            traceId = self.traceId, 
            reraise = reraise or self.reraise 
        )

        self.current_span = current_span

        with mdc_context(trace_id=str(current_span.traceId), span_id=current_span.id, 
                        component=current_span.name, operation=current_span.operation):
            start_details = add_timestamp_to_dict(details or {}, "startTime", current_span.start_time)
            try:
                self.emitter(current_span.name, f"{current_span.operation}_START", 
                           start_details, current_span.id, current_span.traceId)
                yield self
                
            except Exception as e:
                error_time = datetime.now()
                error_details = add_timestamp_to_dict(start_details, "errorTime", error_time)
                error_details["exception"] = traceback.format_exc()
                self.emitter(current_span.name, f"{current_span.operation}_ERROR", 
                           error_details, current_span.id, current_span.traceId)
                if reraise:
                    raise

            finally: 
                current_span.end_time = datetime.now()
                end_details = add_timestamp_to_dict(start_details, "endTime", current_span.end_time)
                end_details["totalTime"] = calculate_time_diff(current_span.start_time, current_span.end_time)
                self.emitter(current_span.name, f"{current_span.operation}_END", 
                           end_details, current_span.id, current_span.traceId)

