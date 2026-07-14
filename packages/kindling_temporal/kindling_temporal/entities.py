"""Canonical temporal entity metadata."""

from abc import ABC, abstractmethod
from typing import Any

from kindling.data_entities import EntityMetadata
from kindling.injection import GlobalInjector
from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    IntegerType,
    LongType,
    MapType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


def events_schema() -> StructType:
    """Return the canonical temporal event envelope schema."""
    string_map = MapType(StringType(), StringType(), True)
    return StructType(
        [
            StructField("event_id", StringType(), False),
            StructField("event_type", StringType(), False),
            StructField("generation", IntegerType(), False),
            StructField("event_class", StringType(), False),
            StructField("subject_type", StringType(), False),
            StructField("subject_id", StringType(), False),
            StructField("event_ts", TimestampType(), False),
            StructField("source_system", StringType(), True),
            StructField("correlation_id", StringType(), True),
            StructField("payload", string_map, True),
            StructField("attributes", string_map, True),
            StructField("ingested_at", TimestampType(), False),
        ]
    )


def conditions_schema() -> StructType:
    """Return the rules-as-data condition schema."""
    return StructType(
        [
            StructField("condition_id", StringType(), False),
            StructField("consumes_event_type", ArrayType(StringType(), False), False),
            StructField("subject_type", StringType(), False),
            StructField("parameters", MapType(StringType(), StringType(), True), False),
            StructField("enabled", BooleanType(), False),
            StructField("valid_from", TimestampType(), False),
            StructField("valid_to", TimestampType(), True),
        ]
    )


def episodes_schema() -> StructType:
    """Return the canonical episode schema."""
    return StructType(
        [
            StructField("episode_id", StringType(), False),
            StructField("episode_type", StringType(), False),
            StructField("condition_id", StringType(), False),
            StructField("parent_episode_id", StringType(), True),
            StructField("subject_type", StringType(), False),
            StructField("subject_id", StringType(), False),
            StructField("start_event_id", StringType(), False),
            StructField("end_event_id", StringType(), True),
            StructField("start_time", TimestampType(), False),
            StructField("end_time", TimestampType(), True),
            StructField("status", StringType(), False),
            StructField("close_reason", StringType(), True),
            StructField("end_event_synthetic", BooleanType(), True),
            StructField("duration_ms", LongType(), True),
            StructField("event_count", LongType(), True),
            StructField("created_at", TimestampType(), False),
            StructField("updated_at", TimestampType(), False),
            StructField("attributes", MapType(StringType(), StringType(), True), True),
        ]
    )


class TemporalEntityResolver(ABC):
    """Resolve canonical temporal entities for this app/runtime."""

    @abstractmethod
    def get_events_entity(self) -> Any:
        pass

    @abstractmethod
    def get_conditions_entity(self) -> Any:
        pass

    @abstractmethod
    def get_episodes_entity(self) -> Any:
        pass


@GlobalInjector.singleton_autobind()
class SimpleTemporalEntityResolver(TemporalEntityResolver):
    """Default resolver: one shared temporal table set for the app."""

    def __init__(self):
        self._events_entity = EntityMetadata(
            entityid="silver.events",
            name="events",
            merge_columns=["event_id"],
            tags={"provider_type": "delta", "temporal.kind": "events"},
            schema=events_schema(),
            partition_columns=[],
        )
        self._conditions_entity = EntityMetadata(
            entityid="silver.conditions",
            name="conditions",
            merge_columns=["condition_id"],
            tags={
                "provider_type": "delta",
                "temporal.kind": "conditions",
                "scd.type": "2",
                "scd.routing_key": "hash",
                "scd.close_on_missing": "true",
                "scd.current_entity_id": "silver.conditions.current",
            },
            schema=conditions_schema(),
            partition_columns=[],
        )
        self._episodes_entity = EntityMetadata(
            entityid="silver.episodes",
            name="episodes",
            merge_columns=["episode_id"],
            tags={"provider_type": "delta", "temporal.kind": "episodes"},
            schema=episodes_schema(),
            partition_columns=[],
        )

    def get_events_entity(self) -> EntityMetadata:
        return self._events_entity

    def get_conditions_entity(self) -> EntityMetadata:
        return self._conditions_entity

    def get_episodes_entity(self) -> EntityMetadata:
        return self._episodes_entity
