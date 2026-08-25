"""Integration test: a real WatermarkAspect-driven read against a real
MemoryEntityProvider-backed entity fed by two independent writers.

This is the temporal extension's own "fan-in" pattern: several
``register_base_event`` pipes append into one shared events entity
configured with ``provider_type: memory``, and a downstream reader (e.g. a
``ConditionEngine``, whose ``use_watermark`` defaults to ``True`` in
``kindling_ext_temporal.registry.ConditionEngineMetadata``) reads it with
watermarking enabled. Neither the temporal extension's own unit tests
(mocked registries, no real provider) nor its integration/system tests
(deliberately "real providers" == Delta) ever exercised this combination,
so the AttributeError this guards against (`_legacy_version_read` calling
`get_entity_version` on a provider that doesn't have it) reached production
before landing here.

Exercises the full real signal-wiring path (WatermarkAspect.read.resolve_read
-> WatermarkManager.read_changes -> _legacy_version_read) against a real
MemoryEntityProvider and a real local Spark session -- not just the
isolated unit-level fallback logic in test_watermarking_legacy_version_read.py.
"""

from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from kindling.data_entities import EntityMetadata
from kindling.entity_provider_memory import MemoryEntityProvider
from kindling.signaling import BlinkerSignalProvider
from kindling.spark_log_provider import PythonLoggerProvider
from kindling.watermarking import (
    SimpleWatermarkEntityFinder,
    WatermarkAspect,
    WatermarkManager,
)
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructField, StructType


@pytest.fixture(scope="module")
def spark():
    session = (
        SparkSession.builder.appName("WatermarkMemoryProviderFanIn")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


@pytest.fixture
def logger_provider():
    provider = MagicMock(spec=PythonLoggerProvider)
    provider.get_logger.return_value = MagicMock()
    return provider


@pytest.fixture
def memory_provider(spark, logger_provider, monkeypatch):
    monkeypatch.setattr(
        "kindling.entity_provider_memory.get_or_create_spark_session", lambda: spark
    )
    return MemoryEntityProvider(logger_provider)


def _fan_in_events_entity() -> EntityMetadata:
    schema = StructType(
        [
            StructField("subject_id", StringType(), False),
            StructField("event_type", StringType(), False),
        ]
    )
    return EntityMetadata(
        entityid="temporal.fanin.events",
        name="Fan-in events",
        merge_columns=["subject_id", "event_type"],
        tags={"provider_type": "memory"},
        schema=schema,
    )


def test_watermarked_read_against_memory_fan_in_entity_does_not_crash(
    spark, memory_provider, logger_provider, monkeypatch
):
    """Two independent "base event" writers append into the same
    memory-backed entity (fan-in); a downstream reader with
    use_watermark=True (the temporal extension's ConditionEngine default)
    must get a real full read, not an AttributeError."""
    monkeypatch.setattr("kindling.watermarking.get_or_create_spark_session", lambda: spark)

    entity = _fan_in_events_entity()
    memory_provider.append_to_entity(
        spark.createDataFrame([("subj-1", "started")], schema=entity.schema), entity
    )
    memory_provider.append_to_entity(
        spark.createDataFrame([("subj-1", "completed")], schema=entity.schema), entity
    )

    provider_registry = MagicMock()
    provider_registry.get_provider_for_entity.return_value = memory_provider

    wms = WatermarkManager(
        ep=memory_provider,
        wef=SimpleWatermarkEntityFinder(),
        lp=logger_provider,
        signal_provider=None,
        provider_registry=provider_registry,
    )

    signal_provider = BlinkerSignalProvider()
    aspect = WatermarkAspect(wms=wms, lp=logger_provider, signal_provider=signal_provider)
    aspect.register()

    pipe = SimpleNamespace(
        pipeid="pipe.condition_engine",
        name="condition_engine",
        input_entity_ids=[entity.entityid],
    )
    signal = signal_provider.get_signal("read.resolve_read") or signal_provider.create_signal(
        "read.resolve_read"
    )
    results = signal.send(None, entity=entity, pipe=pipe, use_watermark=True)

    resolved = [retval for _, retval in results if retval is not None]
    assert len(resolved) == 1
    assert resolved[0].df.count() == 2
