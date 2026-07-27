# Databricks notebook source
#
# Run this cell after kindling.initialize(...).  It performs only local/runtime
# checks: it does not read an Event Hub, enumerate secrets, install packages,
# write data, or create tables.

from __future__ import annotations

import json
import re
import sys
import time
from collections.abc import Mapping

from pyspark.sql import SparkSession

import kindling
from kindling.data_entities import DataEntityRegistry
from kindling.data_pipes import DataPipesRegistry
from kindling.entity_provider_eventhub import EventHubEntityProvider
from kindling.entity_provider_registry import EntityProviderRegistry
from kindling.injection import GlobalInjector, get_kindling_service
from kindling.platform_provider import PlatformServiceProvider
from kindling.spark_config import ConfigService

_SENSITIVE_KEY = re.compile(
    r"(?:secret|password|token|credential|private|access.?key|connection.?string|sas)",
    re.IGNORECASE,
)
_SENSITIVE_VALUE = re.compile(
    r"(?P<name>SharedAccessKey|sig|token|password|client_secret)=(?P<value>[^;,&\s]+)",
    re.IGNORECASE,
)


def _redact(key: str, value):
    """Return a printable value without exposing credentials or secret values."""
    if _SENSITIVE_KEY.search(str(key)):
        return "<redacted>"
    if isinstance(value, Mapping):
        return {str(k): _redact(f"{key}.{k}", v) for k, v in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_redact(key, item) for item in value]
    if isinstance(value, str):
        if value.startswith("@secret"):
            return "<secret reference>"
        return _SENSITIVE_VALUE.sub(r"\g<name>=<redacted>", value)
    return value


def _print_section(title: str):
    print(f"\n=== {title} ===")


def _safe_call(label: str, fn):
    started = time.perf_counter()
    try:
        value = fn()
        elapsed = time.perf_counter() - started
        print(f"PASS {label} ({elapsed:.3f}s)")
        return value
    except Exception as exc:
        elapsed = time.perf_counter() - started
        print(f"FAIL {label} ({elapsed:.3f}s): {type(exc).__name__}: {exc}")
        return None


def _spark_conf(key: str, default: str = "<unset>") -> str:
    """Read Spark configuration without touching the unsupported SparkContext."""
    try:
        return spark.conf.get(key, default)
    except Exception:
        return default


spark = _safe_call("active Spark session", SparkSession.getActiveSession)
if spark is None:
    raise RuntimeError("No active Spark session was found after Kindling initialization")

_print_section("Runtime")
runtime = {
    "python": sys.version.split()[0],
    "kindling": getattr(kindling, "__version__", "<version unavailable>"),
    "spark": spark.version,
    "master": _spark_conf("spark.master"),
    "platform": _spark_conf("kindling.platform.name"),
    "environment": _spark_conf("environment"),
    "databricks_runtime": _spark_conf("spark.databricks.clusterUsageTags.sparkVersion", "<unset>"),
}
for key, value in runtime.items():
    print(f"{key}: {value}")

_print_section("Kindling services")
print(f"framework_initialized: {kindling.is_framework_initialized()}")

platform_provider = _safe_call(
    "platform service lookup", lambda: get_kindling_service(PlatformServiceProvider)
)
if platform_provider is not None:
    platform_service = platform_provider.get_service()
    print(f"platform_service: {type(platform_service).__name__}")

config_service = _safe_call("config service lookup", lambda: get_kindling_service(ConfigService))
if config_service is not None:
    print("\nNon-sensitive configuration:")
    config = config_service.get_all()
    for key in sorted(config, key=str):
        safe_value = _redact(str(key), config[key])
        try:
            rendered = json.dumps(safe_value, default=str, sort_keys=True)
        except Exception:
            rendered = repr(safe_value)
        print(f"{key} = {rendered}")

_print_section("Registries")
provider_registry = _safe_call(
    "entity provider registry lookup", lambda: GlobalInjector.get(EntityProviderRegistry)
)
if provider_registry is not None:
    providers = provider_registry.list_registered_providers()
    print(f"providers: {providers}")

entity_registry = _safe_call(
    "entity registry lookup", lambda: GlobalInjector.get(DataEntityRegistry)
)
if entity_registry is not None:
    entity_ids = sorted(str(entity_id) for entity_id in entity_registry.get_entity_ids())
    print(f"entities ({len(entity_ids)}): {entity_ids}")

pipe_registry = _safe_call("pipe registry lookup", lambda: GlobalInjector.get(DataPipesRegistry))
if pipe_registry is not None:
    pipe_ids = sorted(str(pipe_id) for pipe_id in pipe_registry.get_pipe_ids())
    print(f"pipes ({len(pipe_ids)}): {pipe_ids}")

_print_section("Provider construction")
eventhub_provider = _safe_call(
    "Event Hub provider construction",
    lambda: provider_registry.get_provider("eventhub") if provider_registry else None,
)
if isinstance(eventhub_provider, EventHubEntityProvider):
    print(f"eventhub_provider_platform: {eventhub_provider.platform}")
    print("eventhub_provider_network_read: not run")

_print_section("Optional storage checks")
print("No storage paths were probed by default.")
print(
    "To test a specific approved path without reading data, run separately: "
    "dbutils.fs.ls(<path>)"
)

print("\nSmoke test complete: initialization state and local registrations inspected.")
