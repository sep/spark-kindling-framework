#!/usr/bin/env python3
"""Provenance stamping + runtime manifest system test app.

Runs a real batch pipe through the executor into an SCD2 entity with
provenance stamping on, on whatever platform the job runs, and checks:

- runtime_manifest_populated: bootstrap built the runtime manifest
  (run_id, platform, Kindling version, fingerprints);
- provenance_stamped: every output row carries the stamp of the pipe that
  wrote it, with the manifest's run_id and the executor's execution id;
- provenance_only_difference_no_version: re-running the pipe on unchanged
  data opens no SCD2 version and leaves the stamps as they were;
- scd2_close_keeps_stamp: a real change closes the current version, which
  keeps its stamp, and the new version carries the new write's stamp.

Emits TEST_ID= markers for the system test harness.
"""

import sys
import traceback

from kindling.data_entities import DataEntities, DataEntityRegistry, EntityNameMapper
from kindling.data_pipes import DataPipes, DataPipesExecution
from kindling.entity_provider_registry import EntityProviderRegistry
from kindling.injection import GlobalInjector, get_kindling_service
from kindling.platform_provider import PlatformServiceProvider
from kindling.runtime_manifest import get_runtime_manifest
from kindling.spark_config import ConfigService
from kindling.spark_session import get_or_create_spark_session
from pyspark.sql.types import StringType, StructField, StructType

config_service = get_kindling_service(ConfigService)
test_id = str(config_service.get("test_id") or "unknown").replace("-", "_")
platform_service = get_kindling_service(PlatformServiceProvider).get_service()
platform_name = platform_service.get_platform_name() if platform_service else "unknown"
print(f"TEST_ID={test_id} status=STARTED platform={platform_name}", flush=True)

spark = get_or_create_spark_session()
results = {}


def _check(name, passed, detail=""):
    results[name] = bool(passed)
    status = "PASSED" if passed else "FAILED"
    suffix = f" {detail}" if detail else ""
    print(f"TEST_ID={test_id} test={name} status={status}{suffix}", flush=True)


SCHEMA = StructType(
    [
        StructField("id", StringType(), False),
        StructField("status", StringType(), True),
    ]
)
# One-part entity ids: each platform's storage config qualifies them (a
# two-part id would need that schema to exist in the lakehouse/catalog).
source_id = f"prov_source_{test_id}"
output_id = f"prov_output_{test_id}"
pipe_id = f"prov_pipe_{test_id}"

DataEntities.entity(
    entityid=source_id,
    name=f"prov_source_{test_id}",
    merge_columns=["id"],
    tags={"provider_type": "delta"},
    schema=SCHEMA,
)
DataEntities.entity(
    entityid=output_id,
    name=f"prov_output_{test_id}",
    merge_columns=["id"],
    tags={
        "provider_type": "delta",
        "scd.type": "2",
        "provenance.enabled": "true",
        "provenance.pipe_tags": "owner",
    },
    schema=SCHEMA,
)


@DataPipes.pipe(
    pipeid=pipe_id,
    name="Provenance system test pipe",
    tags={"owner": "systest"},
    input_entity_ids=[source_id],
    output_entity_id=output_id,
    output_type="delta",
)
def build_output(**inputs):
    return next(iter(inputs.values()))


registry = GlobalInjector.get(DataEntityRegistry)
providers = GlobalInjector.get(EntityProviderRegistry)
source = registry.get_entity_definition(source_id)
output = registry.get_entity_definition(output_id)
source_provider = providers.get_provider_for_entity(source)
output_provider = providers.get_provider_for_entity(output)
executor = get_kindling_service(DataPipesExecution)


def _output_rows():
    return output_provider.read_entity(output).collect()


exit_code = 0
try:
    manifest = get_runtime_manifest()
    _check(
        "runtime_manifest_populated",
        manifest is not None
        and manifest.run_id
        and manifest.platform
        and manifest.kindling_version
        and manifest.runtime_fingerprint
        and manifest.package_fingerprint,
        manifest.summary() if manifest is not None else "no manifest",
    )

    source_provider.merge_to_entity(
        spark.createDataFrame([("a", "new"), ("b", "new")], SCHEMA), source
    )
    executor.run_datapipes([pipe_id])
    rows = _output_rows()
    stamps = [row["__provenance"] for row in rows]
    first_persist_ids = {row["id"]: row["__provenance"]["persist_id"] for row in rows}
    _check(
        "provenance_stamped",
        len(rows) == 2
        and all(
            stamp is not None
            and stamp["pipe_id"] == pipe_id
            and stamp["mode"] == "batch"
            and stamp["run_id"] == manifest.run_id
            and stamp["execution_id"] in manifest.execution_ids
            and stamp["persist_id"]
            and stamp["tags"] == {"owner": "systest"}
            and stamp["input_entity_ids"] == [source_id]
            for stamp in stamps
        ),
        f"stamps={[s.asDict(recursive=True) if s is not None else None for s in stamps]}",
    )

    executor.run_datapipes([pipe_id])
    rows = _output_rows()
    _check(
        "provenance_only_difference_no_version",
        len(rows) == 2
        and {row["id"]: row["__provenance"]["persist_id"] for row in rows} == first_persist_ids,
        f"rows={len(rows)}",
    )

    source_provider.merge_to_entity(spark.createDataFrame([("a", "shipped")], SCHEMA), source)
    executor.run_datapipes([pipe_id])
    versions = [row for row in _output_rows() if row["id"] == "a"]
    closed = [row for row in versions if not row["__is_current"]]
    current = [row for row in versions if row["__is_current"]]
    _check(
        "scd2_close_keeps_stamp",
        len(closed) == 1
        and len(current) == 1
        and closed[0]["__provenance"]["persist_id"] == first_persist_ids["a"]
        and current[0]["status"] == "shipped"
        and current[0]["__provenance"]["persist_id"] != first_persist_ids["a"]
        and current[0]["__provenance"]["run_id"] == manifest.run_id,
        f"versions={len(versions)}",
    )
except Exception as exc:  # noqa: BLE001 - reported through the markers
    print(f"TEST_ID={test_id} error={exc}", flush=True)
    traceback.print_exc()
    exit_code = 1
finally:
    # Catalog tables outlive the storage cleanup; drop them here.
    name_mapper = GlobalInjector.get(EntityNameMapper)
    for entity in (output, source):
        try:
            spark.sql(f"DROP TABLE IF EXISTS {name_mapper.get_table_name(entity)}")
        except Exception as cleanup_exc:  # noqa: BLE001 - best effort
            print(f"TEST_ID={test_id} table cleanup warning: {cleanup_exc}", flush=True)

overall = bool(results) and all(results.values()) and exit_code == 0
print(f"TEST_ID={test_id} status=COMPLETED result={'PASSED' if overall else 'FAILED'}", flush=True)
if not overall:
    sys.exit(1)
