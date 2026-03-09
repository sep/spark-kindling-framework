#!/usr/bin/env python3
"""System test app for default EntityNameMapper behavior."""

import sys

from kindling.data_entities import EntityMetadata, EntityNameMapper
from kindling.entity_provider_registry import EntityProviderRegistry
from kindling.injection import get_kindling_service
from kindling.spark_config import ConfigService
from kindling.spark_log_provider import SparkLoggerProvider
from kindling.spark_session import get_or_create_spark_session
from pyspark.sql.types import StringType, StructField, StructType


def _emit(logger, test_id: str, test_name: str, passed: bool, details: str = "") -> None:
    status = "PASSED" if passed else "FAILED"
    suffix = f" {details}" if details else ""
    line = f"TEST_ID={test_id} test={test_name} status={status}{suffix}"
    logger.info(line)
    print(line, flush=True)


def _quote_table_identifier(table_name: str) -> str:
    parts = [part.strip() for part in table_name.split(".") if part.strip()]
    if not parts:
        raise ValueError("Table name cannot be empty")
    return ".".join([f"`{part.replace('`', '``')}`" for part in parts])


def _get_current_namespace(spark):
    catalog = None
    schema = None

    try:
        row = spark.sql("SELECT current_catalog() AS catalog, current_database() AS schema").first()
        if row is not None:
            catalog = row["catalog"]
            schema = row["schema"]
    except Exception:
        try:
            row = spark.sql("SELECT current_database() AS schema").first()
            if row is not None:
                schema = row["schema"]
        except Exception:
            pass

    return catalog, schema


def _make_entity(entityid: str, schema: StructType) -> EntityMetadata:
    return EntityMetadata(
        entityid=entityid,
        name=entityid.split(".")[-1],
        partition_columns=[],
        merge_columns=[],
        tags={
            "provider_type": "delta",
            "provider.access_mode": "forName",
        },
        schema=schema,
    )


def main() -> int:
    config_service = get_kindling_service(ConfigService)
    logger_provider = get_kindling_service(SparkLoggerProvider)
    logger = logger_provider.get_logger("name-mapper-test-app")
    spark = get_or_create_spark_session()

    test_id = str(config_service.get("test_id") or "unknown")
    start_line = f"TEST_ID={test_id} status=STARTED component=name_mapper"
    logger.info(start_line)
    print(start_line, flush=True)

    mapper = get_kindling_service(EntityNameMapper)
    provider_registry = get_kindling_service(EntityProviderRegistry)
    catalog, schema_name = _get_current_namespace(spark)

    if not schema_name:
        _emit(logger, test_id, "namespace_resolution", False, "schema=None")
        final_line = f"TEST_ID={test_id} status=COMPLETED result=FAILED"
        logger.info(final_line)
        print(final_line, flush=True)
        return 1

    _emit(
        logger,
        test_id,
        "namespace_resolution",
        True,
        f"catalog={catalog} schema={schema_name}",
    )

    row_schema = StructType(
        [
            StructField("id", StringType(), False),
            StructField("value", StringType(), True),
        ]
    )
    base_leaf = f"name_mapper_{str(test_id).replace('-', '_')}"
    cleanup_tables = []
    test_results = []

    def _run_case(test_prefix: str, entityid: str, expected_table_name: str) -> None:
        entity = _make_entity(entityid=entityid, schema=row_schema)
        provider = provider_registry.get_provider_for_entity(entity)
        resolved_table_name = mapper.get_table_name(entity)

        mapping_ok = resolved_table_name == expected_table_name
        _emit(
            logger,
            test_id,
            f"{test_prefix}_mapping",
            mapping_ok,
            f"resolved={resolved_table_name} expected={expected_table_name}",
        )
        test_results.append(mapping_ok)

        if not mapping_ok:
            return

        cleanup_tables.append(expected_table_name)

        df = spark.createDataFrame(
            [(f"{test_prefix}_{test_id}", f"value_{test_prefix}")],
            schema=row_schema,
        )
        provider.write_to_entity(df, entity)
        exists = provider.check_entity_exists(entity)
        count = spark.read.table(expected_table_name).count()

        write_ok = exists and count >= 1
        _emit(
            logger,
            test_id,
            f"{test_prefix}_write",
            write_ok,
            f"table={expected_table_name} count={count}",
        )
        test_results.append(write_ok)

    try:
        two_part_leaf = f"{base_leaf}_two_part"
        two_part_entityid = f"{schema_name}.{two_part_leaf}"
        if catalog:
            expected_two_part = f"{catalog}.{schema_name}.{two_part_leaf}"
        else:
            expected_two_part = f"{schema_name}.{two_part_leaf}"
        _run_case("two_part_name", two_part_entityid, expected_two_part)

        if catalog:
            three_part_leaf = f"{base_leaf}_three_part"
            three_part_entityid = f"{catalog}.{schema_name}.{three_part_leaf}"
            expected_three_part = f"{catalog}.{schema_name}.{three_part_leaf}"
            _run_case("three_part_name", three_part_entityid, expected_three_part)

        passed = all(test_results)
        final_result = "PASSED" if passed else "FAILED"
        final_line = f"TEST_ID={test_id} status=COMPLETED result={final_result}"
        logger.info(final_line)
        print(final_line, flush=True)
        return 0 if passed else 1
    finally:
        for table_name in cleanup_tables:
            try:
                spark.sql(f"DROP TABLE IF EXISTS {_quote_table_identifier(table_name)}")
            except Exception as cleanup_error:
                logger.warning(f"Cleanup failed for table '{table_name}': {cleanup_error}")


if __name__ == "__main__":
    sys.exit(main())
