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


def _emit(logger, test_id: str, test_name: str, passed: bool, details: str = "") -> str:
    status = "PASSED" if passed else "FAILED"
    suffix = f" {details}" if details else ""
    line = f"TEST_ID={test_id} test={test_name} status={status}{suffix}"
    logger.info(line)
    print(line, flush=True)
    return line


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


def _extract_schema_name(row) -> str | None:
    """Best-effort schema name extraction across Spark row shapes."""
    for key in ("namespace", "databaseName", "schemaName"):
        try:
            value = row[key]
        except Exception:
            value = getattr(row, key, None)
        if value:
            return str(value)
    return None


def _list_schemas(spark, catalog: str | None) -> set[str]:
    queries = []
    if catalog:
        quoted_catalog = _quote_table_identifier(catalog)
        queries.extend(
            [
                f"SHOW SCHEMAS IN {quoted_catalog}",
                f"SHOW DATABASES IN {quoted_catalog}",
            ]
        )
    else:
        queries.extend(["SHOW SCHEMAS", "SHOW DATABASES"])

    for query in queries:
        try:
            rows = spark.sql(query).collect()
        except Exception:
            continue

        schemas = {
            schema_name
            for schema_name in (_extract_schema_name(row) for row in rows)
            if schema_name
        }
        if schemas:
            return schemas

    return set()


def _resolve_write_schema(spark, catalog: str | None, current_schema: str | None) -> str | None:
    schemas = _list_schemas(spark, catalog)
    if not schemas:
        return current_schema

    preferred = []
    if current_schema:
        preferred.append(current_schema)
    preferred.extend(["kindling", "test", "default"])

    for schema_name in preferred:
        if schema_name in schemas:
            return schema_name

    for schema_name in sorted(schemas):
        if schema_name.lower() != "information_schema":
            return schema_name

    return current_schema


def _extract_volume_namespace(path: str | None) -> tuple[str | None, str | None]:
    if not path:
        return None, None

    normalized = str(path).strip().rstrip("/")
    if not normalized.startswith("/Volumes/"):
        return None, None

    parts = [part for part in normalized.split("/") if part]
    if len(parts) < 4:
        return None, None

    return parts[1], parts[2]


def _get_target_namespace(config_service, current_catalog: str | None, current_schema: str | None):
    table_root = config_service.get("kindling.storage.table_root")
    checkpoint_root = config_service.get("kindling.storage.checkpoint_root")

    for candidate in (table_root, checkpoint_root):
        catalog, schema = _extract_volume_namespace(candidate)
        if catalog and schema:
            return catalog, schema

    return current_catalog, current_schema


def _has_explicit_namespace_config(config_service) -> bool:
    for key in (
        "kindling.storage.table_catalog",
        "kindling.storage.table_schema",
        "kindling.databricks.catalog",
        "kindling.databricks.schema",
        "kindling.fabric.catalog",
        "kindling.fabric.schema",
        "kindling.synapse.schema",
    ):
        value = config_service.get(key)
        if value is None:
            continue
        cleaned = str(value).strip()
        if cleaned and cleaned.lower() not in {"auto", "none", "null"}:
            return True
    return False


def _clean_config_namespace_value(value: object) -> str | None:
    if value is None:
        return None
    cleaned = str(value).strip()
    if not cleaned or cleaned.lower() in {"auto", "none", "null"}:
        return None
    return cleaned


def _get_config_namespace(config_service) -> tuple[str | None, str | None]:
    catalog = _clean_config_namespace_value(config_service.get("kindling.storage.table_catalog"))
    schema = _clean_config_namespace_value(config_service.get("kindling.storage.table_schema"))

    if catalog is None:
        catalog = _clean_config_namespace_value(
            config_service.get("kindling.databricks.catalog")
        ) or _clean_config_namespace_value(config_service.get("kindling.fabric.catalog"))
    if schema is None:
        schema = (
            _clean_config_namespace_value(config_service.get("kindling.databricks.schema"))
            or _clean_config_namespace_value(config_service.get("kindling.fabric.schema"))
            or _clean_config_namespace_value(config_service.get("kindling.synapse.schema"))
        )

    return catalog, schema


def _use_namespace(spark, catalog: str | None, schema: str | None) -> tuple[str | None, str | None]:
    if catalog:
        spark.sql(f"USE CATALOG {_quote_table_identifier(catalog)}")
    if schema:
        spark.sql(f"USE {_quote_table_identifier(schema)}")
    return _get_current_namespace(spark)


def _build_entity_case(
    leaf_name: str,
    write_schema: str,
    catalog: str | None,
    use_config_namespace: bool,
    config_catalog: str | None = None,
    config_schema: str | None = None,
) -> tuple[str, str]:
    if use_config_namespace:
        entity_id = leaf_name
        if config_catalog and config_schema:
            expected_table_name = f"{config_catalog}.{config_schema}.{leaf_name}"
        elif config_schema:
            expected_table_name = f"{config_schema}.{leaf_name}"
        elif catalog:
            expected_table_name = f"{catalog}.{write_schema}.{leaf_name}"
        else:
            expected_table_name = f"{write_schema}.{leaf_name}"
    else:
        entity_id = f"{write_schema}.{leaf_name}"
        if catalog:
            expected_table_name = f"{catalog}.{write_schema}.{leaf_name}"
        else:
            expected_table_name = f"{write_schema}.{leaf_name}"

    return entity_id, expected_table_name


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
    platform = str(config_service.get("platform") or "").strip().lower()
    target_catalog, target_schema = _get_target_namespace(config_service, catalog, schema_name)
    use_config_namespace = _has_explicit_namespace_config(config_service)
    config_catalog, config_schema = _get_config_namespace(config_service)

    if platform == "databricks" and (target_catalog, target_schema) != (catalog, schema_name):
        catalog, schema_name = _use_namespace(spark, target_catalog, target_schema)

    write_schema = _resolve_write_schema(spark, catalog, schema_name)

    if not write_schema:
        _emit(logger, test_id, "namespace_resolution", False, "schema=None write_schema=None")
        final_line = f"TEST_ID={test_id} status=COMPLETED result=FAILED"
        logger.info(final_line)
        print(final_line, flush=True)
        return 1

    row_schema = StructType(
        [
            StructField("id", StringType(), False),
            StructField("value", StringType(), True),
        ]
    )
    base_leaf = f"name_mapper_{str(test_id).replace('-', '_')}"
    cleanup_tables = []
    test_results = []
    emitted_lines = []
    emitted_lines.append(
        _emit(
            logger,
            test_id,
            "namespace_resolution",
            True,
            f"catalog={catalog} schema={schema_name} write_schema={write_schema}",
        )
    )

    def _run_case(test_prefix: str, entityid: str, expected_table_name: str) -> None:
        entity = _make_entity(entityid=entityid, schema=row_schema)
        provider = provider_registry.get_provider_for_entity(entity)
        resolved_table_name = mapper.get_table_name(entity)

        mapping_ok = resolved_table_name == expected_table_name
        emitted_lines.append(
            _emit(
                logger,
                test_id,
                f"{test_prefix}_mapping",
                mapping_ok,
                f"resolved={resolved_table_name} expected={expected_table_name}",
            )
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
        emitted_lines.append(
            _emit(
                logger,
                test_id,
                f"{test_prefix}_write",
                write_ok,
                f"table={expected_table_name} count={count}",
            )
        )
        test_results.append(write_ok)

    try:
        two_part_leaf = f"{base_leaf}_two_part"
        two_part_entityid, expected_two_part = _build_entity_case(
            leaf_name=two_part_leaf,
            write_schema=write_schema,
            catalog=catalog,
            use_config_namespace=use_config_namespace,
            config_catalog=config_catalog,
            config_schema=config_schema,
        )
        _run_case("two_part_name", two_part_entityid, expected_two_part)

        if catalog:
            three_part_leaf = f"{base_leaf}_three_part"
            three_part_entityid = f"{catalog}.{write_schema}.{three_part_leaf}"
            expected_three_part = f"{catalog}.{write_schema}.{three_part_leaf}"
            _run_case("three_part_name", three_part_entityid, expected_three_part)

        for line in emitted_lines:
            logger.info(line)
            print(line, flush=True)

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
