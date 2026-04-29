from unittest.mock import MagicMock

from pyspark.sql.types import (
    BooleanType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from kindling.data_entities import EntityMetadata, SCDConfig
from kindling.entity_provider_delta import (
    DeltaAccessMode,
    DeltaEntityProvider,
    DeltaTableReference,
)


def _provider():
    provider = DeltaEntityProvider.__new__(DeltaEntityProvider)
    provider.config = MagicMock()
    provider.config.get.return_value = None
    provider.logger = MagicMock()
    provider.spark = MagicMock()
    provider._get_cluster_columns = MagicMock(return_value=[])
    provider._should_partition_files = MagicMock(return_value=False)
    return provider


def _scd_config(**overrides):
    values = {
        "enabled": True,
        "tracked_columns": None,
        "effective_from_column": "__effective_from",
        "effective_to_column": "__effective_to",
        "is_current_column": "__is_current",
        "current_entity_id": "orders.current",
        "routing_key_method": "hash",
    }
    values.update(overrides)
    return SCDConfig(**values)


def _schema():
    return StructType([StructField("id", StringType(), False)])


def _field(schema, name):
    return next(field for field in schema.fields if field.name == name)


def _entity(tags=None):
    return EntityMetadata(
        entityid="orders",
        name="orders",
        merge_columns=["id"],
        tags=tags or {},
        schema=_schema(),
    )


def test_augment_schema_adds_all_three_temporal_columns():
    augmented = _provider()._augment_schema_for_scd2(_schema(), _scd_config())

    assert {field.name for field in augmented.fields} == {
        "id",
        "__effective_from",
        "__effective_to",
        "__is_current",
    }


def test_augment_schema_skips_columns_already_in_schema():
    schema = StructType(
        [
            StructField("id", StringType(), False),
            StructField("__effective_from", TimestampType(), False),
        ]
    )

    augmented = _provider()._augment_schema_for_scd2(schema, _scd_config())

    assert [field.name for field in augmented.fields].count("__effective_from") == 1
    assert {field.name for field in augmented.fields} == {
        "id",
        "__effective_from",
        "__effective_to",
        "__is_current",
    }


def test_augment_schema_effective_from_is_not_nullable():
    augmented = _provider()._augment_schema_for_scd2(_schema(), _scd_config())

    field = _field(augmented, "__effective_from")
    assert isinstance(field.dataType, TimestampType)
    assert field.nullable is False


def test_augment_schema_effective_to_is_nullable():
    augmented = _provider()._augment_schema_for_scd2(_schema(), _scd_config())

    field = _field(augmented, "__effective_to")
    assert isinstance(field.dataType, TimestampType)
    assert field.nullable is True


def test_augment_schema_is_current_is_not_nullable():
    augmented = _provider()._augment_schema_for_scd2(_schema(), _scd_config())

    field = _field(augmented, "__is_current")
    assert isinstance(field.dataType, BooleanType)
    assert field.nullable is False


def test_augment_schema_uses_custom_column_names_from_config():
    cfg = _scd_config(
        effective_from_column="valid_from",
        effective_to_column="valid_to",
        is_current_column="current_flag",
    )

    augmented = _provider()._augment_schema_for_scd2(_schema(), cfg)

    assert {field.name for field in augmented.fields} == {
        "id",
        "valid_from",
        "valid_to",
        "current_flag",
    }


def test_create_physical_table_augments_schema_when_scd2_enabled():
    provider = _provider()
    entity = _entity({"scd.type": "2"})
    table_ref = DeltaTableReference.__new__(DeltaTableReference)
    table_ref.table_path = "/tmp/orders"
    table_ref.table_name = None
    table_ref.access_mode = DeltaAccessMode.STORAGE

    provider._create_physical_table(entity, table_ref)

    schema = provider.spark.createDataFrame.call_args.kwargs["schema"]
    assert "__effective_from" in schema.names
    assert "__effective_to" in schema.names
    assert "__is_current" in schema.names


def test_create_physical_table_no_augment_when_scd2_disabled():
    provider = _provider()
    entity = _entity()
    table_ref = DeltaTableReference.__new__(DeltaTableReference)
    table_ref.table_path = "/tmp/orders"
    table_ref.table_name = None
    table_ref.access_mode = DeltaAccessMode.STORAGE

    provider._create_physical_table(entity, table_ref)

    schema = provider.spark.createDataFrame.call_args.kwargs["schema"]
    assert schema.names == ["id"]
