"""
Unit tests for the derived-dataset declaration surface.

A derived dataset (``dataset.kind: derived``) has no independent state —
its contents are a pure function of its inputs, so writes replace rather
than evolve. The derived and state vocabularies are mutually exclusive:
``write.mode`` and ``scd.*`` are rejected on a derived entity at
registration time, as is ``derived.replace_keys`` without the kind tag.
"""

import pytest
from kindling.data_entities import (
    DataEntityManager,
    DerivedConfig,
    EntityMetadata,
    derived_config_from_tags,
)
from pyspark.sql.types import StringType, StructField, StructType


def _entity(tags=None, schema=None, merge_columns=None):
    return EntityMetadata(
        "gold.summary",
        name="Summary",
        tags=tags or {},
        schema=schema,
        merge_columns=merge_columns,
    )


def _register(manager, tags=None, schema=None, merge_columns=None):
    manager.register_entity(
        "gold.summary",
        name="Summary",
        tags=tags or {},
        schema=schema,
        merge_columns=merge_columns,
    )


class TestDerivedConfigFromTags:
    def test_absent_kind_is_disabled(self):
        cfg = derived_config_from_tags(_entity())
        assert cfg == DerivedConfig(enabled=False, replace_keys=None)

    def test_derived_kind_enables(self):
        cfg = derived_config_from_tags(_entity(tags={"dataset.kind": "derived"}))
        assert cfg.enabled is True
        assert cfg.replace_keys is None

    def test_replace_keys_parse_and_strip(self):
        cfg = derived_config_from_tags(
            _entity(tags={"dataset.kind": "derived", "derived.replace_keys": " run_id , site "})
        )
        assert cfg.replace_keys == ["run_id", "site"]


class TestDerivedRegistrationValidation:
    def test_plain_derived_registers(self):
        manager = DataEntityManager()
        _register(manager, tags={"dataset.kind": "derived"})
        assert "gold.summary" in manager.registry

    def test_invalid_kind_rejected(self):
        with pytest.raises(ValueError, match="invalid dataset.kind"):
            _register(DataEntityManager(), tags={"dataset.kind": "materialized"})

    def test_derived_rejects_write_mode(self):
        with pytest.raises(ValueError, match="write.mode does not apply"):
            _register(
                DataEntityManager(),
                tags={"dataset.kind": "derived", "write.mode": "merge"},
            )

    def test_derived_rejects_scd(self):
        with pytest.raises(ValueError, match="scd.type does not apply"):
            _register(
                DataEntityManager(),
                tags={"dataset.kind": "derived", "scd.type": "2"},
                merge_columns=["id"],
            )

    def test_replace_keys_require_derived_kind(self):
        with pytest.raises(ValueError, match="requires\\s+dataset.kind='derived'"):
            _register(DataEntityManager(), tags={"derived.replace_keys": "run_id"})

    def test_replace_keys_must_exist_in_declared_schema(self):
        schema = StructType([StructField("value", StringType())])
        with pytest.raises(ValueError, match="not in entity schema.*run_id"):
            _register(
                DataEntityManager(),
                tags={"dataset.kind": "derived", "derived.replace_keys": "run_id"},
                schema=schema,
            )

    def test_replace_keys_in_schema_register(self):
        schema = StructType(
            [StructField("run_id", StringType()), StructField("value", StringType())]
        )
        manager = DataEntityManager()
        _register(
            manager,
            tags={"dataset.kind": "derived", "derived.replace_keys": "run_id"},
            schema=schema,
        )
        assert "gold.summary" in manager.registry

    def test_replace_keys_without_schema_register(self):
        # Schema-less entities defer column checks to write time.
        manager = DataEntityManager()
        _register(manager, tags={"dataset.kind": "derived", "derived.replace_keys": "run_id"})
        assert "gold.summary" in manager.registry


class TestInsertWriteModeValidation:
    def test_insert_requires_merge_columns(self):
        with pytest.raises(ValueError, match="'insert' requires\\s+merge_columns"):
            _register(DataEntityManager(), tags={"write.mode": "insert"})

    def test_insert_with_merge_columns_registers(self):
        manager = DataEntityManager()
        _register(manager, tags={"write.mode": "insert"}, merge_columns=["event_id"])
        assert "gold.summary" in manager.registry

    def test_insert_contradicts_scd(self):
        with pytest.raises(ValueError, match="'insert' contradicts\\s+scd.type"):
            _register(
                DataEntityManager(),
                tags={"write.mode": "insert", "scd.type": "2"},
                merge_columns=["event_id"],
            )

    def test_unknown_write_mode_still_rejected(self):
        with pytest.raises(ValueError, match="invalid write.mode"):
            _register(DataEntityManager(), tags={"write.mode": "overwrite"})
