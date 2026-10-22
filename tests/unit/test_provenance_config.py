"""Provenance stamping: tags, the kindling.provenance.* settings defaults and
registration validation. Pure metadata, no Spark session; the column
builder and the write paths are covered in test_provenance.py and
tests/integration."""

from unittest.mock import MagicMock

import pytest
from kindling.data_entities import (
    DataEntityManager,
    EntityMetadata,
    ProvenanceConfig,
    provenance_column,
    provenance_config_from_tags,
    provenance_kept_inputs,
)
from kindling.data_pipes import PipeMetadata
from pyspark.sql.types import StringType, StructField, StructType

SCHEMA = StructType(
    [
        StructField("customer_id", StringType(), False),
        StructField("status", StringType(), True),
    ]
)


def _config_service(values):
    """Stub ConfigService answering dotted keys (and overlay sections)."""
    config_service = MagicMock()
    config_service.get.side_effect = lambda key, default=None: values.get(key, default)
    config_service.get_entity_tags.return_value = {}
    return config_service


def _register(manager, entityid="silver.customers", tags=None, **params):
    params.setdefault("merge_columns", ["customer_id"])
    params.setdefault("schema", SCHEMA)
    manager.register_entity(entityid, name=entityid.split(".")[-1], tags=dict(tags or {}), **params)
    return manager.get_entity_definition(entityid)


def _entity(tags, entityid="silver.customers"):
    return EntityMetadata(entityid, "customers", ["customer_id"], dict(tags), SCHEMA)


class TestProvenanceConfigFromTags:
    def test_no_tags_is_disabled_default(self):
        assert provenance_config_from_tags(_entity({})) == ProvenanceConfig()
        assert provenance_column(_entity({})) is None

    def test_parses_enabled_column_and_pipe_tags(self):
        cfg = provenance_config_from_tags(
            _entity(
                {
                    "provenance.enabled": "TRUE",
                    "provenance.column": "_lineage",
                    "provenance.pipe_tags": "domain, owner ,layer.*",
                }
            )
        )
        assert cfg == ProvenanceConfig(
            enabled=True, column="_lineage", pipe_tags=("domain", "owner", "layer.*")
        )
        assert provenance_column(_entity({"provenance.enabled": "true"})) == "__provenance"

    def test_bool_tag_values_parse(self):
        assert provenance_config_from_tags(_entity({"provenance.enabled": True})).enabled

    def test_disabled_column_is_none(self):
        assert provenance_column(_entity({"provenance.enabled": "false"})) is None

    def test_kept_inputs_from_pipe_tags(self):
        pipe = PipeMetadata(
            pipeid="p",
            name="p",
            execute=lambda df: df,
            tags={"provenance.keep_inputs": "bronze.a, bronze.b"},
            input_entity_ids=["bronze.a", "bronze.b"],
            output_entity_id="silver.x",
            output_type="delta",
        )
        assert provenance_kept_inputs(pipe) == ("bronze.a", "bronze.b")
        assert provenance_kept_inputs(None) == ()


class TestSettingsDefaults:
    def test_settings_block_is_baked_into_tags(self):
        manager = DataEntityManager(
            config_service=_config_service(
                {
                    "kindling.provenance.enabled": True,
                    "kindling.provenance.column": "_prov",
                    "kindling.provenance.pipe_tags": ["domain", "layer.*"],
                }
            )
        )
        entity = _register(manager)
        assert entity.tags["provenance.enabled"] == "true"
        assert entity.tags["provenance.column"] == "_prov"
        assert entity.tags["provenance.pipe_tags"] == "domain,layer.*"
        assert provenance_column(entity) == "_prov"

    def test_absent_block_adds_no_tags(self):
        manager = DataEntityManager(config_service=_config_service({}))
        entity = _register(manager)
        assert not any(key.startswith("provenance.") for key in entity.tags)

    def test_unexpected_value_types_are_ignored(self):
        # A MagicMock config service answers every key with a MagicMock.
        manager = DataEntityManager(config_service=MagicMock())
        entity = _register(manager)
        assert not any(key.startswith("provenance.") for key in entity.tags)

    def test_entity_opts_out_against_settings_default(self):
        manager = DataEntityManager(
            config_service=_config_service({"kindling.provenance.enabled": True})
        )
        entity = _register(manager, tags={"provenance.enabled": "false"})
        assert entity.tags["provenance.enabled"] == "false"
        assert provenance_column(entity) is None

    def test_entity_tag_overrides_only_its_own_key(self):
        manager = DataEntityManager(
            config_service=_config_service(
                {"kindling.provenance.enabled": True, "kindling.provenance.column": "_prov"}
            )
        )
        entity = _register(manager, tags={"provenance.column": "_mine"})
        assert provenance_config_from_tags(entity) == ProvenanceConfig(enabled=True, column="_mine")

    def test_overlays_win_over_settings(self):
        manager = DataEntityManager()
        _register(manager)
        manager.apply_config_overrides(
            _config_service(
                {
                    "kindling.provenance.enabled": True,
                    "dataentities": {"silver.customers": {"tags": {"provenance.enabled": "false"}}},
                }
            )
        )
        assert provenance_column(manager.get_entity_definition("silver.customers")) is None

    def test_settings_apply_with_overlay_matchers(self):
        manager = DataEntityManager(
            config_service=_config_service({"kindling.provenance.enabled": True})
        )
        _register(manager)
        manager.apply_config_overrides(
            _config_service(
                {
                    "kindling.provenance.enabled": True,
                    "dataentities": {"silver.*": {"tags": {"layer": "silver"}}},
                }
            )
        )
        entity = manager.get_entity_definition("silver.customers")
        assert entity.tags["layer"] == "silver"
        assert provenance_column(entity) == "__provenance"

    def test_run_scoped_tag_overrides_win(self):
        manager = DataEntityManager(
            config_service=_config_service({"kindling.provenance.enabled": True})
        )
        _register(manager)
        with manager.tag_overrides({"silver.customers": {"provenance.enabled": "false"}}):
            assert provenance_column(manager.get_entity_definition("silver.customers")) is None
        assert provenance_column(manager.get_entity_definition("silver.customers"))

    @pytest.mark.parametrize(
        "entityid, tags, params",
        [
            ("silver.events", {"provider_type": "csv"}, {}),
            ("silver.events", {"provider_type": "eventhub"}, {}),
            ("silver.events", {"provider.read_only": "true"}, {}),
            ("system.watermarks", {}, {}),
        ],
    )
    def test_ineligible_entities_are_skipped(self, entityid, tags, params):
        manager = DataEntityManager(
            config_service=_config_service({"kindling.provenance.enabled": True})
        )
        entity = _register(manager, entityid=entityid, tags=tags, **params)
        assert "provenance.enabled" not in entity.tags

    def test_memory_provider_is_eligible(self):
        manager = DataEntityManager(
            config_service=_config_service({"kindling.provenance.enabled": True})
        )
        entity = _register(manager, tags={"provider_type": "memory"})
        assert provenance_column(entity) == "__provenance"

    def test_sql_entity_is_skipped(self):
        manager = DataEntityManager(
            config_service=_config_service({"kindling.provenance.enabled": True})
        )
        manager.register_entity(
            "reporting.recent",
            name="recent",
            merge_columns=[],
            tags={"provider_type": "view"},
            schema=None,
            sql="SELECT 1 AS x",
        )
        entity = manager.get_entity_definition("reporting.recent")
        assert "provenance.enabled" not in entity.tags

    def test_explicit_tag_opts_in_other_provider(self):
        manager = DataEntityManager(
            config_service=_config_service({"kindling.provenance.enabled": True})
        )
        entity = _register(manager, tags={"provider_type": "parquet", "provenance.enabled": "true"})
        assert provenance_column(entity) == "__provenance"
        assert "provenance.column" not in entity.tags  # default not baked

    def test_scd2_companion_inherits_base_tags(self):
        manager = DataEntityManager(
            config_service=_config_service({"kindling.provenance.enabled": True})
        )
        _register(manager, tags={"scd.type": "2"})
        companion = manager.get_entity_definition("silver.customers.current")
        assert companion.tags["provider_type"] == "current_view"
        # Copied from the base (so reads of the companion drop the column),
        # never defaulted on the companion itself.
        assert companion.tags["provenance.enabled"] == "true"


class TestValidation:
    def _fails(self, match, tags, **params):
        with pytest.raises(ValueError, match=match):
            _register(DataEntityManager(), tags=tags, **params)

    def test_unknown_tag(self):
        self._fails("unknown provenance tag 'provenance.colum'", {"provenance.colum": "x"})

    def test_keep_inputs_is_a_pipe_tag(self):
        self._fails("unknown provenance tag", {"provenance.keep_inputs": "a"})

    def test_enabled_must_be_boolean(self):
        self._fails("provenance.enabled must be 'true' or 'false'", {"provenance.enabled": "yes"})

    def test_empty_column(self):
        self._fails(
            "provenance.column is set but empty",
            {"provenance.enabled": "true", "provenance.column": " "},
        )

    @pytest.mark.parametrize("pattern", ["*", "**", "domain,*"])
    def test_pipe_tags_rejects_match_all(self, pattern):
        self._fails("allow-list", {"provenance.pipe_tags": pattern})

    def test_pipe_tags_accepts_narrow_glob(self):
        entity = _register(
            DataEntityManager(),
            tags={"provenance.enabled": "true", "provenance.pipe_tags": "layer.*"},
        )
        assert provenance_config_from_tags(entity).pipe_tags == ("layer.*",)

    @pytest.mark.parametrize(
        "tags, params, label",
        [
            ({}, {"merge_columns": ["__provenance"]}, "merge_columns"),
            ({}, {"partition_columns": ["__PROVENANCE"]}, "partition_columns"),
            ({}, {"cluster_columns": ["__provenance"]}, "cluster_columns"),
            ({"scd.type": "2", "scd.tracked": "status,__provenance"}, {}, "scd.tracked"),
            ({"scd.untracked": "__provenance"}, {}, "scd.untracked"),
            (
                {"surrogate.column": "sk", "surrogate.from": "customer_id,__provenance"},
                {},
                "surrogate.from",
            ),
            ({"surrogate.column": "__provenance"}, {}, "surrogate key columns"),
            (
                {"dataset.kind": "derived", "derived.replace_keys": "__provenance"},
                {"merge_columns": []},
                "derived.replace_keys",
            ),
        ],
    )
    def test_column_rejected_in_structural_lists(self, tags, params, label):
        params.setdefault("schema", None)
        with pytest.raises(ValueError, match=f"must not appear in {label}"):
            _register(DataEntityManager(), tags={"provenance.enabled": "true", **tags}, **params)

    def test_column_rejected_as_sequence_by(self):
        self._fails(
            "must not appear in scd.sequence_by",
            {
                "provenance.enabled": "true",
                "provenance.column": "status",
                "scd.type": "2",
                "scd.sequence_by": "status",
            },
        )

    def test_column_rejected_as_temporal_column(self):
        self._fails(
            "SCD2 temporal column",
            {
                "provenance.enabled": "true",
                "provenance.column": "__is_current",
                "scd.type": "2",
            },
            schema=None,
        )

    @pytest.mark.parametrize("column", ["__merge_key", "__scd2_action", "__SCD2_x"])
    def test_reserved_staging_names(self, column):
        self._fails(
            "reserved SCD2 staging name",
            {"provenance.enabled": "true", "provenance.column": column},
        )

    def test_column_rejected_in_declared_schema(self):
        schema = StructType(SCHEMA.fields + [StructField("__provenance", StringType(), True)])
        self._fails(
            "remove it from the declared schema", {"provenance.enabled": "true"}, schema=schema
        )

    def test_read_only_entity_rejected(self):
        self._fails(
            "read-only entity",
            {"provenance.enabled": "true", "provider.read_only": "true"},
        )

    def test_disabled_skips_column_checks(self):
        entity = _register(
            DataEntityManager(),
            tags={"provenance.enabled": "false"},
            merge_columns=["__provenance"],
            schema=None,
        )
        assert provenance_column(entity) is None
