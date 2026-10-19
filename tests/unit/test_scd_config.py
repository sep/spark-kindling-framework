from types import SimpleNamespace

import pytest
from kindling.data_entities import (
    EntityMetadata,
    _validate_scd_config,
    scd_change_columns,
    scd_config_from_tags,
)


def make_entity(
    *,
    entityid="silver.customer",
    merge_columns=None,
    tags=None,
    schema=None,
):
    return EntityMetadata(
        entityid=entityid,
        name="Customer",
        merge_columns=[] if merge_columns is None else merge_columns,
        tags={} if tags is None else tags,
        schema=schema,
    )


def make_schema(*field_names):
    return SimpleNamespace(fields=[SimpleNamespace(name=field_name) for field_name in field_names])


def test_scd_config_from_tags_no_tag_returns_disabled_config():
    cfg = scd_config_from_tags(make_entity())

    assert cfg.enabled is False
    assert cfg.tracked_columns is None
    assert cfg.effective_from_column == "__effective_from"
    assert cfg.effective_to_column == "__effective_to"
    assert cfg.is_current_column == "__is_current"
    assert cfg.current_entity_id == "silver.customer.current"
    assert cfg.routing_key_method == "hash"


def test_scd_config_from_tags_scd_type_2_returns_enabled_config():
    cfg = scd_config_from_tags(make_entity(tags={"scd.type": "2"}))

    assert cfg.enabled is True


def test_scd_config_from_tags_invalid_scd_type_raises_value_error():
    entity = make_entity(tags={"scd.type": "1"})

    with pytest.raises(ValueError, match="scd.type must be '2'"):
        scd_config_from_tags(entity)


def test_scd_config_from_tags_custom_column_names_respected():
    cfg = scd_config_from_tags(
        make_entity(
            tags={
                "scd.type": "2",
                "scd.effective_from_col": "valid_from",
                "scd.effective_to_col": "valid_to",
                "scd.current_col": "is_active",
            }
        )
    )

    assert cfg.effective_from_column == "valid_from"
    assert cfg.effective_to_column == "valid_to"
    assert cfg.is_current_column == "is_active"


def test_scd_config_from_tags_tracked_columns_parsed_from_comma_list():
    cfg = scd_config_from_tags(
        make_entity(tags={"scd.type": "2", "scd.tracked": "name, email, status"})
    )

    assert cfg.tracked_columns == ["name", "email", "status"]


def test_scd_config_from_tags_empty_tracked_returns_none():
    cfg = scd_config_from_tags(make_entity(tags={"scd.type": "2", "scd.tracked": "  "}))

    assert cfg.tracked_columns is None


def test_scd_config_from_tags_routing_key_hash_is_default():
    cfg = scd_config_from_tags(make_entity(tags={"scd.type": "2"}))

    assert cfg.routing_key_method == "hash"


def test_scd_config_from_tags_routing_key_concat_accepted():
    cfg = scd_config_from_tags(make_entity(tags={"scd.type": "2", "scd.routing_key": "concat"}))

    assert cfg.routing_key_method == "concat"


def test_scd_config_from_tags_invalid_routing_key_raises_value_error():
    entity = make_entity(tags={"scd.type": "2", "scd.routing_key": "uuid"})

    with pytest.raises(ValueError, match="scd.routing_key"):
        scd_config_from_tags(entity)


def test_scd_config_from_tags_default_current_entity_id_is_entityid_dot_current():
    cfg = scd_config_from_tags(make_entity(entityid="gold.account", tags={"scd.type": "2"}))

    assert cfg.current_entity_id == "gold.account.current"


def test_validate_scd_config_scd2_missing_merge_columns_raises_value_error():
    entity = make_entity(tags={"scd.type": "2"}, merge_columns=[])

    with pytest.raises(ValueError, match="requires merge_columns"):
        _validate_scd_config(entity)


def test_validate_scd_config_tracked_cols_overlap_merge_cols_raises_value_error():
    entity = make_entity(
        merge_columns=["customer_id"],
        tags={"scd.type": "2", "scd.tracked": "customer_id,email"},
    )

    with pytest.raises(ValueError, match="must not include merge_columns"):
        _validate_scd_config(entity)


def test_validate_scd_config_temporal_col_collides_with_schema_raises_value_error():
    entity = make_entity(
        merge_columns=["customer_id"],
        tags={"scd.type": "2"},
        schema=make_schema("customer_id", "__effective_from"),
    )

    with pytest.raises(ValueError, match="collide with business schema columns"):
        _validate_scd_config(entity)


def test_validate_scd_config_non_scd_entity_passes():
    _validate_scd_config(make_entity(merge_columns=[], schema={"no": "fields attr"}))


def test_validate_scd_config_merge_key_column_in_schema_raises_value_error():
    entity = make_entity(
        merge_columns=["customer_id"],
        tags={"scd.type": "2"},
        schema=make_schema("customer_id", "__merge_key"),
    )

    with pytest.raises(ValueError, match="__merge_key.*reserved"):
        _validate_scd_config(entity)


def test_validate_scd_config_current_entity_id_equals_base_raises_value_error():
    entity = make_entity(
        entityid="silver.customer",
        merge_columns=["customer_id"],
        tags={"scd.type": "2", "scd.current_entity_id": "silver.customer"},
    )

    with pytest.raises(ValueError, match="scd.current_entity_id must"):
        _validate_scd_config(entity)


def test_validate_scd_config_current_entity_id_empty_raises_value_error():
    entity = make_entity(
        entityid="silver.customer",
        merge_columns=["customer_id"],
        tags={"scd.type": "2", "scd.current_entity_id": "   "},
    )

    with pytest.raises(ValueError, match="scd.current_entity_id must"):
        _validate_scd_config(entity)


def test_scd_config_from_tags_close_on_missing_false_by_default():
    cfg = scd_config_from_tags(make_entity(tags={"scd.type": "2"}))

    assert cfg.close_on_missing is False


def test_scd_config_from_tags_close_on_missing_true_when_tag_set():
    cfg = scd_config_from_tags(make_entity(tags={"scd.type": "2", "scd.close_on_missing": "true"}))

    assert cfg.close_on_missing is True


def test_scd_config_from_tags_close_on_missing_case_insensitive():
    cfg = scd_config_from_tags(make_entity(tags={"scd.type": "2", "scd.close_on_missing": "True"}))

    assert cfg.close_on_missing is True


def test_scd_config_from_tags_optimize_unchanged_false_by_default():
    cfg = scd_config_from_tags(make_entity(tags={"scd.type": "2"}))

    assert cfg.optimize_unchanged is False


def test_scd_config_from_tags_optimize_unchanged_true_when_tag_set():
    cfg = scd_config_from_tags(
        make_entity(tags={"scd.type": "2", "scd.optimize_unchanged": "true"})
    )

    assert cfg.optimize_unchanged is True


def test_scd_config_from_tags_optimize_unchanged_case_insensitive():
    cfg = scd_config_from_tags(
        make_entity(tags={"scd.type": "2", "scd.optimize_unchanged": "TRUE"})
    )

    assert cfg.optimize_unchanged is True


# -- Declared-flow additions: sequence_by / source_kind / delete_when --------


def test_scd_defaults_are_change_feed_without_sequence_or_deletes():
    cfg = scd_config_from_tags(make_entity(tags={"scd.type": "2"}))

    assert cfg.source_kind == "change_feed"
    assert cfg.sequence_by is None
    assert cfg.delete_when is None
    assert cfg.close_on_missing is False


def test_scd_source_kind_snapshot_implies_close_on_missing():
    cfg = scd_config_from_tags(make_entity(tags={"scd.type": "2", "scd.source_kind": "snapshot"}))

    assert cfg.source_kind == "snapshot"
    assert cfg.close_on_missing is True


def test_scd_close_on_missing_is_sugar_for_snapshot():
    cfg = scd_config_from_tags(make_entity(tags={"scd.type": "2", "scd.close_on_missing": "true"}))

    assert cfg.source_kind == "snapshot"
    assert cfg.close_on_missing is True


def test_scd_source_kind_invalid_value_raises():
    with pytest.raises(ValueError, match="scd.source_kind"):
        scd_config_from_tags(make_entity(tags={"scd.type": "2", "scd.source_kind": "stream"}))


def test_scd_change_feed_with_close_on_missing_raises():
    with pytest.raises(ValueError, match="contradicts"):
        scd_config_from_tags(
            make_entity(
                tags={
                    "scd.type": "2",
                    "scd.source_kind": "change_feed",
                    "scd.close_on_missing": "true",
                }
            )
        )


def test_scd_delete_when_with_snapshot_raises():
    with pytest.raises(ValueError, match="delete_when"):
        scd_config_from_tags(
            make_entity(
                tags={
                    "scd.type": "2",
                    "scd.source_kind": "snapshot",
                    "scd.delete_when": "op = 'D'",
                }
            )
        )


def test_scd_delete_when_parsed_for_change_feed():
    cfg = scd_config_from_tags(make_entity(tags={"scd.type": "2", "scd.delete_when": "op = 'D'"}))

    assert cfg.delete_when == "op = 'D'"
    assert cfg.source_kind == "change_feed"


def test_scd_sequence_by_parsed():
    cfg = scd_config_from_tags(make_entity(tags={"scd.type": "2", "scd.sequence_by": "updated_at"}))

    assert cfg.sequence_by == "updated_at"


def test_validate_sequence_by_must_be_in_schema():
    entity = make_entity(
        merge_columns=["id"],
        tags={"scd.type": "2", "scd.sequence_by": "updated_at"},
        schema=make_schema("id", "status"),
    )
    with pytest.raises(ValueError, match="sequence_by"):
        _validate_scd_config(entity)


def test_validate_sequence_by_must_not_be_temporal_column():
    entity = make_entity(
        merge_columns=["id"],
        tags={"scd.type": "2", "scd.sequence_by": "__effective_from"},
        schema=make_schema("id", "status"),
    )
    with pytest.raises(ValueError, match="business data column"):
        _validate_scd_config(entity)


def test_validate_sequence_by_in_schema_passes():
    entity = make_entity(
        merge_columns=["id"],
        tags={"scd.type": "2", "scd.sequence_by": "updated_at"},
        schema=make_schema("id", "status", "updated_at"),
    )
    _validate_scd_config(entity)


# -- scd.untracked -----------------------------------------------------------


def test_scd_untracked_parsed_for_scd2():
    cfg = scd_config_from_tags(
        make_entity(tags={"scd.type": "2", "scd.untracked": "ingested_at, load_id"})
    )

    assert cfg.untracked_columns == ["ingested_at", "load_id"]
    assert cfg.tracked_columns is None


def test_scd_untracked_parsed_for_scd1_entity_without_scd_type():
    cfg = scd_config_from_tags(make_entity(tags={"scd.untracked": "ingested_at"}))

    assert cfg.enabled is False
    assert cfg.untracked_columns == ["ingested_at"]


def test_scd_untracked_absent_is_none():
    assert scd_config_from_tags(make_entity()).untracked_columns is None
    assert scd_config_from_tags(make_entity(tags={"scd.type": "2"})).untracked_columns is None


def _untracked_entity(tags, *, merge_columns=("id",), schema=None):
    return make_entity(merge_columns=list(merge_columns), tags=tags, schema=schema)


def test_validate_untracked_and_tracked_together_raises():
    entity = _untracked_entity(
        {"scd.type": "2", "scd.tracked": "status", "scd.untracked": "ingested_at"}
    )
    with pytest.raises(ValueError, match="mutually exclusive"):
        _validate_scd_config(entity)


def test_validate_untracked_and_tracked_together_raises_for_scd1_too():
    entity = _untracked_entity({"scd.tracked": "status", "scd.untracked": "ingested_at"})
    with pytest.raises(ValueError, match="mutually exclusive"):
        _validate_scd_config(entity)


@pytest.mark.parametrize("tags", [{"scd.type": "2"}, {}], ids=["scd2", "scd1"])
def test_validate_untracked_business_key_raises(tags):
    entity = _untracked_entity({**tags, "scd.untracked": "id"})
    with pytest.raises(ValueError, match="scd.untracked must not include merge_columns"):
        _validate_scd_config(entity)


@pytest.mark.parametrize("column", ["__effective_from", "__is_current", "__merge_key"])
def test_validate_untracked_scd2_temporal_or_reserved_column_raises(column):
    entity = _untracked_entity({"scd.type": "2", "scd.untracked": column})
    with pytest.raises(ValueError, match="temporal or reserved"):
        _validate_scd_config(entity)


@pytest.mark.parametrize("tags", [{"scd.type": "2"}, {}], ids=["scd2", "scd1"])
def test_validate_untracked_unknown_column_raises(tags):
    entity = _untracked_entity(
        {**tags, "scd.untracked": "nope"}, schema=make_schema("id", "status")
    )
    with pytest.raises(ValueError, match=r"not in the entity schema: \['nope'\]"):
        _validate_scd_config(entity)


@pytest.mark.parametrize("tags", [{"scd.type": "2"}, {}], ids=["scd2", "scd1"])
def test_validate_untracked_all_columns_raises(tags):
    entity = _untracked_entity(
        {**tags, "scd.untracked": "status,ingested_at"},
        schema=make_schema("id", "status", "ingested_at"),
    )
    with pytest.raises(ValueError, match="leaves no tracked column"):
        _validate_scd_config(entity)


def test_validate_untracked_sequence_by_counts_as_untracked():
    entity = _untracked_entity(
        {"scd.type": "2", "scd.sequence_by": "updated_at", "scd.untracked": "status"},
        schema=make_schema("id", "status", "updated_at"),
    )
    with pytest.raises(ValueError, match="leaves no tracked column"):
        _validate_scd_config(entity)


def test_validate_untracked_requires_merge_columns():
    entity = _untracked_entity({"scd.untracked": "ingested_at"}, merge_columns=())
    with pytest.raises(ValueError, match="requires merge_columns"):
        _validate_scd_config(entity)


def test_validate_untracked_without_usable_names_raises():
    entity = _untracked_entity({"scd.untracked": " , "})
    with pytest.raises(ValueError, match="no usable column names"):
        _validate_scd_config(entity)


@pytest.mark.parametrize("tags", [{"scd.type": "2"}, {}], ids=["scd2", "scd1"])
def test_validate_untracked_valid_passes(tags):
    _validate_scd_config(
        _untracked_entity(
            {**tags, "scd.untracked": "ingested_at"},
            schema=make_schema("id", "status", "ingested_at"),
        )
    )


def test_validate_untracked_without_schema_skips_schema_checks():
    _validate_scd_config(_untracked_entity({"scd.untracked": "ingested_at"}))


def test_validate_untracked_with_write_mode_insert_raises():
    from kindling.data_entities import _validate_write_mode_tag

    entity = _untracked_entity({"write.mode": "insert", "scd.untracked": "ingested_at"})
    with pytest.raises(ValueError, match="scd.untracked has no effect"):
        _validate_write_mode_tag(entity)


def test_validate_untracked_on_derived_dataset_raises():
    from kindling.data_entities import _validate_derived_config

    entity = _untracked_entity({"dataset.kind": "derived", "scd.untracked": "ingested_at"})
    with pytest.raises(ValueError, match="does not apply to a derived dataset"):
        _validate_derived_config(entity)


@pytest.mark.parametrize(
    "tags, match",
    [
        ({"scd.tracked": "status", "scd.untracked": "load_id"}, "mutually exclusive"),
        ({"scd.untracked": "customer_id"}, "must not include merge_columns"),
        ({"scd.untracked": "status,load_id"}, "leaves no tracked column"),
        ({"scd.untracked": "nope"}, "not in the entity schema"),
    ],
    ids=["both_tags", "key_untracked", "all_untracked", "unknown_column"],
)
@pytest.mark.parametrize("scd_tags", [{"scd.type": "2"}, {}], ids=["scd2", "scd1"])
def test_register_entity_rejects_invalid_untracked(scd_tags, tags, match):
    from kindling.data_entities import DataEntityManager
    from pyspark.sql.types import StringType, StructField, StructType

    manager = DataEntityManager()
    with pytest.raises(ValueError, match=match):
        manager.register_entity(
            "silver.customer",
            name="Customer",
            merge_columns=["customer_id"],
            tags={**scd_tags, **tags},
            schema=StructType(
                [StructField(name, StringType()) for name in ("customer_id", "status", "load_id")]
            ),
        )
    assert manager.registry == {}


# -- scd_change_columns ------------------------------------------------------


def _cfg(tags):
    return scd_config_from_tags(make_entity(tags=tags))


COLS = ["id", "status", "ingested_at", "updated_at", "__effective_from", "__is_current"]


def test_change_columns_default_tracks_every_non_key_non_temporal_column():
    tracked, untracked = scd_change_columns(_cfg({"scd.type": "2"}), COLS, ["id"])

    assert tracked == ["status", "ingested_at", "updated_at"]
    assert untracked == []


def test_change_columns_untracked_and_sequence_by_are_untracked():
    cfg = _cfg({"scd.type": "2", "scd.untracked": "ingested_at", "scd.sequence_by": "updated_at"})
    tracked, untracked = scd_change_columns(cfg, COLS, ["id"])

    assert tracked == ["status"]
    assert untracked == ["ingested_at", "updated_at"]


def test_change_columns_tracked_allow_list_makes_the_rest_untracked():
    cfg = _cfg({"scd.type": "2", "scd.tracked": "status"})
    tracked, untracked = scd_change_columns(cfg, COLS, ["id"])

    assert tracked == ["status"]
    assert untracked == ["ingested_at", "updated_at"]


def test_change_columns_skip_untracked_columns_missing_from_the_batch():
    cfg = _cfg({"scd.type": "2", "scd.untracked": "ingested_at,load_id"})
    tracked, untracked = scd_change_columns(cfg, ["id", "status", "ingested_at"], ["id"])

    assert tracked == ["status"]
    assert untracked == ["ingested_at"]


def test_change_columns_scd1_does_not_treat_temporal_names_specially():
    cfg = _cfg({"scd.untracked": "ingested_at"})
    tracked, untracked = scd_change_columns(cfg, COLS, ["id"])

    assert tracked == ["status", "updated_at", "__effective_from", "__is_current"]
    assert untracked == ["ingested_at"]


def test_null_safe_change_condition_is_never_null():
    """NOT (a <=> b): both-null compares equal, so negating the condition
    is safe (the snapshot sentinel and in-place classes rely on it)."""
    from kindling.data_entities import build_null_safe_change_condition

    assert (
        build_null_safe_change_condition("s", "t", ["a", "b"])
        == "(NOT (s.`a` <=> t.`a`)) OR (NOT (s.`b` <=> t.`b`))"
    )
    assert build_null_safe_change_condition("s", "t", []) == "false"


def test_validate_scd_config_scd2_staging_prefix_in_schema_raises_value_error():
    entity = make_entity(
        merge_columns=["customer_id"],
        tags={"scd.type": "2"},
        schema=make_schema("customer_id", "__scd2_action"),
    )

    with pytest.raises(ValueError, match="__scd2_.*reserved"):
        _validate_scd_config(entity)
