from types import SimpleNamespace

import pytest

from kindling.data_entities import (
    EntityMetadata,
    _validate_scd_config,
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
