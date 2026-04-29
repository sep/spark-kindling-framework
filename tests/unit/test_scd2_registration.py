from unittest.mock import MagicMock

import pytest

from kindling.data_entities import DataEntityManager


def register_base_scd2_entity(manager, *, entityid="silver.customer", tags=None):
    merged_tags = {"scd.type": "2", **(tags or {})}
    manager.register_entity(
        entityid,
        name="Customer",
        merge_columns=["customer_id"],
        tags=merged_tags,
        schema=None,
    )


def test_register_entity_scd2_creates_companion_in_registry():
    manager = DataEntityManager()

    register_base_scd2_entity(manager)

    assert "silver.customer.current" in manager.registry


def test_register_entity_scd2_companion_id_is_entityid_dot_current():
    manager = DataEntityManager()

    register_base_scd2_entity(manager)
    companion = manager.registry["silver.customer.current"]

    assert companion.entityid == "silver.customer.current"


def test_register_entity_scd2_companion_has_provider_type_current_view():
    manager = DataEntityManager()

    register_base_scd2_entity(manager)
    companion = manager.registry["silver.customer.current"]

    assert companion.tags["provider_type"] == "current_view"


def test_register_entity_scd2_companion_has_scd_companion_of_tag():
    manager = DataEntityManager()

    register_base_scd2_entity(manager)
    companion = manager.registry["silver.customer.current"]

    assert companion.tags["scd.companion_of"] == "silver.customer"
    assert companion.tags["scd.view_type"] == "current"
    assert companion.tags["provider.read_only"] == "true"


def test_register_entity_scd2_companion_inherits_merge_columns():
    manager = DataEntityManager()

    register_base_scd2_entity(manager)
    companion = manager.registry["silver.customer.current"]

    assert companion.merge_columns == ["customer_id"]


def test_register_entity_scd2_custom_current_entity_id_used():
    manager = DataEntityManager()

    register_base_scd2_entity(
        manager,
        tags={"scd.current_entity_id": "serving.current_customer"},
    )

    assert "serving.current_customer" in manager.registry
    assert "silver.customer.current" not in manager.registry


def test_register_entity_scd2_user_declared_companion_not_overwritten():
    manager = DataEntityManager()
    manager.register_entity(
        "silver.customer.current",
        name="User Declared Current Customer",
        merge_columns=["customer_id"],
        tags={"provider_type": "delta", "owner": "analytics"},
        schema=None,
    )

    register_base_scd2_entity(manager)
    companion = manager.registry["silver.customer.current"]

    assert companion.name == "User Declared Current Customer"
    assert companion.tags == {"provider_type": "delta", "owner": "analytics"}


def test_register_entity_scd2_emits_companion_registered_signal():
    manager = DataEntityManager()
    manager.emit = MagicMock()

    register_base_scd2_entity(manager)

    manager.emit.assert_any_call(
        "entity.scd2_companion_registered",
        entity_id="silver.customer",
        companion_entity_id="silver.customer.current",
    )


def test_register_entity_non_scd2_does_not_create_companion():
    manager = DataEntityManager()

    manager.register_entity(
        "silver.customer",
        name="Customer",
        merge_columns=["customer_id"],
        tags={},
        schema=None,
    )

    assert "silver.customer" in manager.registry
    assert "silver.customer.current" not in manager.registry


def test_register_entity_scd2_invalid_config_raises_before_registration():
    manager = DataEntityManager()

    with pytest.raises(ValueError, match="requires merge_columns"):
        manager.register_entity(
            "silver.customer",
            name="Customer",
            merge_columns=[],
            tags={"scd.type": "2"},
            schema=None,
        )

    assert manager.registry == {}
