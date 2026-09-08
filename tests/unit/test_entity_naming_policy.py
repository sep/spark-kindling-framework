import importlib
import sys
from pathlib import Path
from unittest.mock import MagicMock

import pytest


def _naming():
    return importlib.import_module("kindling.entity_naming")


def _make_config_service(sections):
    config_service = MagicMock()
    config_service.get.side_effect = lambda key, default=None: sections.get(key, default)
    return config_service


def test_entity_naming_import_has_no_platform_or_spark_side_effects():
    module_name = "_kindling_entity_naming_import_probe"
    module_path = Path(__file__).parents[2] / "packages" / "kindling" / "entity_naming.py"
    before = set(sys.modules)

    spec = importlib.util.spec_from_file_location(module_name, module_path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    try:
        spec.loader.exec_module(module)
    finally:
        sys.modules.pop(module_name, None)

    loaded = set(sys.modules) - before
    forbidden = (
        "databricks",
        "kindling_ext_",
        "pyspark",
    )
    assert not any(
        name == blocked or name.startswith(f"{blocked}.") or name.startswith(blocked)
        for name in loaded
        for blocked in forbidden
    )


def test_parse_table_naming_mode_accepts_unset_and_case_insensitive_values():
    naming = _naming()

    assert naming.parse_table_naming_mode(None, key=naming.GLOBAL_NAMING_KEY) is None
    assert naming.parse_table_naming_mode("  ", key=naming.GLOBAL_NAMING_KEY) is None
    assert (
        naming.parse_table_naming_mode(" LeAf ", key=naming.GLOBAL_NAMING_KEY)
        == naming.TableNamingMode.LEAF
    )
    assert (
        naming.parse_table_naming_mode("NORMALIZED", key=naming.GLOBAL_NAMING_KEY)
        == naming.TableNamingMode.NORMALIZED
    )
    assert (
        naming.parse_table_naming_mode("legacy", key=naming.GLOBAL_NAMING_KEY)
        == naming.TableNamingMode.LEGACY
    )


def test_invalid_table_naming_mode_names_key_value_allowed_modes_and_entity():
    naming = _naming()

    with pytest.raises(ValueError) as exc_info:
        naming.parse_table_naming_mode(
            "per-leaf",
            key=naming.ENTITY_NAMING_TAG,
            entity_id="silver.device_telemetry",
        )

    message = str(exc_info.value)
    assert "provider.table_naming" in message
    assert "'per-leaf'" in message
    assert "'legacy', 'normalized', or 'leaf'" in message
    assert "silver.device_telemetry" in message


def test_sdp_dataset_naming_parse_error_keeps_existing_message_shape():
    naming = _naming()

    with pytest.raises(
        ValueError,
        match=r"Invalid kindling\.sdp\.dataset_naming value 'typo'; expected",
    ):
        naming.parse_table_naming_mode("typo", key="kindling.sdp.dataset_naming")


def test_table_component_derivation_is_pure_and_normalizes_leafs():
    naming = _naming()

    assert naming.normalize_table_leaf("silver.device-telemetry") == "silver_device_telemetry"
    assert (
        naming.derive_table_component("silver.device-telemetry", naming.TableNamingMode.NORMALIZED)
        == "silver_device_telemetry"
    )
    assert (
        naming.derive_table_component("silver.device-telemetry", naming.TableNamingMode.LEAF)
        == "device_telemetry"
    )
    with pytest.raises(ValueError, match="legacy"):
        naming.derive_table_component("silver.device-telemetry", naming.TableNamingMode.LEGACY)


def test_policy_resolves_entity_mode_before_global_mode():
    naming = _naming()
    policy = naming.TableNamingPolicy.from_config_value("normalized")

    assert (
        policy.mode_for(
            "silver.device_telemetry",
            {naming.ENTITY_NAMING_TAG: "leaf"},
        )
        == naming.TableNamingMode.LEAF
    )
    assert (
        policy.component_for(
            "silver.device-telemetry",
            {naming.ENTITY_NAMING_TAG: "leaf"},
        )
        == "device_telemetry"
    )


def test_policy_explicit_legacy_opts_out_of_global_component_derivation():
    naming = _naming()
    policy = naming.TableNamingPolicy.from_config_value("leaf")

    assert (
        policy.mode_for(
            "silver.device_telemetry",
            {naming.ENTITY_NAMING_TAG: " legacy "},
        )
        == naming.TableNamingMode.LEGACY
    )
    assert (
        policy.component_for(
            "silver.device_telemetry",
            {naming.ENTITY_NAMING_TAG: "legacy"},
        )
        is None
    )


def test_valid_entity_mode_takes_precedence_over_invalid_global_mode():
    naming = _naming()
    policy = naming.TableNamingPolicy.from_config_value("not-a-mode")

    assert (
        policy.mode_for(
            "silver.device_telemetry",
            {naming.ENTITY_NAMING_TAG: "leaf"},
        )
        == naming.TableNamingMode.LEAF
    )

    with pytest.raises(ValueError, match=r"Invalid kindling\.storage\.table_naming"):
        policy.mode_for("silver.device_telemetry", {})


def test_sdp_mode_projection_maps_legacy_to_normalized():
    naming = _naming()

    assert naming.sdp_mode_for(None) == "normalized"
    assert naming.sdp_mode_for(naming.TableNamingMode.LEGACY) == "normalized"
    assert naming.sdp_mode_for(naming.TableNamingMode.NORMALIZED) == "normalized"
    assert naming.sdp_mode_for(naming.TableNamingMode.LEAF) == "leaf"


def test_table_naming_tag_can_be_assigned_by_tag_rule_and_overridden_by_id_pattern():
    from kindling.data_entities import DataEntityManager

    naming = _naming()
    signal_provider = MagicMock()
    signal_provider.create_signal.return_value = MagicMock()
    manager = DataEntityManager(signal_provider)
    manager.register_entity(
        "silver.device_telemetry",
        name="device_telemetry",
        merge_columns=["device_id"],
        tags={"tier": "bronze"},
        schema=None,
    )
    config_service = _make_config_service(
        {
            "dataentities-bytag": {
                "tier": {
                    "bronze": {"tags": {naming.ENTITY_NAMING_TAG: "leaf"}},
                },
            },
            "dataentities": {
                "silver.*": {"tags": {naming.ENTITY_NAMING_TAG: "legacy"}},
            },
        }
    )

    manager.apply_config_overrides(config_service)

    entity = manager.get_entity_definition("silver.device_telemetry")
    assert entity.tags[naming.ENTITY_NAMING_TAG] == "legacy"
    assert (
        naming.TableNamingPolicy.from_config_value("normalized").mode_for(
            entity.entityid, entity.tags
        )
        == naming.TableNamingMode.LEGACY
    )
