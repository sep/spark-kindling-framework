"""How settings layers merge: mappings deep-merge, lists and scalars replace
(the same rule as config overlays and `kindling bundle build`), opt-in
appends via Dynaconf's markers, and no implicit settings.local.yaml."""

from unittest.mock import MagicMock, patch

import pytest
import yaml
from kindling.spark_config import (
    DynaconfConfig,
    _merged_settings_file,
    merge_settings_layers,
    peek_settings_value,
)


def _merge(*layers):
    merged = {}
    for layer in layers:
        merged = merge_settings_layers(merged, layer)
    return merged


def test_mappings_deep_merge_and_lists_replace():
    merged = _merge(
        {"kindling": {"extensions": ["a", "b"], "storage": {"root": "/base", "fmt": "delta"}}},
        {"kindling": {"extensions": ["c"], "storage": {"root": "/dev"}}},
    )
    assert merged == {
        "kindling": {"extensions": ["c"], "storage": {"root": "/dev", "fmt": "delta"}}
    }


@pytest.mark.parametrize(
    "override,expected",
    [
        (["c", "dynaconf_merge"], ["a", "b", "c"]),
        (["dynaconf_merge", "b", "c"], ["a", "b", "b", "c"]),
        (["dynaconf_merge_unique", "b", "c"], ["a", "b", "c"]),
        ("@merge [c, d]", ["a", "b", "c", "d"]),
        ("@merge c,d", ["a", "b", "c", "d"]),
    ],
)
def test_list_append_is_opt_in(override, expected):
    assert _merge({"x": ["a", "b"]}, {"x": override}) == {"x": expected}


def test_mapping_merge_markers():
    base = {"k": {"a": 1, "b": 2}}
    assert _merge(base, {"k": {"dynaconf_merge": False, "c": 3}}) == {"k": {"c": 3}}
    assert _merge(base, {"k": "@merge {c: 3}"}) == {"k": {"a": 1, "b": 2, "c": 3}}


def test_runtime_merge_matches_bundle_build():
    """`kindling bundle build` inlines settings merged by its own deep_merge;
    the runtime must resolve the same YAML to the same values."""
    from kindling_cli.bundle import deep_merge

    layers = [
        {"kindling": {"extensions": ["a"], "s": {"x": 1, "y": [1, 2]}, "flag": True}},
        {"kindling": {"extensions": ["b", "c"], "s": {"y": [3]}, "flag": False}},
        {"kindling": {"s": {"z": {"deep": 1}}}, "spark_configs": {"k": "v"}},
    ]
    bundle_merged = {}
    for layer in layers:
        bundle_merged = deep_merge(bundle_merged, layer)
    assert _merge(*layers) == bundle_merged


def _spark_without_conf():
    spark = MagicMock()
    spark.conf.get.return_value = None
    return spark


def _write(path, data):
    path.write_text(yaml.safe_dump(data), encoding="utf-8")
    return str(path)


def test_local_file_beside_settings_is_not_loaded_implicitly(tmp_path):
    base = _write(tmp_path / "settings.yaml", {"kindling": {"probe": "base", "items": ["a"]}})
    dev = _write(tmp_path / "settings.dev.yaml", {"kindling": {"probe": "dev"}})
    _write(tmp_path / "settings.local.yaml", {"kindling": {"probe": "local", "items": ["z"]}})

    assert peek_settings_value([base, dev], "kindling.probe") == "dev"
    assert peek_settings_value([base, dev], "kindling.items") == ["a"]


def test_local_layer_applies_once_when_selected(tmp_path):
    base = _write(tmp_path / "settings.yaml", {"kindling": {"items": ["a", "b"]}})
    local = _write(tmp_path / "settings.local.yaml", {"kindling": {"items": ["c"]}})

    assert peek_settings_value([base, local], "kindling.items") == ["c"]


def test_merged_file_lives_outside_the_config_directory(tmp_path):
    base = _write(tmp_path / "settings.yaml", {"kindling": {"a": 1}})
    merged = _merged_settings_file([base, str(tmp_path / "missing.yaml")])
    assert merged and not merged.startswith(str(tmp_path))
    assert yaml.safe_load(open(merged)) == {"kindling": {"a": 1}}
    assert _merged_settings_file([str(tmp_path / "missing.yaml")]) is None


def test_format_lazy_values_still_resolve(tmp_path):
    base = _write(
        tmp_path / "settings.yaml",
        {"kindling": {"root": "/data", "path": "@format {this.kindling.root}/bronze"}},
    )
    assert peek_settings_value([base], "kindling.path") == "/data/bronze"


@patch(
    "kindling.spark_config.get_or_create_spark_session", side_effect=lambda: _spark_without_conf()
)
def test_parameter_for_nested_log_level_updates_flat_key(_spark, tmp_path):
    settings = _write(
        tmp_path / "settings.yaml", {"kindling": {"telemetry": {"logging": {"level": "WARN"}}}}
    )
    config = DynaconfConfig()
    config.initialize(
        config_files=[settings],
        initial_config={"kindling.telemetry.logging.level": "DEBUG"},
    )
    assert config.get("log_level") == "DEBUG"
    assert config.get("kindling.telemetry.logging.level") == "DEBUG"


@patch(
    "kindling.spark_config.get_or_create_spark_session", side_effect=lambda: _spark_without_conf()
)
def test_file_log_level_is_used_without_a_parameter(_spark, tmp_path):
    settings = _write(
        tmp_path / "settings.yaml", {"kindling": {"telemetry": {"logging": {"level": "WARN"}}}}
    )
    config = DynaconfConfig()
    config.initialize(config_files=[settings], initial_config={})
    assert config.get("log_level") == "WARN"
