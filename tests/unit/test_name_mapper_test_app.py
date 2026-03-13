import importlib.util
from pathlib import Path
from types import SimpleNamespace


def _load_name_mapper_app_module():
    app_path = (
        Path(__file__).resolve().parents[1] / "data-apps" / "name-mapper-test-app" / "main.py"
    )
    spec = importlib.util.spec_from_file_location("name_mapper_test_app", app_path)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


def test_resolve_write_schema_prefers_current_schema_when_present():
    module = _load_name_mapper_app_module()
    spark = SimpleNamespace(
        sql=lambda _query: SimpleNamespace(collect=lambda: [{"namespace": "analytics"}])
    )

    assert module._resolve_write_schema(spark, "main", "analytics") == "analytics"


def test_resolve_write_schema_falls_back_to_existing_known_schema():
    module = _load_name_mapper_app_module()
    spark = SimpleNamespace(
        sql=lambda _query: SimpleNamespace(
            collect=lambda: [
                {"namespace": "information_schema"},
                {"namespace": "kindling"},
            ]
        )
    )

    assert module._resolve_write_schema(spark, "sep_db_kindling_np", "default") == "kindling"


def test_extract_volume_namespace_parses_catalog_and_schema():
    module = _load_name_mapper_app_module()

    assert module._extract_volume_namespace("/Volumes/sep_db_kindling_np/kindling/temp/tables") == (
        "sep_db_kindling_np",
        "kindling",
    )


def test_get_target_namespace_prefers_volume_backed_storage_root():
    module = _load_name_mapper_app_module()
    config = SimpleNamespace(
        get=lambda key, default=None: {
            "kindling.storage.table_root": "/Volumes/sep_db_kindling_np/kindling/temp/tables",
            "kindling.storage.checkpoint_root": "/Volumes/sep_db_kindling_np/kindling/temp/checkpoints",
        }.get(key, default)
    )

    assert module._get_target_namespace(config, "sep_db_kindling_np", "default") == (
        "sep_db_kindling_np",
        "kindling",
    )


def test_has_explicit_namespace_config_detects_synapse_schema():
    module = _load_name_mapper_app_module()
    config = SimpleNamespace(
        get=lambda key, default=None: {
            "kindling.synapse.schema": "kindling_system_tests",
        }.get(key, default)
    )

    assert module._has_explicit_namespace_config(config) is True


def test_build_entity_case_keeps_two_part_entity_leaf_when_namespace_is_configured():
    module = _load_name_mapper_app_module()

    entity_id, expected_table_name = module._build_entity_case(
        leaf_name="name_mapper_t123_two_part",
        write_schema="kindling_system_tests",
        catalog=None,
        use_config_namespace=True,
    )

    assert entity_id == "name_mapper_t123_two_part"
    assert expected_table_name == "kindling_system_tests.name_mapper_t123_two_part"
