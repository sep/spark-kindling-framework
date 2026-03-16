import importlib.util
from pathlib import Path
from types import SimpleNamespace


def _load_streaming_orchestrator_app_module():
    app_path = (
        Path(__file__).resolve().parents[1]
        / "data-apps"
        / "streaming-orchestrator-test-app"
        / "main.py"
    )
    spec = importlib.util.spec_from_file_location("streaming_orchestrator_test_app", app_path)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


def test_resolve_runtime_paths_uses_databricks_configured_roots():
    module = _load_streaming_orchestrator_app_module()
    config = SimpleNamespace(
        get=lambda key, default=None: {
            "kindling.storage.table_root": "/Volumes/kindling/kindling/artifacts/temp/tables",
            "kindling.storage.checkpoint_root": "/Volumes/kindling/kindling/artifacts/temp/checkpoints",
            "kindling.temp_path": "/Volumes/kindling/kindling/artifacts/temp",
        }.get(key, default)
    )

    paths = module._resolve_runtime_paths(config, "databricks", "abc123")

    assert (
        paths["source_path"]
        == "/Volumes/kindling/kindling/artifacts/temp/tables/streaming_orchestrator_abc123/source"
    )
    assert (
        paths["sink_path"]
        == "/Volumes/kindling/kindling/artifacts/temp/tables/streaming_orchestrator_abc123/sink"
    )
    assert (
        paths["checkpoint_root"]
        == "/Volumes/kindling/kindling/artifacts/temp/checkpoints/streaming_orchestrator_abc123"
    )


def test_extract_volume_namespace_parses_catalog_and_schema():
    module = _load_streaming_orchestrator_app_module()

    assert module._extract_volume_namespace("/Volumes/kindling/kindling/artifacts/temp") == (
        "kindling",
        "kindling",
    )


def test_entity_tags_use_name_mode_for_databricks_with_namespace():
    module = _load_streaming_orchestrator_app_module()

    tags = module._entity_tags(
        "databricks",
        "/Volumes/kindling/kindling/artifacts/temp/source",
        "streaming_orchestrator_test_source",
        "kindling",
        "kindling",
    )

    assert tags["provider.access_mode"] == "forName"
    assert tags["provider.table_name"] == "kindling.kindling.streaming_orchestrator_test_source"
