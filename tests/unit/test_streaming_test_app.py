import importlib.util
from pathlib import Path
from types import SimpleNamespace


def _load_streaming_app_module():
    app_path = (
        Path(__file__).resolve().parents[1]
        / "data-apps"
        / "streaming-test-app"
        / "main.py"
    )
    source = app_path.read_text()
    prefix = source.split("# Initialize", 1)[0]
    spec = importlib.util.spec_from_loader("streaming_test_app", loader=None)
    module = importlib.util.module_from_spec(spec)
    exec(prefix, module.__dict__)
    return module


def test_resolve_stream_paths_uses_configured_databricks_roots():
    module = _load_streaming_app_module()
    config = SimpleNamespace(
        get=lambda key, default=None: {
            "kindling.storage.checkpoint_root": "/Volumes/kindling/kindling/artifacts/temp/checkpoints",
            "kindling.storage.table_root": "/Volumes/kindling/kindling/artifacts/temp/tables",
            "kindling.temp_path": "/Volumes/kindling/kindling/artifacts/temp",
        }.get(key, default)
    )

    checkpoint_dir, output_dir = module._resolve_stream_paths(
        config, "databricks", "abc123", 123456
    )

    assert (
        checkpoint_dir
        == "/Volumes/kindling/kindling/artifacts/temp/checkpoints/streaming_test_abc123_123456"
    )
    assert (
        output_dir
        == "/Volumes/kindling/kindling/artifacts/temp/tables/streaming_output/streaming_test_abc123_123456"
    )


def test_resolve_stream_paths_falls_back_to_tmp_for_databricks_without_config():
    module = _load_streaming_app_module()
    config = SimpleNamespace(get=lambda _key, default=None: default)

    checkpoint_dir, output_dir = module._resolve_stream_paths(
        config, "databricks", "abc123", 123456
    )

    assert checkpoint_dir == "/tmp/checkpoints/streaming_test_abc123_123456"
    assert output_dir == "/tmp/streaming_output/streaming_test_abc123_123456"


def test_resolve_stream_paths_uses_files_for_non_databricks():
    module = _load_streaming_app_module()
    config = SimpleNamespace(get=lambda _key, default=None: default)

    checkpoint_dir, output_dir = module._resolve_stream_paths(config, "fabric", "abc123", 123456)

    assert checkpoint_dir == "Files/checkpoints/streaming_test_abc123_123456"
    assert output_dir == "Files/streaming_output/streaming_test_abc123_123456"
