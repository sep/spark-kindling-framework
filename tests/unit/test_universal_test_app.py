import importlib.util
from pathlib import Path
from types import SimpleNamespace


def _load_universal_test_app_module():
    app_path = Path(__file__).resolve().parents[1] / "data-apps" / "universal-test-app" / "app.py"
    source = app_path.read_text()
    marker = "# Execute test app directly"
    assert marker in source, "universal-test-app unit loader marker not found"
    prefix = source.split(marker, 1)[0]
    spec = importlib.util.spec_from_loader("universal_test_app", loader=None)
    module = importlib.util.module_from_spec(spec)
    exec(prefix, module.__dict__)
    return module


def test_resolve_storage_check_path_uses_configured_databricks_temp_path():
    """Regression test for the v0.12.14 release-blocking system-test failure.

    test_storage_access() used to call dbutils.fs.ls("/") unconditionally.
    On a Databricks Standard/Shared-access-mode cluster (Spark Connect), a
    job submitted via the Jobs API has no notebook-attached "local command
    context", so Databricks' legacy DBFS-root permission check fails with
    `IllegalStateException: No api url found in local command context` --
    unconditionally, for every such job. Kindling's own bootstrap lists a
    path under the configured Unity Catalog volume root successfully in
    this same execution mode, so the storage-access smoke check should
    probe that configured path on Databricks instead of the DBFS root.
    """
    module = _load_universal_test_app_module()
    config = SimpleNamespace(
        get=lambda key, default=None: {
            "kindling.temp_path": "/Volumes/kindling/kindling/artifacts/temp/abc123",
        }.get(key, default)
    )

    path = module._resolve_storage_check_path(config, "databricks")

    assert path == "/Volumes/kindling/kindling/artifacts/temp/abc123"


def test_resolve_storage_check_path_falls_back_to_root_for_databricks_without_config():
    module = _load_universal_test_app_module()
    config = SimpleNamespace(get=lambda _key, default=None: default)

    path = module._resolve_storage_check_path(config, "databricks")

    assert path == "/"


def test_resolve_storage_check_path_falls_back_to_root_for_databricks_without_config_service():
    module = _load_universal_test_app_module()

    path = module._resolve_storage_check_path(None, "databricks")

    assert path == "/"


def test_resolve_storage_check_path_uses_root_for_non_databricks():
    module = _load_universal_test_app_module()
    config = SimpleNamespace(
        get=lambda key, default=None: {
            "kindling.temp_path": "/Volumes/kindling/kindling/artifacts/temp/abc123",
        }.get(key, default)
    )

    path = module._resolve_storage_check_path(config, "fabric")

    assert path == "/"
