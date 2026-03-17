from kindling_sdk.platform_databricks import DatabricksAPI


def _make_api() -> DatabricksAPI:
    api = DatabricksAPI.__new__(DatabricksAPI)
    api.storage_account = "sepstdatalakedev"
    api.container = "artifacts"
    api.base_path = "system-tests/run-123/databricks"
    return api


def test_resolve_system_test_mode_prefers_bootstrap_override():
    api = _make_api()

    mode = api._resolve_system_test_mode(
        {
            "config_overrides": {
                "kindling": {
                    "system_tests": {
                        "databricks": {
                            "mode": "classic",
                        }
                    }
                }
            }
        }
    )

    assert mode == "classic"


def test_resolve_artifacts_storage_path_uses_abfss_for_classic_by_default(monkeypatch):
    api = _make_api()
    monkeypatch.delenv("KINDLING_DATABRICKS_CLASSIC_ARTIFACTS_PATH", raising=False)

    path = api._resolve_artifacts_storage_path({}, "classic")

    assert (
        path
        == "abfss://artifacts@sepstdatalakedev.dfs.core.windows.net/system-tests/run-123/databricks"
    )


def test_resolve_artifacts_storage_path_honors_classic_override(monkeypatch):
    api = _make_api()
    monkeypatch.setenv("KINDLING_DATABRICKS_CLASSIC_ARTIFACTS_PATH", "dbfs:/mnt/artifacts")

    path = api._resolve_artifacts_storage_path({}, "classic")

    assert path == "dbfs:/mnt/artifacts"


def test_resolve_python_file_uses_abfss_for_classic_by_default(monkeypatch):
    api = _make_api()
    monkeypatch.delenv("KINDLING_DATABRICKS_CLASSIC_BOOTSTRAP_ROOT", raising=False)

    python_file = api._resolve_python_file(
        main_file="kindling_bootstrap.py",
        job_config={},
        mode="classic",
        artifacts_storage_path="dbfs:/mnt/artifacts",
    )

    assert (
        python_file
        == "abfss://artifacts@sepstdatalakedev.dfs.core.windows.net/system-tests/run-123/databricks/scripts/kindling_bootstrap.py"
    )


def test_resolve_python_file_honors_explicit_classic_bootstrap_root(monkeypatch):
    api = _make_api()
    monkeypatch.setenv("KINDLING_DATABRICKS_CLASSIC_BOOTSTRAP_ROOT", "dbfs:/mnt/artifacts")

    python_file = api._resolve_python_file(
        main_file="kindling_bootstrap.py",
        job_config={},
        mode="classic",
        artifacts_storage_path="dbfs:/mnt/artifacts",
    )

    assert python_file == "dbfs:/mnt/artifacts/scripts/kindling_bootstrap.py"


def test_resolve_python_file_uses_abfss_for_uc():
    api = _make_api()

    python_file = api._resolve_python_file(
        main_file="kindling_bootstrap.py",
        job_config={},
        mode="uc",
        artifacts_storage_path="abfss://artifacts@sepstdatalakedev.dfs.core.windows.net/system-tests/run-123/databricks",
    )

    assert (
        python_file
        == "abfss://artifacts@sepstdatalakedev.dfs.core.windows.net/system-tests/run-123/databricks/scripts/kindling_bootstrap.py"
    )
