from unittest.mock import MagicMock, patch

import pytest
from kindling_sdk.platform_databricks import DatabricksAPI


def _make_api() -> DatabricksAPI:
    api = DatabricksAPI.__new__(DatabricksAPI)
    api.storage_account = "mystorageaccount"
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
    monkeypatch.delenv("AZURE_STORAGE_DFS_ENDPOINT_SUFFIX", raising=False)
    monkeypatch.delenv("AZURE_CLOUD", raising=False)

    path = api._resolve_artifacts_storage_path({}, "classic")

    assert (
        path
        == "abfss://artifacts@mystorageaccount.dfs.core.windows.net/system-tests/run-123/databricks"
    )


def test_resolve_artifacts_storage_path_uses_dfs_suffix_env(monkeypatch):
    api = _make_api()
    monkeypatch.setenv("AZURE_STORAGE_DFS_ENDPOINT_SUFFIX", "dfs.core.usgovcloudapi.net")
    monkeypatch.delenv("KINDLING_DATABRICKS_CLASSIC_ARTIFACTS_PATH", raising=False)

    path = api._resolve_artifacts_storage_path({}, "classic")

    assert (
        path
        == "abfss://artifacts@mystorageaccount.dfs.core.usgovcloudapi.net/system-tests/run-123/databricks"
    )


def test_resolve_artifacts_storage_path_honors_classic_override(monkeypatch):
    api = _make_api()
    monkeypatch.setenv("KINDLING_DATABRICKS_CLASSIC_ARTIFACTS_PATH", "dbfs:/mnt/artifacts")

    path = api._resolve_artifacts_storage_path({}, "classic")

    assert path == "dbfs:/mnt/artifacts"


def test_resolve_python_file_uses_abfss_for_classic_by_default(monkeypatch):
    api = _make_api()
    monkeypatch.delenv("KINDLING_DATABRICKS_CLASSIC_BOOTSTRAP_ROOT", raising=False)
    monkeypatch.delenv("AZURE_STORAGE_DFS_ENDPOINT_SUFFIX", raising=False)
    monkeypatch.delenv("AZURE_CLOUD", raising=False)

    python_file = api._resolve_python_file(
        main_file="kindling_bootstrap.py",
        job_config={},
        mode="classic",
        artifacts_storage_path="dbfs:/mnt/artifacts",
    )

    assert (
        python_file
        == "abfss://artifacts@mystorageaccount.dfs.core.windows.net/system-tests/run-123/databricks/scripts/kindling_bootstrap.py"
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
        artifacts_storage_path="abfss://artifacts@mystorageaccount.dfs.core.windows.net/system-tests/run-123/databricks",
    )

    assert (
        python_file
        == "abfss://artifacts@mystorageaccount.dfs.core.windows.net/system-tests/run-123/databricks/scripts/kindling_bootstrap.py"
    )


# --- Bug fix: DATABRICKS_TOKEN respected in _create_sdk_client ---


def test_create_sdk_client_prefers_token_over_sp_credentials():
    """DATABRICKS_TOKEN takes priority even when Azure SP vars are also set."""
    mock_client = MagicMock()
    with patch("databricks.sdk.WorkspaceClient", return_value=mock_client) as MockClient:
        api = DatabricksAPI.__new__(DatabricksAPI)
        api.workspace_url = "https://adb-123.azuredatabricks.net"
        api.token = "dapi-mytoken"
        api.azure_tenant_id = "tenant-id"
        api.azure_client_id = "client-id"
        api.azure_client_secret = "client-secret"

        api._create_sdk_client()

        MockClient.assert_called_once_with(
            host="https://adb-123.azuredatabricks.net", token="dapi-mytoken"
        )


def test_create_sdk_client_uses_sp_when_no_token():
    """Falls through to SP auth when token is absent."""
    mock_client = MagicMock()
    with patch("databricks.sdk.WorkspaceClient", return_value=mock_client) as MockClient:
        api = DatabricksAPI.__new__(DatabricksAPI)
        api.workspace_url = "https://adb-123.azuredatabricks.net"
        api.token = None
        api.azure_tenant_id = "tenant-id"
        api.azure_client_id = "client-id"
        api.azure_client_secret = "client-secret"

        api._create_sdk_client()

        MockClient.assert_called_once_with(
            host="https://adb-123.azuredatabricks.net",
            azure_tenant_id="tenant-id",
            azure_client_id="client-id",
            azure_client_secret="client-secret",
            auth_type="azure-client-secret",
        )


def test_from_env_reads_databricks_token(monkeypatch):
    """from_env() passes DATABRICKS_TOKEN to the constructor."""
    monkeypatch.setenv("DATABRICKS_HOST", "https://adb-123.azuredatabricks.net")
    monkeypatch.setenv("DATABRICKS_TOKEN", "dapi-envtoken")
    monkeypatch.delenv("AZURE_TENANT_ID", raising=False)
    monkeypatch.delenv("AZURE_CLIENT_ID", raising=False)
    monkeypatch.delenv("AZURE_CLIENT_SECRET", raising=False)

    with patch("databricks.sdk.WorkspaceClient"):
        api = DatabricksAPI.from_env()

    assert api.token == "dapi-envtoken"


# --- Bug fix: cluster_logs_volume defaults to None; UC log delivery is opt-in ---


def _make_api_for_create_job() -> DatabricksAPI:
    api = _make_api()
    api.workspace_url = "https://adb-123.azuredatabricks.net"
    api.default_cluster_id = None
    api._client = MagicMock()
    api._client.jobs.create.return_value = MagicMock(job_id=99)
    api._job_mapping = {}
    return api


def test_create_job_omits_cluster_log_conf_when_no_uc_volume():
    """No cluster_logs_volume in config → no ClusterLogConf, no SINGLE_USER mode."""
    api = _make_api_for_create_job()

    api.create_job("test-job", {"main_file": "abfss://c@sa.dfs.core.windows.net/boot.py"})

    create_call = api._client.jobs.create.call_args
    task = create_call.kwargs["tasks"][0]
    cluster = task.new_cluster
    assert cluster.cluster_log_conf is None
    assert cluster.data_security_mode is None


def test_create_job_sets_cluster_log_conf_when_uc_volume_provided():
    """cluster_logs_volume in config → ClusterLogConf set with SINGLE_USER mode."""
    api = _make_api_for_create_job()

    api.create_job(
        "test-job",
        {
            "main_file": "abfss://c@sa.dfs.core.windows.net/boot.py",
            "cluster_logs_volume": "/Volumes/cat/schema/logs",
        },
    )

    from databricks.sdk.service.compute import DataSecurityMode

    create_call = api._client.jobs.create.call_args
    task = create_call.kwargs["tasks"][0]
    cluster = task.new_cluster
    assert cluster.cluster_log_conf is not None
    assert "test-job" in cluster.cluster_log_conf.volumes.destination
    assert cluster.data_security_mode == DataSecurityMode.SINGLE_USER


# --- Bug fix: _submit_one_time_run calls jobs.submit, not jobs.runs.submit ---


def test_submit_one_time_run_calls_jobs_submit_not_runs_submit():
    """WorkspaceClient.jobs has no .runs sub-namespace; submit() is a direct method."""
    api = _make_api_for_create_job()
    api._client.jobs.submit.return_value = MagicMock(run_id=12345)

    run_id = api._submit_one_time_run("myapp", {})

    submit_call = api._client.jobs.submit.call_args
    assert submit_call.kwargs["run_name"] == "kindling-adhoc-myapp"
    tasks = submit_call.kwargs["tasks"]
    assert len(tasks) == 1
    assert tasks[0].task_key == "main"
    assert run_id == "12345"


def test_submit_app_run_delegates_to_one_time_run():
    """submit_app_run (the CLI's ad-hoc run entry point) reaches jobs.submit."""
    api = _make_api_for_create_job()
    api._client.jobs.submit.return_value = MagicMock(run_id=67890)

    run_id = api.submit_app_run("myapp", environment="dev", parameters={"foo": "bar"})

    submit_call = api._client.jobs.submit.call_args
    assert submit_call.kwargs["run_name"] == "kindling-adhoc-myapp"
    assert run_id == "67890"


# --- gh#216: KINDLING_ARTIFACTS_STORAGE_PATH (Volumes) support ---


def test_from_env_resolves_volumes_artifacts_path(monkeypatch):
    """from_env() reads KINDLING_ARTIFACTS_STORAGE_PATH into artifacts_path."""
    monkeypatch.setenv("DATABRICKS_HOST", "https://adb-123.azuredatabricks.net")
    monkeypatch.setenv("KINDLING_ARTIFACTS_STORAGE_PATH", "/Volumes/cat/schema/vol/kindling")
    monkeypatch.delenv("AZURE_STORAGE_ACCOUNT", raising=False)

    with patch("databricks.sdk.WorkspaceClient"):
        api = DatabricksAPI.from_env()

    assert api.artifacts_path == "/Volumes/cat/schema/vol/kindling"


def test_from_env_artifacts_path_none_when_unconfigured(monkeypatch):
    """from_env() leaves artifacts_path None (and doesn't raise) when unset."""
    monkeypatch.setenv("DATABRICKS_HOST", "https://adb-123.azuredatabricks.net")
    monkeypatch.delenv("KINDLING_ARTIFACTS_STORAGE_PATH", raising=False)
    monkeypatch.delenv("AZURE_STORAGE_ACCOUNT", raising=False)

    with patch("databricks.sdk.WorkspaceClient"):
        api = DatabricksAPI.from_env()

    assert api.artifacts_path is None


def test_from_env_legacy_azure_storage_account_unaffected(monkeypatch):
    """Regression guard: AZURE_STORAGE_ACCOUNT-only setups are untouched by this fix."""
    monkeypatch.setenv("DATABRICKS_HOST", "https://adb-123.azuredatabricks.net")
    monkeypatch.setenv("AZURE_STORAGE_ACCOUNT", "mystorageaccount")
    monkeypatch.delenv("KINDLING_ARTIFACTS_STORAGE_PATH", raising=False)

    with patch("databricks.sdk.WorkspaceClient"):
        api = DatabricksAPI.from_env()

    assert api.artifacts_path is None
    assert api.storage_account == "mystorageaccount"


def test_resolve_artifacts_storage_path_prefers_artifacts_path_over_legacy_triple():
    """KINDLING_ARTIFACTS_STORAGE_PATH wins over the legacy abfss synthesis."""
    api = _make_api()
    api.artifacts_path = "/Volumes/cat/schema/vol/kindling"

    path = api._resolve_artifacts_storage_path({}, "uc")

    assert path == "/Volumes/cat/schema/vol/kindling"


def test_resolve_artifacts_storage_path_missing_attribute_falls_back(monkeypatch):
    """Test doubles that never set .artifacts_path must not raise AttributeError."""
    api = _make_api()
    monkeypatch.delenv("AZURE_STORAGE_DFS_ENDPOINT_SUFFIX", raising=False)
    monkeypatch.delenv("AZURE_CLOUD", raising=False)

    path = api._resolve_artifacts_storage_path({}, "uc")

    assert (
        path
        == "abfss://artifacts@mystorageaccount.dfs.core.windows.net/system-tests/run-123/databricks"
    )


def test_resolve_artifacts_storage_path_raises_when_nothing_configured():
    """No explicit path, no artifacts_path, no legacy triple -> actionable error, not a bare literal."""
    api = DatabricksAPI.__new__(DatabricksAPI)
    api.storage_account = None
    api.container = None
    api.base_path = None
    api.artifacts_path = None

    with pytest.raises(ValueError, match="not configured"):
        api._resolve_artifacts_storage_path({}, "uc")


def test_resolve_python_file_uses_volumes_path_for_uc():
    """_resolve_python_file has no abfss-only assumption -- a Volumes root works too."""
    api = _make_api()

    python_file = api._resolve_python_file(
        main_file="kindling_bootstrap.py",
        job_config={},
        mode="uc",
        artifacts_storage_path="/Volumes/cat/schema/vol/kindling",
    )

    assert python_file == "/Volumes/cat/schema/vol/kindling/scripts/kindling_bootstrap.py"


def test_submit_one_time_run_produces_valid_volumes_python_file():
    """End-to-end regression guard for the reported bug: a Volumes-only config must
    produce a valid, non-bare python_file for jobs.submit()."""
    api = DatabricksAPI.__new__(DatabricksAPI)
    api.storage_account = None
    api.container = None
    api.base_path = None
    api.artifacts_path = "/Volumes/cat/schema/vol/kindling"
    api.workspace_url = "https://adb-123.azuredatabricks.net"
    api.default_cluster_id = None
    api._client = MagicMock()
    api._client.jobs.submit.return_value = MagicMock(run_id=555)
    api._job_mapping = {}

    run_id = api._submit_one_time_run("myapp", {})

    submit_call = api._client.jobs.submit.call_args
    tasks = submit_call.kwargs["tasks"]
    assert (
        tasks[0].spark_python_task.python_file
        == "/Volumes/cat/schema/vol/kindling/scripts/kindling_bootstrap.py"
    )
    assert run_id == "555"
