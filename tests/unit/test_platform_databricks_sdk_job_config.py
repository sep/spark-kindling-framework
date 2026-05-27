from unittest.mock import MagicMock, patch

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
