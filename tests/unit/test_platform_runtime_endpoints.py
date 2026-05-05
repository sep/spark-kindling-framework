from kindling.platform_fabric import FabricService
from kindling.platform_synapse import SynapseService


def test_runtime_fabric_base_url_uses_env(monkeypatch):
    monkeypatch.setenv("FABRIC_API_BASE_URL", "https://api.fabric.microsoft.us/v1")
    svc = FabricService.__new__(FabricService)

    assert svc._build_base_url() == "https://api.fabric.microsoft.us/v1/"


def test_runtime_fabric_base_url_uses_azure_cloud(monkeypatch):
    monkeypatch.delenv("FABRIC_API_BASE_URL", raising=False)
    monkeypatch.setenv("AZURE_CLOUD", "AzureUSGovernment")

    svc = FabricService.__new__(FabricService)

    assert svc._build_base_url() == "https://api.fabric.microsoft.us/v1/"


def test_runtime_synapse_token_scope_uses_env(monkeypatch):
    monkeypatch.setenv("AZURE_SYNAPSE_TOKEN_SCOPE", "https://dev.azuresynapse.us/.default")

    svc = SynapseService.__new__(SynapseService)
    svc._token_cache = {}

    class Credential:
        def get_token(self, scope):
            self.scope = scope
            return type("Token", (), {"token": "token", "expires_on": 123})()

    credential = Credential()
    svc.credential = credential

    assert svc._get_token() == "token"
    assert credential.scope == "https://dev.azuresynapse.us/.default"


def test_runtime_synapse_token_scope_uses_azure_cloud(monkeypatch):
    monkeypatch.delenv("AZURE_SYNAPSE_TOKEN_SCOPE", raising=False)
    monkeypatch.setenv("AZURE_CLOUD", "AzureUSGovernment")

    svc = SynapseService.__new__(SynapseService)
    svc._token_cache = {}

    class Credential:
        def get_token(self, scope):
            self.scope = scope
            return type("Token", (), {"token": "token", "expires_on": 123})()

    credential = Credential()
    svc.credential = credential

    assert svc._get_token() == "token"
    assert credential.scope == "https://dev.azuresynapse.us/.default"


def test_runtime_synapse_deployment_path_uses_dfs_suffix_env(monkeypatch):
    monkeypatch.setenv("AZURE_STORAGE_DFS_ENDPOINT_SUFFIX", "dfs.core.usgovcloudapi.net")

    svc = SynapseService.__new__(SynapseService)
    svc.logger = type("Logger", (), {"info": lambda *a: None, "debug": lambda *a: None})()
    svc.workspace_name = "workspace"
    svc.storage_account = "acct"
    svc._upload_files_to_storage = lambda deployment_path, app_files: {"main.py": deployment_path}

    captured = {}

    def create_batch(job_name, entry_point, deployment_path, job_config):
        captured["deployment_path"] = deployment_path
        return {}

    svc._create_synapse_spark_batch = create_batch

    result = svc.deploy_spark_job(
        {"kindling_bootstrap.py": "print('hi')"},
        {"job_name": "job-1", "spark_pool_name": "pool"},
    )

    assert result["job_id"] == "job-1"
    assert (
        captured["deployment_path"]
        == "abfss://workspace@acct.dfs.core.usgovcloudapi.net/kindling_jobs/job-1"
    )
