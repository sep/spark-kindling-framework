import base64
import json
from unittest.mock import MagicMock

from kindling_sdk.platform_fabric import FabricAPI
from kindling_sdk.platform_provider import azure_abfss_uri, azure_storage_account_url
from kindling_sdk.platform_synapse import SynapseAPI


def test_fabric_create_job_uses_base_path_for_default_bootstrap_script(monkeypatch):
    monkeypatch.delenv("AZURE_STORAGE_DFS_ENDPOINT_SUFFIX", raising=False)
    monkeypatch.delenv("AZURE_CLOUD", raising=False)
    api = FabricAPI.__new__(FabricAPI)
    api.workspace_id = "workspace-123"
    api.lakehouse_id = "lakehouse-123"
    api.storage_account = "mystorageaccount"
    api.container = "artifacts"
    api.base_path = "release-candidates/v0.8.2/fabric"
    api.base_url = "https://api.fabric.microsoft.com/v1"
    api._make_request = MagicMock(
        return_value=MagicMock(
            json=lambda: {
                "id": "job-123",
                "displayName": "fabric-job",
            }
        )
    )

    api.create_job("fabric-job", {})

    payload = api._make_request.call_args.kwargs["json"]
    definition_base64 = payload["definition"]["parts"][0]["payload"]
    definition_json = json.loads(base64.b64decode(definition_base64))

    assert (
        definition_json["executableFile"]
        == "abfss://artifacts@mystorageaccount.dfs.core.windows.net/release-candidates/v0.8.2/fabric/scripts/kindling_bootstrap.py"
    )

    assert "config:artifacts_storage_path=Files/artifacts/release-candidates/v0.8.2/fabric" in (
        definition_json["commandLineArguments"]
    )


def test_synapse_create_job_uses_base_path_for_default_bootstrap_script(monkeypatch):
    monkeypatch.delenv("AZURE_STORAGE_DFS_ENDPOINT_SUFFIX", raising=False)
    monkeypatch.delenv("AZURE_CLOUD", raising=False)
    api = SynapseAPI.__new__(SynapseAPI)
    api.workspace_name = "workspace-123"
    api.spark_pool_name = "spark-pool"
    api.storage_account = "mystorageaccount"
    api.container = "artifacts"
    api.base_path = "release-candidates/v0.8.2/synapse"
    api.base_url = "https://workspace-123.dev.azuresynapse.net"
    api._job_mapping = {}
    api._make_request = MagicMock(
        return_value=MagicMock(
            status_code=200,
            json=lambda: {},
        )
    )

    api.create_job("job-123", {})

    # call_args_list[0] is the PUT; call_args_list[1] is the GET readback poll
    payload = api._make_request.call_args_list[0].kwargs["json"]
    job_props = payload["properties"]["jobProperties"]

    assert (
        job_props["file"]
        == "abfss://artifacts@mystorageaccount.dfs.core.windows.net/release-candidates/v0.8.2/synapse/scripts/kindling_bootstrap.py"
    )


def test_synapse_create_job_uses_dfs_suffix_env(monkeypatch):
    monkeypatch.setenv("AZURE_STORAGE_DFS_ENDPOINT_SUFFIX", "dfs.core.usgovcloudapi.net")
    api = SynapseAPI.__new__(SynapseAPI)
    api.workspace_name = "workspace-123"
    api.spark_pool_name = "spark-pool"
    api.storage_account = "mystorageaccount"
    api.container = "artifacts"
    api.base_path = "release-candidates/v0.8.2/synapse"
    api.base_url = "https://workspace-123.dev.azuresynapse.net"
    api._job_mapping = {}
    api._make_request = MagicMock(
        return_value=MagicMock(
            status_code=200,
            json=lambda: {},
        )
    )

    api.create_job("job-123", {})

    payload = api._make_request.call_args_list[0].kwargs["json"]
    job_props = payload["properties"]["jobProperties"]
    assert (
        job_props["file"]
        == "abfss://artifacts@mystorageaccount.dfs.core.usgovcloudapi.net/release-candidates/v0.8.2/synapse/scripts/kindling_bootstrap.py"
    )


def test_storage_helpers_use_azure_environment(monkeypatch):
    monkeypatch.delenv("AZURE_STORAGE_DFS_ENDPOINT_SUFFIX", raising=False)
    monkeypatch.delenv("AZURE_STORAGE_BLOB_ENDPOINT_SUFFIX", raising=False)
    monkeypatch.setenv("AZURE_CLOUD", "AzureUSGovernment")

    assert (
        azure_storage_account_url("govacct", service="blob")
        == "https://govacct.blob.core.usgovcloudapi.net"
    )
    assert (
        azure_abfss_uri("artifacts", "govacct", "apps/job")
        == "abfss://artifacts@govacct.dfs.core.usgovcloudapi.net/apps/job"
    )


def test_synapse_run_job_passes_args_inline_to_execute():
    """run_job passes per-run args directly in the POST /execute body — no PUT."""
    api = SynapseAPI.__new__(SynapseAPI)
    api.workspace_name = "my-workspace"
    api.spark_pool_name = "spark-pool"
    api.base_url = "https://my-workspace.dev.azuresynapse.net"
    api._last_request_time = 0
    api._min_request_interval = 0
    api._execute_endpoint_available = None

    api._job_mapping = {
        "kindling-runner": {
            "file": "abfss://c@sa.dfs.core.windows.net/bootstrap.py",
            "args": ["config:app_name=kindling-runner", "config:platform=synapse"],
            "conf": {},
        }
    }

    execute_response = MagicMock(json=lambda: {"id": 77})
    api._make_request = MagicMock(return_value=execute_response)

    batch_id = api.run_job("kindling-runner", parameters={"app_name": "my-app"})

    assert batch_id == "77"
    assert api._make_request.call_count == 1, "no PUT should be issued — args go in POST body"
    (execute_call,) = api._make_request.call_args_list
    assert execute_call.args[0] == "POST"
    assert "/execute" in execute_call.args[1]
    posted_args = execute_call.kwargs["json"]["args"]
    assert "config:app_name=my-app" in posted_args
    assert not any(a.startswith("config:app_name=kindling-runner") for a in posted_args)


def test_synapse_run_job_uses_dev_endpoint_suffix_env(monkeypatch):
    """The /execute URL uses the cloud-specific Synapse endpoint suffix."""
    monkeypatch.setenv("AZURE_SYNAPSE_DEV_ENDPOINT_SUFFIX", "dev.azuresynapse.usgovcloudapi.net")

    api = SynapseAPI(
        workspace_name="my-workspace",
        spark_pool_name="spark-pool",
        credential=object(),
    )
    defn_body = {
        "name": "my-job",
        "properties": {
            "jobProperties": {
                "file": "abfss://c@sa.dfs.core.usgovcloudapi.net/bootstrap.py",
                "args": ["config:platform=synapse"],
                "conf": {},
            }
        },
    }
    api._make_request = MagicMock(
        return_value=MagicMock(
            json=lambda: {"id": 42, "state": "not_started"},
        )
    )
    api._job_mapping = {
        "my-job": {
            "file": "abfss://c@sa.dfs.core.usgovcloudapi.net/bootstrap.py",
            "args": ["config:platform=synapse"],
            "conf": {},
            "_defn_body": defn_body,
        }
    }

    batch_id = api.run_job("my-job")

    assert batch_id == "42"
    execute_call = api._make_request.call_args
    assert execute_call.args[0] == "POST"
    assert execute_call.args[1].startswith(
        "https://my-workspace.dev.azuresynapse.usgovcloudapi.net/"
    )
    assert "/execute" in execute_call.args[1]


def test_synapse_run_job_falls_back_to_get_for_cross_invocation():
    """run_job fetches the SparkJobDefinition via GET when no in-process cache exists,
    then submits via /execute."""
    api = SynapseAPI.__new__(SynapseAPI)
    api.workspace_name = "my-workspace"
    api.spark_pool_name = "spark-pool"
    api.base_url = "https://my-workspace.dev.azuresynapse.net"
    api._last_request_time = 0
    api._min_request_interval = 0
    api._execute_endpoint_available = None
    api._job_mapping = {}

    defn_response = MagicMock(
        json=lambda: {
            "properties": {
                "jobProperties": {
                    "file": "abfss://c@sa.dfs.core.windows.net/bootstrap.py",
                    "args": ["config:platform=synapse"],
                    "conf": {"spark.executor.instances": "2"},
                }
            }
        }
    )
    execute_response = MagicMock(json=lambda: {"id": 99, "state": "not_started"})
    api._make_request = MagicMock(side_effect=[defn_response, execute_response])

    batch_id = api.run_job("my-job")

    assert batch_id == "99"
    get_call, post_call = api._make_request.call_args_list
    assert get_call.args[0] == "GET"
    assert "sparkJobDefinitions/my-job" in get_call.args[1]
    assert post_call.args[0] == "POST"
    assert "/execute" in post_call.args[1]


def test_fabric_run_job_uses_api_base_url_env(monkeypatch):
    monkeypatch.setenv("FABRIC_API_BASE_URL", "https://api.fabric.microsoft.us/v1")

    api = FabricAPI(
        workspace_id="workspace-123",
        lakehouse_id="lakehouse-123",
        credential=object(),
    )
    api._make_request = MagicMock(
        return_value=MagicMock(
            status_code=202,
            headers={
                "Location": (
                    "https://api.fabric.microsoft.us/v1/workspaces/workspace-123/"
                    "items/job-123/jobs/instances/run-123"
                )
            },
        )
    )

    run_id = api.run_job("job-123")

    assert run_id == "run-123"
    call_args = api._make_request.call_args
    assert call_args.args[0] == "POST"
    assert call_args.args[1].startswith("https://api.fabric.microsoft.us/v1/")
    assert "/workspaces/workspace-123/items/job-123/jobs/instances" in call_args.args[1]
