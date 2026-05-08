import pytest
from kindling_sdk.platform_api import (
    create_platform_api_from_env,
    list_supported_platforms,
)
from kindling_sdk.platform_synapse import SynapseAPI


def test_list_supported_platforms():
    assert list_supported_platforms() == ["databricks", "fabric", "synapse"]


def test_create_platform_api_from_env_rejects_unknown_platform():
    with pytest.raises(ValueError, match="Unknown platform"):
        create_platform_api_from_env("unknown")


def test_synapse_from_env_uses_spark_pool_name(monkeypatch):
    monkeypatch.setenv("SYNAPSE_WORKSPACE_NAME", "workspace")
    monkeypatch.setenv("SYNAPSE_SPARK_POOL_NAME", "spark-pool")
    monkeypatch.setenv("AZURE_STORAGE_ACCOUNT", "storage")
    monkeypatch.setattr(
        "kindling_sdk.platform_synapse.create_azure_credential",
        lambda credential=None: object(),
    )

    api = SynapseAPI.from_env()

    assert api.workspace_name == "workspace"
    assert api.spark_pool_name == "spark-pool"
    assert api.storage_account == "storage"
