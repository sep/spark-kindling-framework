import pytest

from kindling_sdk.platform_api import create_platform_api_from_env, list_supported_platforms


def test_list_supported_platforms():
    assert list_supported_platforms() == ["databricks", "fabric", "synapse"]


def test_create_platform_api_from_env_rejects_unknown_platform():
    with pytest.raises(ValueError, match="Unknown platform"):
        create_platform_api_from_env("unknown")
