"""
Unit tests for DatabricksService's ADLS SDK fallback on missing paths and
failed authentication.

Regression coverage for: on a cluster with no Azure credential, each ADLS SDK
fallback waits out DefaultAzureCredential's managed identity retries (about 80
seconds) before failing. read() fell back for every dbutils failure, so each
optional app file an app doesn't ship (app manifest, settings overlays,
requirements.txt, lake-reqs.txt) cost about 80 seconds on every app run.
"""

import sys
import types
from unittest.mock import MagicMock

import kindling.platform_databricks as platform_databricks
import pytest
from kindling.platform_databricks import DatabricksService

PATH = "abfss://artifacts@account.dfs.core.windows.net/data-apps/app/lake-reqs.txt"
DIR = "abfss://artifacts@account.dfs.core.windows.net/data-apps/app/"


class ClientAuthenticationError(Exception):
    """Stands in for azure.core.exceptions.ClientAuthenticationError."""


class CredentialUnavailableError(ClientAuthenticationError):
    """Stands in for azure.identity.CredentialUnavailableError."""


def _service():
    svc = DatabricksService.__new__(DatabricksService)
    svc.config = {}
    svc.logger = MagicMock()
    return svc


@pytest.fixture(autouse=True)
def _reset_auth_failure():
    platform_databricks._adls_auth_failure = None
    yield
    platform_databricks._adls_auth_failure = None


@pytest.fixture
def dbutils(monkeypatch):
    import __main__

    fake = MagicMock()
    monkeypatch.setattr(__main__, "dbutils", fake, raising=False)
    return fake


@pytest.fixture
def adls(monkeypatch):
    """A fake azure.storage.filedatalake whose service client is a MagicMock."""
    module = types.ModuleType("azure.storage.filedatalake")
    module.DataLakeServiceClient = MagicMock(name="DataLakeServiceClient")
    monkeypatch.setitem(sys.modules, "azure.storage.filedatalake", module)
    monkeypatch.delenv("AZURE_STORAGE_KEY", raising=False)
    monkeypatch.delenv("AZURE_STORAGE_ACCOUNT_KEY", raising=False)
    monkeypatch.setattr(DatabricksService, "_adls_credential", lambda self: object())
    return module.DataLakeServiceClient


def _not_found():
    return Exception(
        "An error occurred while calling z:...head. : java.io.FileNotFoundException: "
        'Operation failed: "The specified path does not exist.", 404, HEAD'
    )


def _download_failing_with(adls, error):
    file_client = adls.return_value.get_file_system_client.return_value.get_file_client
    file_client.return_value.download_file.side_effect = error


class TestMissingPaths:
    def test_read_of_a_missing_file_skips_the_adls_sdk(self, dbutils, adls):
        dbutils.fs.head.side_effect = _not_found()

        with pytest.raises(FileNotFoundError):
            _service().read(PATH)

        adls.assert_not_called()

    def test_read_falls_back_to_the_adls_sdk_for_other_dbutils_failures(self, dbutils, adls):
        dbutils.fs.head.side_effect = Exception("PERMISSION_DENIED: no access to the location")
        file_client = adls.return_value.get_file_system_client.return_value.get_file_client
        file_client.return_value.download_file.return_value.readall.return_value = b"x==1"

        assert _service().read(PATH) == "x==1"

    def test_list_of_a_missing_directory_skips_the_adls_sdk(self, dbutils, adls):
        dbutils.fs.ls.side_effect = _not_found()

        assert _service().list(DIR) == []
        adls.assert_not_called()


class TestAuthenticationFailure:
    def test_a_failed_authentication_skips_later_adls_calls(self, dbutils, adls):
        dbutils.fs.head.side_effect = Exception("PERMISSION_DENIED")
        dbutils.fs.cp.side_effect = Exception("PERMISSION_DENIED")
        _download_failing_with(adls, CredentialUnavailableError("no managed identity"))
        svc = _service()

        with pytest.raises(CredentialUnavailableError):
            svc.read(PATH)
        assert adls.call_count == 1

        with pytest.raises(RuntimeError, match="cannot authenticate"):
            svc.read(PATH)
        assert svc.list(DIR) == []
        with pytest.raises(Exception, match="All copy strategies failed"):
            svc.copy(PATH, "file:///tmp/lake-reqs.txt")
        assert adls.call_count == 1, "no ADLS client after the authentication failure"
        svc.logger.warning.assert_any_call(
            "The ADLS SDK fallback cannot authenticate on this cluster; skipping it "
            "for the rest of this process: no managed identity"
        )

    def test_other_adls_failures_are_not_remembered(self, dbutils, adls):
        dbutils.fs.head.side_effect = Exception("PERMISSION_DENIED")
        _download_failing_with(adls, Exception("connection reset"))
        svc = _service()

        for _ in range(2):
            with pytest.raises(Exception, match="connection reset"):
                svc.read(PATH)
        assert adls.call_count == 2

    def test_a_storage_key_still_reaches_the_adls_sdk(self, dbutils, adls, monkeypatch):
        platform_databricks._adls_auth_failure = ClientAuthenticationError("earlier")
        monkeypatch.setattr(DatabricksService, "_adls_credential", lambda self: "storage-key")
        dbutils.fs.head.side_effect = Exception("PERMISSION_DENIED")
        file_client = adls.return_value.get_file_system_client.return_value.get_file_client
        file_client.return_value.download_file.return_value.readall.return_value = b"ok"

        assert _service().read(PATH) == "ok"
