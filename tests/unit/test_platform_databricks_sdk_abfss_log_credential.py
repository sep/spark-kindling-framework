"""Unit tests for the ABFSS driver-log credential cache in kindling_sdk.platform_databricks.

Regression coverage for: get_job_logs() is polled every few seconds for the
lifetime of a running Databricks job (via stream_stdout_logs). While a run
is RUNNING, get_run_output() (PRIORITY 1) is legitimately unavailable, so
every single poll fell through to the ABFSS driver-log fallback
(PRIORITY 2), which constructed a fresh DefaultAzureCredential() on every
call. In any environment without Azure auth configured for this fallback
(e.g. CI), that repeated the full multi-credential failure chain -- and its
huge multi-paragraph error output -- on every poll for the entire job
duration. _resolve_abfss_log_credential() mirrors the
kindling.platform_databricks._resolve_dbutils() pattern: attempt
construction/auth exactly once per process and cache the outcome (including
failure), so later polls short-circuit past the fallback instead of
retrying a failure already known.
"""

import sys
import types
from unittest.mock import MagicMock

import kindling_sdk.platform_databricks as platform_databricks
import pytest
from kindling_sdk.platform_databricks import (
    DatabricksAPI,
    _resolve_abfss_log_credential,
)


@pytest.fixture(autouse=True)
def _reset_abfss_log_credential_cache():
    """The resolver caches its attempt at module scope; isolate tests from it."""
    platform_databricks._abfss_log_credential_cache = None
    platform_databricks._abfss_log_credential_attempted = False
    yield
    platform_databricks._abfss_log_credential_cache = None
    platform_databricks._abfss_log_credential_attempted = False


def _install_fake_azure_identity(monkeypatch, credential_factory):
    """Inject a fake azure.identity module exposing DefaultAzureCredential.

    Avoids depending on the real azure-identity package being installed and
    lets tests control exactly what construction/get_token do.
    """
    fake_module = types.ModuleType("azure.identity")
    fake_module.DefaultAzureCredential = credential_factory
    monkeypatch.setitem(sys.modules, "azure.identity", fake_module)


class TestResolveAbfssLogCredential:
    def test_first_call_with_failing_credential_logs_one_warning_and_returns_none(
        self, monkeypatch
    ):
        def _raise_on_construct(*args, **kwargs):
            raise Exception("no azure auth configured")

        _install_fake_azure_identity(monkeypatch, _raise_on_construct)

        logger_mock = MagicMock()
        monkeypatch.setattr(platform_databricks, "_abfss_log_credential_logger", logger_mock)

        result = _resolve_abfss_log_credential()

        assert result is None
        logger_mock.warning.assert_called_once()

    def test_second_call_does_not_reattempt_or_relog_after_failure(self, monkeypatch):
        construct_calls = []

        def _raise_on_construct(*args, **kwargs):
            construct_calls.append(1)
            raise Exception("no azure auth configured")

        _install_fake_azure_identity(monkeypatch, _raise_on_construct)

        logger_mock = MagicMock()
        monkeypatch.setattr(platform_databricks, "_abfss_log_credential_logger", logger_mock)

        first = _resolve_abfss_log_credential()
        # Remove the fake module to prove the second call doesn't re-import/reconstruct.
        monkeypatch.delitem(sys.modules, "azure.identity", raising=False)
        second = _resolve_abfss_log_credential()

        assert first is None
        assert second is None
        assert len(construct_calls) == 1
        logger_mock.warning.assert_called_once()

    def test_successful_credential_is_cached_and_reused(self, monkeypatch):
        construct_calls = []
        token_calls = []

        class _FakeCredential:
            def get_token(self, *scopes):
                token_calls.append(scopes)
                return MagicMock()

        def _construct(*args, **kwargs):
            construct_calls.append(1)
            return _FakeCredential()

        _install_fake_azure_identity(monkeypatch, _construct)

        first = _resolve_abfss_log_credential()
        # Remove the fake module to prove the second call doesn't re-import/reconstruct.
        monkeypatch.delitem(sys.modules, "azure.identity", raising=False)
        second = _resolve_abfss_log_credential()

        assert first is not None
        assert second is first
        assert len(construct_calls) == 1
        assert len(token_calls) == 1


class TestGetJobLogsAbfssFallbackShortCircuits:
    """End-to-end: get_job_logs() skips the ABFSS fallback once the credential
    resolver has already failed, instead of reconstructing/retrying it."""

    def _make_api(self):
        api = DatabricksAPI.__new__(DatabricksAPI)
        api.storage_account = "mystorageaccount"
        api.container = "artifacts"
        api.base_path = None
        return api

    def _make_run_info(self):
        run_info = MagicMock()
        run_info.tasks = []
        run_info.cluster_instance = MagicMock()
        run_info.cluster_instance.cluster_id = "cluster-123"
        run_info.cluster_instance.spark_context_id = None
        run_info.run_name = "my-job"
        run_info.state = MagicMock()
        run_info.state.life_cycle_state = None
        run_info.state.result_state = None
        run_info.state.state_message = None
        return run_info

    def test_get_job_logs_falls_through_without_constructing_credential_when_cached_failure(
        self, monkeypatch
    ):
        api = self._make_api()
        run_info = self._make_run_info()

        client = MagicMock()
        client.jobs.get_run.return_value = run_info
        client.jobs.get_run_output.side_effect = Exception("run output not available yet")
        api._client = client
        type(api).client = property(lambda self: self._client)

        # Pre-seed the cache as a known failure, as if an earlier poll already
        # attempted and failed.
        platform_databricks._abfss_log_credential_cache = None
        platform_databricks._abfss_log_credential_attempted = True

        def _fail_if_called(*args, **kwargs):
            raise AssertionError(
                "DefaultAzureCredential() should not be constructed when the "
                "credential resolver already cached a failure"
            )

        _install_fake_azure_identity(monkeypatch, _fail_if_called)

        result = api.get_job_logs("42")

        assert result["source"] == "diagnostic_info"
        assert any(
            "Azure credential unavailable for ABFSS driver-log fallback" in line
            for line in result["log"]
        )
