"""
Unit tests for DatabricksService secret resolution and the dbutils bridge.

Regression coverage for: on Spark Connect / ad hoc `kindling app run` jobs
(not a genuine notebook kernel), __main__.dbutils is never injected, so the
existing getattr(__main__, "dbutils", None) lookup always returned None --
silently falling through to the env-var fallback and then failing with
"Databricks secret not found", even when the job runs on a real Databricks
cluster with a perfectly good configured secret scope. _resolve_dbutils()
adds a second attempt via databricks.sdk.runtime.dbutils (Databricks' own
non-notebook dbutils bridge) before giving up.
"""

from unittest.mock import MagicMock

import pytest

import kindling.platform_databricks as platform_databricks
from kindling.platform_databricks import DatabricksService, _resolve_dbutils


def _make_service(config_values):
    svc = DatabricksService.__new__(DatabricksService)
    svc.config = config_values
    svc.logger = MagicMock()
    return svc


@pytest.fixture(autouse=True)
def _reset_dbutils_bridge_cache():
    """The bridge caches its import attempt at module scope; isolate tests from it."""
    platform_databricks._dbutils_runtime_bridge_cache = None
    platform_databricks._dbutils_runtime_bridge_attempted = False
    yield
    platform_databricks._dbutils_runtime_bridge_cache = None
    platform_databricks._dbutils_runtime_bridge_attempted = False


class _FakeSecretsApi:
    def __init__(self, values):
        self._values = values

    def get(self, scope, key):
        try:
            return self._values[(scope, key)]
        except KeyError:
            raise Exception(f"Secret does not exist with scope: {scope} key: {key}")

    def list(self, scope):
        return [MagicMock(key=k) for (s, k) in self._values if s == scope]


class _FakeDbutils:
    def __init__(self, values):
        self.secrets = _FakeSecretsApi(values)


class TestDatabricksConfiguredScopeLookup:
    """Requirement 3: Databricks configured scope/key lookup."""

    def test_get_secret_uses_configured_secret_scope(self, monkeypatch):
        import __main__

        fake_dbutils = _FakeDbutils(
            {("cwmdp", "telemetry_eh_conn_string"): "Endpoint=sb://real/;SharedAccessKey=abc"}
        )
        monkeypatch.setattr(__main__, "dbutils", fake_dbutils, raising=False)

        svc = _make_service({"kindling.secrets.secret_scope": "cwmdp"})

        result = svc.get_secret("telemetry_eh_conn_string")

        assert result == "Endpoint=sb://real/;SharedAccessKey=abc"

    def test_get_secret_explicit_scope_prefix_overrides_configured_scope(self, monkeypatch):
        import __main__

        fake_dbutils = _FakeDbutils({("other_scope", "key_name"): "resolved-value"})
        monkeypatch.setattr(__main__, "dbutils", fake_dbutils, raising=False)

        svc = _make_service({"kindling.secrets.secret_scope": "cwmdp"})

        result = svc.get_secret("other_scope:key_name")

        assert result == "resolved-value"

    def test_get_secret_raises_actionable_keyerror_when_secret_truly_missing(self, monkeypatch):
        import __main__

        fake_dbutils = _FakeDbutils({})
        monkeypatch.setattr(__main__, "dbutils", fake_dbutils, raising=False)

        svc = _make_service({"kindling.secrets.secret_scope": "cwmdp"})

        with pytest.raises(KeyError, match="telemetry_eh_conn_string"):
            svc.get_secret("telemetry_eh_conn_string")


class TestDbutilsSparkConnectBridge:
    """Requirement: Spark Connect compatibility where __main__.dbutils may not exist."""

    def test_resolve_dbutils_prefers_notebook_global_when_present(self, monkeypatch):
        """Existing notebook behavior must be untouched: when __main__.dbutils
        is present, the SDK runtime bridge is never even attempted."""
        import __main__

        notebook_dbutils = _FakeDbutils({})
        monkeypatch.setattr(__main__, "dbutils", notebook_dbutils, raising=False)

        result = _resolve_dbutils()

        assert result is notebook_dbutils
        assert platform_databricks._dbutils_runtime_bridge_attempted is False

    def test_resolve_dbutils_falls_back_to_sdk_runtime_bridge_when_main_absent(self, monkeypatch):
        import __main__

        monkeypatch.delattr(__main__, "dbutils", raising=False)

        bridge_dbutils = _FakeDbutils({("cwmdp", "k"): "v"})

        # Simulate `from databricks.sdk.runtime import dbutils` succeeding,
        # without needing real Databricks SDK auth configured in this
        # sandbox: inject a fake module under the exact import path used by
        # _resolve_dbutils()'s local import.
        import sys
        import types

        fake_runtime_module = types.ModuleType("databricks.sdk.runtime")
        fake_runtime_module.dbutils = bridge_dbutils
        monkeypatch.setitem(sys.modules, "databricks.sdk.runtime", fake_runtime_module)

        result = _resolve_dbutils()

        assert result is bridge_dbutils

    def test_resolve_dbutils_returns_none_and_does_not_raise_when_neither_available(
        self, monkeypatch
    ):
        import __main__

        monkeypatch.delattr(__main__, "dbutils", raising=False)

        # A stand-in module with no `dbutils` attribute makes
        # `from databricks.sdk.runtime import dbutils` raise ImportError
        # naturally, without patching builtins.__import__ (too broad --
        # every import in the test process would be affected).
        import sys
        import types

        poison_module = types.ModuleType("databricks.sdk.runtime")
        monkeypatch.setitem(sys.modules, "databricks.sdk.runtime", poison_module)

        result = _resolve_dbutils()

        assert result is None

    def test_resolve_dbutils_bridge_attempt_is_cached(self, monkeypatch):
        """The SDK runtime import is tried at most once per process, not once per secret lookup."""
        import __main__

        monkeypatch.delattr(__main__, "dbutils", raising=False)

        import sys
        import types

        fake_runtime_module = types.ModuleType("databricks.sdk.runtime")
        fake_runtime_module.dbutils = _FakeDbutils({})
        monkeypatch.setitem(sys.modules, "databricks.sdk.runtime", fake_runtime_module)

        first = _resolve_dbutils()
        # Remove the module to prove the second call doesn't re-import.
        monkeypatch.delitem(sys.modules, "databricks.sdk.runtime", raising=False)
        second = _resolve_dbutils()

        assert first is second

    def test_get_secret_resolves_via_sdk_bridge_when_main_dbutils_absent(self, monkeypatch):
        """End-to-end: get_secret() succeeds through the bridge, matching the
        real failure this ticket reports (ad hoc job, no __main__.dbutils)."""
        import __main__

        monkeypatch.delattr(__main__, "dbutils", raising=False)

        import sys
        import types

        bridge_dbutils = _FakeDbutils(
            {("cwmdp", "telemetry_eh_conn_string"): "Endpoint=sb://real/;SharedAccessKey=abc"}
        )
        fake_runtime_module = types.ModuleType("databricks.sdk.runtime")
        fake_runtime_module.dbutils = bridge_dbutils
        monkeypatch.setitem(sys.modules, "databricks.sdk.runtime", fake_runtime_module)

        svc = _make_service({"kindling.secrets.secret_scope": "cwmdp"})

        result = svc.get_secret("telemetry_eh_conn_string")

        assert result == "Endpoint=sb://real/;SharedAccessKey=abc"
