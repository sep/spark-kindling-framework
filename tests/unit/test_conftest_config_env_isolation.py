"""
Regression coverage for tests/conftest.py::_clear_config_env_overrides.

That autouse fixture strips CONFIG__* env vars so a dev's local
Databricks/Fabric/Synapse system-test env doesn't leak into unrelated unit
tests. It used to run unconditionally for every test, which also stripped
the real CONFIG__* secrets CI injects for system tests (e.g.
CONFIG__platform_fabric__kindling__secrets__key_vault_url) before the test
body ever read them -- breaking
tests/system/core/test_config_secrets.py::TestPlatformSecretProvider
without touching a single line of that test or the env_config parsing
logic. The fixture must exempt anything marked @pytest.mark.system.
"""

import importlib.util
from types import SimpleNamespace

import pytest

_spec = importlib.util.spec_from_file_location(
    "kindling_root_conftest", __file__.rsplit("/tests/", 1)[0] + "/tests/conftest.py"
)
_root_conftest = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_root_conftest)
_clear_config_env_overrides = _root_conftest._clear_config_env_overrides.__wrapped__


def _fake_request(has_system_marker: bool):
    marker = pytest.mark.system.mark if has_system_marker else None
    node = SimpleNamespace(get_closest_marker=lambda name: marker if name == "system" else None)
    return SimpleNamespace(node=node)


def test_clears_config_env_vars_for_a_plain_test(monkeypatch):
    monkeypatch.setenv("CONFIG__kindling__temp_path", "/tmp/somewhere")

    _clear_config_env_overrides(_fake_request(has_system_marker=False), monkeypatch)

    assert "CONFIG__kindling__temp_path" not in __import__("os").environ


def test_leaves_config_env_vars_untouched_for_a_system_marked_test(monkeypatch):
    monkeypatch.setenv(
        "CONFIG__platform_fabric__kindling__secrets__key_vault_url",
        "https://example.vault.azure.net/",
    )

    _clear_config_env_overrides(_fake_request(has_system_marker=True), monkeypatch)

    import os

    assert (
        os.environ["CONFIG__platform_fabric__kindling__secrets__key_vault_url"]
        == "https://example.vault.azure.net/"
    )
