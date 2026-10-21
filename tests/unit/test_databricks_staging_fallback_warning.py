"""Warnings when Databricks bootstrap staging falls back to the DBFS root (#334).

Staging that runs before settings are loaded (the bootstrap script's wheel
staging and ``download_config_files``) sees only the job parameters. Without a
``kindling.temp_path`` there it stages under ``dbfs:/tmp`` and warns; once
settings are loaded, a settings-only ``kindling.temp_path`` is reported as
applying only to later staging.
"""

import ast
import logging
from pathlib import Path
from typing import Optional
from unittest.mock import MagicMock, patch

import pytest
from kindling.bootstrap import _resolve_bootstrap_temp_path, download_config_files
from kindling.spark_config import DynaconfConfig, configure_injector_with_config

_SHIM_PATH = Path(__file__).resolve().parents[2] / "runtime" / "scripts" / "kindling_bootstrap.py"
_SHIM_FUNCTIONS = ("_is_volume_path", "_is_dbfs_path", "_databricks_wheel_staging_target")


def _load_shim_functions():
    """Compile only the shim's staging helpers.

    Importing the bootstrap script runs its top-level code (argument parsing,
    SparkConf reads and, when a config is found, the whole bootstrap), so the
    helpers are taken from its syntax tree instead.
    """
    tree = ast.parse(_SHIM_PATH.read_text(encoding="utf-8"))
    nodes = [
        node
        for node in tree.body
        if isinstance(node, ast.FunctionDef) and node.name in _SHIM_FUNCTIONS
    ]
    assert sorted(node.name for node in nodes) == sorted(_SHIM_FUNCTIONS)
    namespace = {"Optional": Optional}
    exec(compile(ast.Module(body=nodes, type_ignores=[]), str(_SHIM_PATH), "exec"), namespace)
    return namespace["_databricks_wheel_staging_target"]


class DBUtils:
    """Stand-in for Databricks DBUtils; bootstrap detects Databricks by this class name."""

    def __init__(self):
        self.fs = _FakeDbfs()


class MsSparkUtils:
    """Stand-in for the Fabric/Synapse storage utility."""

    def __init__(self):
        self.fs = _FakeLocalFs()


class _FakeDbfs:
    def __init__(self):
        self.puts = []

    def head(self, remote_path, max_bytes):
        if remote_path.endswith("/config/settings.yaml"):
            return b"kindling: {}\n"
        raise Exception(f"FileNotFoundException: {remote_path}")

    def put(self, path, content, overwrite=False):
        self.puts.append(path)


class _FakeLocalFs:
    def cp(self, remote_path, local_uri):
        if not remote_path.endswith("/config/settings.yaml"):
            raise Exception(f"FileNotFoundException: {remote_path}")
        Path(local_uri.replace("file://", "")).write_text("kindling: {}\n", encoding="utf-8")


class _ConfigStub:
    def __init__(self, values):
        self.values = dict(values)

    def get(self, key, default=None):
        return self.values.get(key, default)

    def set(self, key, value):
        self.values[key] = value


def _dbfs_root_warnings(caplog):
    return [
        record.getMessage()
        for record in caplog.records
        if record.name == "kindling.bootstrap"
        and record.levelno == logging.WARNING
        and "dbfs:/tmp" in record.getMessage()
    ]


# --------------------------------------------------------------------------- #
# Bootstrap script: Kindling wheel staging
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize("temp_path", [None, ""])
def test_shim_warns_and_stages_wheel_on_dbfs_root_without_temp_path(capsys, temp_path):
    staging_target = _load_shim_functions()

    target = staging_target(temp_path, "spark_kindling-0.14.0-py3-none-any.whl")

    assert target == "dbfs:/tmp/spark_kindling-0.14.0-py3-none-any.whl"
    out = capsys.readouterr().out
    assert "⚠️  WARNING: No kindling.temp_path in the job parameters" in out
    assert "staging the Kindling wheel under dbfs:/tmp (DBFS root)" in out
    assert "--param kindling.temp_path=/Volumes/<catalog>/<schema>/<volume>/" in out
    assert "CONFIG__platform_databricks__kindling__temp_path" in out
    assert "Workspaces that restrict DBFS root access will fail at this step." in out


def test_shim_warns_when_temp_path_is_neither_volume_nor_dbfs(capsys):
    staging_target = _load_shim_functions()

    target = staging_target("/tmp/kindling", "k.whl")

    assert target == "dbfs:/tmp/k.whl"
    out = capsys.readouterr().out
    assert "⚠️  WARNING: kindling.temp_path='/tmp/kindling' is neither" in out
    assert "staging the Kindling wheel under dbfs:/tmp (DBFS root)" in out


@pytest.mark.parametrize(
    ("temp_path", "expected"),
    [
        ("/Volumes/main/sales/kindling/tmp", "/Volumes/main/sales/kindling/tmp/k.whl"),
        ("/Volumes/main/sales/kindling/tmp/", "/Volumes/main/sales/kindling/tmp/k.whl"),
        ("dbfs:/kindling/staging", "dbfs:/kindling/staging/k.whl"),
    ],
)
def test_shim_stages_wheel_on_configured_path_without_warning(capsys, temp_path, expected):
    staging_target = _load_shim_functions()

    assert staging_target(temp_path, "k.whl") == expected
    out = capsys.readouterr().out
    assert "WARNING" not in out
    assert "dbfs:/tmp" not in out


# --------------------------------------------------------------------------- #
# download_config_files: configuration file staging
# --------------------------------------------------------------------------- #


def test_config_download_warns_and_stages_on_dbfs_root_without_temp_path(monkeypatch, caplog):
    storage = DBUtils()
    monkeypatch.setattr("kindling.bootstrap._get_storage_utils", lambda: storage)
    monkeypatch.setattr("kindling.bootstrap.get_temp_path", lambda: "/tmp/kindling_abc123")

    with caplog.at_level(logging.INFO, logger="kindling.bootstrap"):
        files = download_config_files(
            "/Volumes/main/sales/kindling/artifacts", environment="dev", platform="databricks"
        )

    assert storage.fs.puts == ["/tmp/kindling_abc123/0_settings.yaml"]
    assert files == ["/dbfs/tmp/kindling_abc123/0_settings.yaml"]
    warnings = _dbfs_root_warnings(caplog)
    assert len(warnings) == 1
    message = warnings[0]
    assert message.startswith("No kindling.temp_path in the job parameters; staging")
    assert "configuration files under dbfs:/tmp (DBFS root: /tmp/kindling_abc123)" in message
    assert "a temp_path there does not apply here" in message
    assert "--param kindling.temp_path=/Volumes/<catalog>/<schema>/<volume>/" in message
    assert "CONFIG__platform_databricks__kindling__temp_path" in message
    assert "Workspaces that restrict DBFS root access will fail at this step." in message


@pytest.mark.parametrize(
    "temp_path", ["/Volumes/main/sales/kindling/tmp", "dbfs:/kindling/staging"]
)
def test_config_download_uses_temp_path_without_warning(monkeypatch, caplog, temp_path):
    storage = DBUtils()
    monkeypatch.setattr("kindling.bootstrap._get_storage_utils", lambda: storage)
    monkeypatch.setattr("kindling.bootstrap.get_temp_path", lambda: "/tmp/kindling_abc123")

    with caplog.at_level(logging.INFO, logger="kindling.bootstrap"):
        files = download_config_files(
            "/Volumes/main/sales/kindling/artifacts",
            environment="dev",
            platform="databricks",
            temp_path=temp_path,
        )

    assert len(storage.fs.puts) == 1
    assert storage.fs.puts[0].startswith(f"{temp_path}/.kindling-bootstrap/")
    assert files == storage.fs.puts
    assert _dbfs_root_warnings(caplog) == []


def test_config_download_on_fabric_or_synapse_does_not_warn(monkeypatch, caplog, tmp_path):
    storage = MsSparkUtils()
    monkeypatch.setattr("kindling.bootstrap._get_storage_utils", lambda: storage)
    monkeypatch.setattr("kindling.bootstrap.get_temp_path", lambda: str(tmp_path))

    with caplog.at_level(logging.INFO, logger="kindling.bootstrap"):
        files = download_config_files(
            "abfss://artifacts@acct.dfs.core.windows.net/kindling",
            environment="dev",
            platform="fabric",
        )

    assert files == [str(tmp_path / "0_settings.yaml")]
    assert _dbfs_root_warnings(caplog) == []
    assert not [r for r in caplog.records if r.levelno >= logging.WARNING]


# --------------------------------------------------------------------------- #
# _resolve_bootstrap_temp_path: settings-only kindling.temp_path
# --------------------------------------------------------------------------- #

_ARTIFACTS = "/Volumes/main/sales/kindling/artifacts"
_SETTINGS_TEMP = "/Volumes/main/sales/kindling/tmp"


def _settings_only_warnings(caplog):
    return [
        record.getMessage()
        for record in caplog.records
        if record.name == "kindling.bootstrap"
        and record.levelno == logging.WARNING
        and "comes from settings files" in record.getMessage()
    ]


def test_settings_temp_path_after_fallback_warns_naming_both_paths(caplog):
    config = _ConfigStub(
        {"kindling.platform.name": "databricks", "kindling.temp_path": _SETTINGS_TEMP}
    )
    bootstrap_config = {"platform": "databricks", "artifacts_storage_path": _ARTIFACTS}

    with caplog.at_level(logging.WARNING, logger="kindling.bootstrap"):
        resolved = _resolve_bootstrap_temp_path(config, bootstrap_config)

    # Resolution itself is unchanged: later staging uses the settings value.
    assert resolved == _SETTINGS_TEMP
    warnings = _settings_only_warnings(caplog)
    assert len(warnings) == 1
    message = warnings[0]
    assert f"kindling.temp_path={_SETTINGS_TEMP} comes from settings files" in message
    assert "applies only to later staging (such as extension wheels)" in message
    assert "early staging of the framework wheel and configuration files used dbfs:/tmp" in message
    assert f"--param kindling.temp_path={_SETTINGS_TEMP}" in message
    assert "CONFIG__platform_databricks__kindling__temp_path" in message


def test_settings_temp_path_names_only_wheel_when_config_used_derived_root(caplog):
    config = _ConfigStub(
        {"kindling.platform.name": "databricks", "kindling.temp_path": _SETTINGS_TEMP}
    )
    bootstrap_config = {
        "platform": "databricks",
        "artifacts_storage_path": _ARTIFACTS,
        "kindling.features.databricks.volumes_enabled": True,
        "kindling.databricks.volume_staging_root": "/Volumes/main/sales/kindling/stage",
    }

    with caplog.at_level(logging.WARNING, logger="kindling.bootstrap"):
        _resolve_bootstrap_temp_path(config, bootstrap_config)

    (message,) = _settings_only_warnings(caplog)
    assert "early staging of the framework wheel used dbfs:/tmp" in message


@pytest.mark.parametrize("key", ["kindling.temp_path", "temp_path"])
def test_temp_path_in_job_parameters_does_not_warn_after_load(caplog, key):
    config = _ConfigStub(
        {"kindling.platform.name": "databricks", "kindling.temp_path": _SETTINGS_TEMP}
    )
    bootstrap_config = {
        "platform": "databricks",
        "artifacts_storage_path": _ARTIFACTS,
        key: _SETTINGS_TEMP,
    }

    with caplog.at_level(logging.WARNING, logger="kindling.bootstrap"):
        assert _resolve_bootstrap_temp_path(config, bootstrap_config) == _SETTINGS_TEMP

    assert _settings_only_warnings(caplog) == []


def test_no_settings_temp_path_does_not_warn_after_load(caplog):
    config = _ConfigStub({"kindling.platform.name": "databricks"})
    bootstrap_config = {"platform": "databricks", "artifacts_storage_path": _ARTIFACTS}

    with caplog.at_level(logging.WARNING, logger="kindling.bootstrap"):
        assert _resolve_bootstrap_temp_path(config, bootstrap_config) is None

    assert _settings_only_warnings(caplog) == []


def test_nothing_staged_from_artifacts_does_not_warn_after_load(caplog):
    config = _ConfigStub(
        {"kindling.platform.name": "databricks", "kindling.temp_path": _SETTINGS_TEMP}
    )

    with caplog.at_level(logging.WARNING, logger="kindling.bootstrap"):
        _resolve_bootstrap_temp_path(config, {"platform": "databricks"})

    assert _settings_only_warnings(caplog) == []


@pytest.mark.parametrize("platform", ["fabric", "synapse", "standalone"])
def test_settings_temp_path_on_other_platforms_does_not_warn(caplog, platform):
    config = _ConfigStub({"kindling.platform.name": platform, "kindling.temp_path": "/tmp/k"})
    bootstrap_config = {"platform": platform, "artifacts_storage_path": _ARTIFACTS}

    with caplog.at_level(logging.WARNING, logger="kindling.bootstrap"):
        assert _resolve_bootstrap_temp_path(config, bootstrap_config) == "/tmp/k"

    assert not [r for r in caplog.records if r.levelno >= logging.WARNING]


# --------------------------------------------------------------------------- #
# Hot reload stages where the initial download staged
# --------------------------------------------------------------------------- #


def _configured_service_for_reload(temp_path):
    config_service = DynaconfConfig()
    with (
        patch("kindling.spark_config.GlobalInjector.get", return_value=config_service),
        patch("kindling.spark_config.Dynaconf", return_value=MagicMock()),
        patch("kindling.spark_config.get_or_create_spark_session", return_value=MagicMock()),
    ):
        configure_injector_with_config(
            config_files=[],
            environment="dev",
            artifacts_storage_path=_ARTIFACTS,
            platform="databricks",
            temp_path=temp_path,
        )
    return config_service


def _reload(config_service, monkeypatch, caplog):
    storage = DBUtils()
    monkeypatch.setattr("kindling.bootstrap._get_storage_utils", lambda: storage)
    monkeypatch.setattr("kindling.bootstrap.get_temp_path", lambda: "/tmp/kindling_abc123")
    with (
        patch("kindling.spark_config.Dynaconf", return_value=MagicMock()),
        caplog.at_level(logging.INFO, logger="kindling.bootstrap"),
    ):
        result = config_service.reload()
    assert result["status"] == "success"
    return storage


@pytest.mark.parametrize(
    "temp_path", ["/Volumes/main/sales/kindling/tmp", "dbfs:/kindling/staging"]
)
def test_reload_stages_on_initial_temp_path_without_warning(monkeypatch, caplog, temp_path):
    config_service = _configured_service_for_reload(temp_path)

    storage = _reload(config_service, monkeypatch, caplog)

    assert len(storage.fs.puts) == 1
    assert storage.fs.puts[0].startswith(f"{temp_path}/.kindling-bootstrap/")
    assert _dbfs_root_warnings(caplog) == []


def test_reload_without_temp_path_warns_and_falls_back_like_initial_download(monkeypatch, caplog):
    config_service = _configured_service_for_reload(None)

    storage = _reload(config_service, monkeypatch, caplog)

    assert storage.fs.puts == ["/tmp/kindling_abc123/0_settings.yaml"]
    (message,) = _dbfs_root_warnings(caplog)
    assert message.startswith("No kindling.temp_path in the job parameters; staging")
