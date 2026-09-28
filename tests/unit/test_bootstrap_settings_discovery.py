"""Settings discovery by convention (``config_dir`` / ``app_dir``).

Callers name directories; bootstrap resolves the ordered settings-file list
itself from one hierarchy table shared with the artifacts-storage download.
See docs/proposals/settings_discovery_by_convention.md.
"""

import logging
from pathlib import Path
from unittest.mock import MagicMock, patch

from kindling.bootstrap import (
    download_config_files,
    resolve_settings_files,
    settings_hierarchy,
)
from kindling.injection import GlobalInjector

# --------------------------------------------------------------------------- #
# The hierarchy table
# --------------------------------------------------------------------------- #


def test_hierarchy_order_matches_documented_convention():
    assert settings_hierarchy("prod", platform="databricks", workspace_id="ws1") == [
        ("config", "settings.yaml", None),
        ("config", "settings.databricks.yaml", "platform_databricks.yaml"),
        ("config", "workspace_ws1.yaml", None),
        ("config", "settings.prod.yaml", "env_prod.yaml"),
        ("app", "settings.yaml", None),
        ("app", "settings.databricks.yaml", "app.databricks.yaml"),
        ("app", "settings.prod.yaml", "app.prod.yaml"),
    ]


def test_hierarchy_omits_unknown_platform_workspace_and_app_layers():
    assert settings_hierarchy("dev", include_app=False) == [
        ("config", "settings.yaml", None),
        ("config", "settings.dev.yaml", "env_dev.yaml"),
    ]


# --------------------------------------------------------------------------- #
# Local resolution
# --------------------------------------------------------------------------- #


def _write(path: Path, text: str = "kindling: {}\n") -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")
    return path


def test_resolve_settings_files_returns_existing_files_in_order(tmp_path):
    config_dir = tmp_path / "config"
    app_dir = tmp_path / "apps" / "orders"
    for name in (
        "settings.yaml",
        "settings.databricks.yaml",
        "workspace_ws1.yaml",
        "settings.dev.yaml",
    ):
        _write(config_dir / name)
    for name in ("settings.yaml", "settings.dev.yaml"):
        _write(app_dir / name)
    _write(app_dir / "settings.local.yaml")  # a different environment: not selected
    _write(config_dir / "settings.prod.yaml")

    resolved = resolve_settings_files(config_dir, app_dir, "dev", "databricks", "ws1")

    assert resolved == [
        str(config_dir / "settings.yaml"),
        str(config_dir / "settings.databricks.yaml"),
        str(config_dir / "workspace_ws1.yaml"),
        str(config_dir / "settings.dev.yaml"),
        str(app_dir / "settings.yaml"),
        str(app_dir / "settings.dev.yaml"),
    ]


def test_resolve_settings_files_honours_legacy_names_only_when_canonical_is_absent(tmp_path):
    config_dir = tmp_path / "config"
    _write(config_dir / "settings.yaml")
    _write(config_dir / "platform_databricks.yaml")
    _write(config_dir / "env_prod.yaml")
    _write(config_dir / "settings.prod.yaml")  # canonical present: legacy env file ignored

    resolved = resolve_settings_files(config_dir, None, "prod", "databricks")

    assert resolved == [
        str(config_dir / "settings.yaml"),
        str(config_dir / "platform_databricks.yaml"),
        str(config_dir / "settings.prod.yaml"),
    ]


def test_resolve_settings_files_with_only_an_app_dir(tmp_path):
    app_dir = tmp_path / "apps" / "sales_ops"
    _write(app_dir / "settings.yaml")
    _write(app_dir / "settings.local.yaml")

    assert resolve_settings_files(None, app_dir, "local") == [
        str(app_dir / "settings.yaml"),
        str(app_dir / "settings.local.yaml"),
    ]
    assert resolve_settings_files(None, None, "local") == []
    assert resolve_settings_files(tmp_path / "missing", None, "local") == []


# --------------------------------------------------------------------------- #
# The download flow uses the same table
# --------------------------------------------------------------------------- #


def test_download_requests_follow_the_hierarchy_table(monkeypatch, tmp_path):
    requested = []

    class FakeFs:
        def cp(self, remote, local):
            requested.append(remote)
            Path(local.replace("file://", "")).write_text("kindling: {}\n", encoding="utf-8")

    storage = MagicMock()
    storage.fs = FakeFs()
    monkeypatch.setattr("kindling.bootstrap._get_storage_utils", lambda: storage)
    monkeypatch.setattr("kindling.bootstrap.get_temp_path", lambda: str(tmp_path))

    files = download_config_files(
        "abfss://c@a.dfs.core.windows.net/artifacts/",
        environment="prod",
        platform="synapse",
        workspace_id="ws1",
        app_name="orders",
    )

    base = "abfss://c@a.dfs.core.windows.net/artifacts"
    expected = []
    for scope, canonical, _ in settings_hierarchy("prod", "synapse", "ws1"):
        folder = "config" if scope == "config" else "data-apps/orders"
        expected.append(f"{base}/{folder}/{canonical}")
    assert requested == expected
    assert len(files) == len(expected)
    # Local copies carry an order prefix plus the app prefix.
    assert Path(files[-1]).name.endswith("_app_orders_settings.prod.yaml")


# --------------------------------------------------------------------------- #
# initialize_framework wiring
# --------------------------------------------------------------------------- #


def _config_service(values=None):
    config_service = MagicMock()
    config_service.dynaconf = None
    config_service.initial_config = {}
    config_service.get.side_effect = lambda key, default=None: (values or {}).get(key, default)
    return config_service


def _run_initialize(config):
    from kindling.bootstrap import initialize_framework
    from kindling.data_entities import DataEntityRegistry
    from kindling.data_pipes import DataPipesRegistry
    from kindling.notebook_framework import NotebookManager
    from kindling.platform_provider import PlatformServiceProvider
    from kindling.spark_config import ConfigService
    from kindling.spark_log_provider import PythonLoggerProvider

    GlobalInjector.reset()
    logger_provider = MagicMock()
    logger_provider.get_logger.return_value = MagicMock()
    config_service = _config_service(
        {
            "kindling.required_packages": [],
            "kindling.extensions": [],
            "kindling.platform.environment": "standalone",
            "kindling.bootstrap.declaration_only": False,
            "load_workspace_packages": "NOT_SET",
        }
    )
    mock_spark = MagicMock()
    mock_spark.conf.getAll.return_value = {}

    def _get_service(iface):
        if iface is ConfigService:
            return config_service
        if iface is PythonLoggerProvider:
            return logger_provider
        if iface is PlatformServiceProvider:
            return MagicMock(spec=PlatformServiceProvider)
        if iface is DataPipesRegistry:
            registry = MagicMock()
            registry.get_pipe_ids.return_value = []
            return registry
        if iface is DataEntityRegistry:
            registry = MagicMock()
            registry.get_entity_ids.return_value = []
            return registry
        if iface is NotebookManager:
            loader = MagicMock()
            loader._notebook_cache = None
            loader._folder_cache = set()
            return loader
        raise AssertionError(f"Unexpected service lookup: {iface}")

    with (
        patch("kindling.spark_config.configure_injector_with_config") as mock_configure,
        patch("kindling.bootstrap.install_bootstrap_dependencies"),
        patch("kindling.bootstrap.initialize_platform_services", return_value=MagicMock()),
        patch("kindling.bootstrap.is_framework_initialized", return_value=False),
        patch("kindling.bootstrap.get_kindling_service", side_effect=_get_service),
        patch("kindling.features.discover_runtime_features"),
        patch("kindling.bootstrap.get_feature_bool", return_value=True),
        patch("kindling.bootstrap.get_or_create_spark_session", return_value=mock_spark),
        patch("kindling.spark_session.get_or_create_spark_session", return_value=mock_spark),
    ):
        initialize_framework(config)

    return mock_configure


def test_initialize_resolves_directories_by_convention(tmp_path, caplog):
    config_dir = tmp_path / "config"
    app_dir = tmp_path / "apps" / "orders"
    _write(config_dir / "settings.yaml")
    _write(config_dir / "settings.standalone.yaml")
    _write(config_dir / "settings.dev.yaml")
    _write(app_dir / "settings.yaml")
    _write(app_dir / "settings.dev.yaml")

    with caplog.at_level(logging.WARNING):
        mock_configure = _run_initialize(
            {
                "platform": "standalone",
                "environment": "dev",
                "config_dir": str(config_dir),
                "app_dir": str(app_dir),
            }
        )

    assert mock_configure.call_args.kwargs["config_files"] == [
        str(config_dir / "settings.yaml"),
        str(config_dir / "settings.standalone.yaml"),
        str(config_dir / "settings.dev.yaml"),
        str(app_dir / "settings.yaml"),
        str(app_dir / "settings.dev.yaml"),
    ]
    assert "deprecated" not in caplog.text


def test_explicit_config_files_still_load_but_warn(tmp_path, caplog):
    explicit = _write(tmp_path / "custom.yaml")

    with caplog.at_level(logging.WARNING):
        mock_configure = _run_initialize(
            {"platform": "standalone", "environment": "dev", "config_files": [str(explicit)]}
        )

    assert mock_configure.call_args.kwargs["config_files"] == [str(explicit)]
    assert "'config_files' is deprecated" in caplog.text
    assert "config_dir" in caplog.text


def test_explicit_files_layer_after_directory_resolved_files(tmp_path):
    app_dir = tmp_path / "apps" / "orders"
    _write(app_dir / "settings.yaml")
    explicit = _write(tmp_path / "override.yaml")

    mock_configure = _run_initialize(
        {
            "platform": "standalone",
            "environment": "dev",
            "app_dir": str(app_dir),
            "config_files": [str(explicit)],
        }
    )

    assert mock_configure.call_args.kwargs["config_files"] == [
        str(app_dir / "settings.yaml"),
        str(explicit),
    ]


def test_platform_environment_in_convention_files_drives_early_platform_selection(tmp_path):
    """A `kindling.platform.environment` in a convention-found settings file is
    honoured before platform detection, as it was for explicit files."""
    config_dir = tmp_path / "config"
    _write(config_dir / "settings.yaml", "kindling:\n  platform:\n    environment: standalone\n")

    mock_configure = _run_initialize({"environment": "dev", "config_dir": str(config_dir)})

    assert mock_configure.call_args.kwargs["platform"] == "standalone"


def test_reinit_check_distinguishes_directories():
    from kindling.bootstrap import _config_service_matches_request

    service = MagicMock()
    service.dynaconf = object()
    service.initial_config = {"environment": "dev", "config_dir": "/a/config", "app_dir": "/a/app"}

    assert _config_service_matches_request(
        service, {"environment": "dev", "config_dir": "/a/config", "app_dir": "/a/app"}
    )
    assert not _config_service_matches_request(
        service, {"environment": "dev", "config_dir": "/b/config", "app_dir": "/a/app"}
    )
    assert not _config_service_matches_request(service, {"environment": "dev", "app_dir": "/b/app"})


# --------------------------------------------------------------------------- #
# Design-time copies stay in step with the runtime table
# --------------------------------------------------------------------------- #


def test_cli_raw_config_loader_matches_runtime_resolution(tmp_path):
    from kindling_cli.cli import _load_effective_raw_config

    settings_dir = tmp_path / "apps" / "orders"
    _write(settings_dir / "settings.yaml", "kindling:\n  a: base\n")
    _write(settings_dir / "platform_databricks.yaml", "kindling:\n  a: platform\n")
    _write(settings_dir / "settings.dev.yaml", "kindling:\n  a: env\n")

    merged, used = _load_effective_raw_config(settings_dir, "dev", "databricks")

    # The CLI loader reads one directory as the shared config scope, where
    # the legacy platform name is platform_<p>.yaml.
    assert [str(path) for path in used] == resolve_settings_files(
        settings_dir, None, "dev", "databricks"
    )
    assert merged["kindling"]["a"] == "env"


def test_bundle_generator_sources_match_runtime_resolution(tmp_path):
    from kindling_cli.bundle import resolve_config_sources

    project = tmp_path / "project"
    config_dir = project / "config"
    app_dir = project / "data-apps" / "orders"
    _write(config_dir / "settings.yaml")
    _write(config_dir / "platform_databricks.yaml")
    _write(config_dir / "workspace_ws1.yaml")
    _write(config_dir / "env_dev.yaml")
    _write(app_dir / "settings.yaml")
    _write(app_dir / "settings.dev.yaml")

    sources = resolve_config_sources(project, config_dir, app_dir, "dev", "ws1")

    assert [str(source.path) for source in sources if source.exists] == resolve_settings_files(
        config_dir, app_dir, "dev", "databricks", "ws1"
    )
