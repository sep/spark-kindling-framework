from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from kindling.injection import GlobalInjector


def _config_service(values=None):
    config_service = MagicMock()
    config_service.dynaconf = None
    config_service.initial_config = {}
    config_service.get.side_effect = lambda key, default=None: (values or {}).get(key, default)
    return config_service


def _run_initialize(config, config_values=None):
    from kindling.bootstrap import initialize_framework
    from kindling.data_entities import DataEntityRegistry
    from kindling.data_pipes import DataPipesRegistry
    from kindling.notebook_framework import NotebookManager
    from kindling.platform_provider import PlatformServiceProvider
    from kindling.spark_config import ConfigService
    from kindling.spark_log_provider import PythonLoggerProvider

    GlobalInjector.reset()
    logger = MagicMock()
    logger_provider = MagicMock()
    logger_provider.get_logger.return_value = logger
    config_service = _config_service(
        {
            "kindling.required_packages": [],
            "kindling.extensions": [],
            "kindling.platform.environment": config.get("platform", "standalone"),
            "kindling.bootstrap.declaration_only": False,
            "load_workspace_packages": "NOT_SET",
            **(config_values or {}),
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


def test_use_lake_packages_false_no_longer_suppresses_artifacts_config_discovery():
    from kindling.bootstrap import initialize_framework
    from kindling.data_entities import DataEntityRegistry
    from kindling.data_pipes import DataPipesRegistry
    from kindling.notebook_framework import NotebookManager
    from kindling.platform_provider import PlatformServiceProvider
    from kindling.spark_config import ConfigService
    from kindling.spark_log_provider import PythonLoggerProvider

    GlobalInjector.reset()
    logger = MagicMock()
    logger_provider = MagicMock()
    logger_provider.get_logger.return_value = logger
    config_service = _config_service(
        {
            "kindling.required_packages": [],
            "kindling.extensions": [],
            "kindling.platform.environment": "standalone",
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
        patch("kindling.spark_config.configure_injector_with_config"),
        patch("kindling.bootstrap._get_storage_utils", return_value=MagicMock()),
        patch(
            "kindling.bootstrap.download_config_files", return_value=["downloaded.yaml"]
        ) as download,
        patch("kindling.bootstrap.install_bootstrap_dependencies"),
        patch("kindling.bootstrap.initialize_platform_services", return_value=MagicMock()),
        patch("kindling.bootstrap.is_framework_initialized", return_value=False),
        patch("kindling.bootstrap.detect_platform", return_value="standalone"),
        patch("kindling.bootstrap.get_kindling_service", side_effect=_get_service),
        patch("kindling.features.discover_runtime_features"),
        patch("kindling.bootstrap.get_feature_bool", return_value=True),
        patch("kindling.bootstrap.get_or_create_spark_session", return_value=mock_spark),
        patch("kindling.spark_session.get_or_create_spark_session", return_value=mock_spark),
    ):
        initialize_framework(
            {
                "platform": "standalone",
                "use_lake_packages": False,
                "artifacts_storage_path": "abfss://artifacts@example/path",
            }
        )

    download.assert_called_once()


def test_artifacts_config_discovery_uses_explicit_workspace_and_app_layers(tmp_path):
    from kindling.bootstrap import initialize_framework
    from kindling.data_entities import DataEntityRegistry
    from kindling.data_pipes import DataPipesRegistry
    from kindling.notebook_framework import NotebookManager
    from kindling.platform_provider import PlatformServiceProvider
    from kindling.spark_config import ConfigService
    from kindling.spark_log_provider import PythonLoggerProvider

    root = tmp_path
    config_dir = root / "config"
    app_dir = root / "data-apps" / "telemetry"
    config_dir.mkdir()
    app_dir.mkdir(parents=True)
    (config_dir / "settings.yaml").write_text("layer: base\n", encoding="utf-8")
    (config_dir / "settings.databricks.yaml").write_text("layer: platform\n", encoding="utf-8")
    (config_dir / "workspace_adb-lakeflow.yaml").write_text("layer: workspace\n", encoding="utf-8")
    (config_dir / "settings.dev.yaml").write_text("layer: environment\n", encoding="utf-8")
    (app_dir / "settings.yaml").write_text("layer: app\n", encoding="utf-8")

    storage = MagicMock()

    def cp(remote, local):
        source = Path(remote)
        if not source.exists():
            raise FileNotFoundError(remote)
        Path(local.replace("file://", "")).write_text(source.read_text(encoding="utf-8"))

    storage.fs.cp = cp
    logger = MagicMock()
    logger_provider = MagicMock()
    logger_provider.get_logger.return_value = logger
    config_service = _config_service(
        {
            "kindling.required_packages": [],
            "kindling.extensions": [],
            "kindling.platform.environment": "databricks",
            "kindling.bootstrap.declaration_only": True,
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
        patch("kindling.spark_config.configure_injector_with_config") as configure,
        patch("kindling.bootstrap._get_storage_utils", return_value=storage),
        patch("kindling.bootstrap.install_bootstrap_dependencies"),
        patch("kindling.bootstrap.initialize_platform_services", return_value=MagicMock()),
        patch("kindling.bootstrap.is_framework_initialized", return_value=False),
        patch("kindling.bootstrap.detect_platform", return_value="databricks"),
        patch("kindling.bootstrap.get_kindling_service", side_effect=_get_service),
        patch("kindling.features.discover_runtime_features"),
        patch("kindling.bootstrap.get_feature_bool", return_value=True),
        patch("kindling.bootstrap.get_or_create_spark_session", return_value=mock_spark),
        patch("kindling.spark_session.get_or_create_spark_session", return_value=mock_spark),
    ):
        initialize_framework(
            {
                "artifacts_storage_path": str(root),
                "declaration_only": True,
                "environment": "dev",
                "platform": "databricks",
                "workspace_id": "adb-lakeflow",
                "app_name": "telemetry",
            }
        )

    names = [Path(path).name for path in configure.call_args.kwargs["config_files"]]
    assert names == [
        "0_settings.yaml",
        "1_settings.databricks.yaml",
        "2_workspace_adb-lakeflow.yaml",
        "3_settings.dev.yaml",
        "4_app_telemetry_settings.yaml",
    ]
    assert configure.call_args.kwargs["app_name"] == "telemetry"
    assert configure.call_args.kwargs["workspace_id"] == "adb-lakeflow"


def test_implicit_artifacts_discovery_degrades_without_storage_utils():
    with (
        patch("kindling.bootstrap._get_storage_utils", return_value=None),
        patch("kindling.bootstrap.download_config_files") as download,
    ):
        mock_configure = _run_initialize(
            {
                "platform": "standalone",
                "artifacts_storage_path": "abfss://artifacts@example/path",
            }
        )

    download.assert_not_called()
    assert mock_configure.call_args.kwargs["config_files"] == []


def test_explicit_artifacts_discovery_requires_storage_utils():
    with pytest.raises(Exception, match="Storage utilities not available"):
        with patch("kindling.bootstrap._get_storage_utils", return_value=None):
            _run_initialize(
                {
                    "platform": "standalone",
                    "artifacts_storage_path": "abfss://artifacts@example/path",
                    "discover_config_files": True,
                }
            )


def test_explicit_config_file_platform_environment_is_peeked_before_discovery(tmp_path):
    settings = tmp_path / "settings.yaml"
    settings.write_text(
        "kindling:\n" "  platform:\n" "    environment: databricks\n",
        encoding="utf-8",
    )

    with (
        patch("kindling.bootstrap._get_storage_utils", return_value=MagicMock()),
        patch("kindling.bootstrap.download_config_files", return_value=[]),
        patch(
            "kindling.bootstrap.detect_platform",
            side_effect=lambda config=None: (config or {}).get("platform_service") or "standalone",
        ) as detect_platform,
    ):
        mock_configure = _run_initialize(
            {
                "declaration_only": True,
                "config_files": [str(settings)],
                "artifacts_storage_path": "abfss://artifacts@example/path",
            },
            config_values={
                "kindling.platform.environment": "databricks",
                "kindling.bootstrap.declaration_only": True,
            },
        )

    assert detect_platform.call_args_list[0].args[0]["platform_service"] == "databricks"
    assert mock_configure.call_args.kwargs["platform"] == "databricks"
