from unittest.mock import ANY, MagicMock, call, patch

from kindling.injection import GlobalInjector


def _config_service(values):
    config_service = MagicMock()
    config_service.dynaconf = None
    config_service.initial_config = {}
    config_service.get.side_effect = lambda key, default=None: values.get(key, default)
    return config_service


def _initialize_with_mocks(config, config_values=None, platform_side_effect=None):
    from kindling.bootstrap import initialize_framework
    from kindling.data_apps import DataAppRunner
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
            "kindling.platform.environment": "databricks",
            "load_workspace_packages": "NOT_SET",
            **(config_values or {}),
        }
    )
    platform_service_provider = MagicMock(spec=PlatformServiceProvider)
    notebook_manager = MagicMock()
    notebook_manager._notebook_cache = None
    notebook_manager._folder_cache = set()
    pipes_registry = MagicMock()
    pipes_registry.get_pipe_ids.return_value = []
    entity_registry = MagicMock()
    entity_registry.get_entity_ids.return_value = []
    data_app_runner = MagicMock(spec=DataAppRunner)
    data_app_runner.run_app.side_effect = AssertionError("declaration must not run app")
    mock_spark = MagicMock()
    mock_spark.conf.getAll.return_value = {}
    mock_spark.conf.set.side_effect = AssertionError("declaration must not write SparkConf")

    def _get_service(iface):
        if iface is ConfigService:
            return config_service
        if iface is PythonLoggerProvider:
            return logger_provider
        if iface is PlatformServiceProvider:
            return platform_service_provider
        if iface is DataPipesRegistry:
            return pipes_registry
        if iface is DataEntityRegistry:
            return entity_registry
        if iface is NotebookManager:
            return notebook_manager
        if iface is DataAppRunner:
            return data_app_runner
        raise AssertionError(f"Unexpected service lookup: {iface}")

    standalone_service = MagicMock(name="standalone_service")
    standalone_service._get_token.side_effect = AssertionError(
        "declaration must not acquire platform tokens"
    )
    standalone_service.storage.write.side_effect = AssertionError(
        "declaration must not write through platform storage"
    )
    standalone_service.write.side_effect = AssertionError(
        "declaration must not write through platform services"
    )
    side_effect = platform_side_effect or [standalone_service]

    with (
        patch("kindling.spark_config.configure_injector_with_config"),
        patch("kindling.bootstrap.download_config_files", return_value=[]),
        patch("kindling.bootstrap.install_bootstrap_dependencies") as mock_install,
        patch(
            "kindling.bootstrap.initialize_platform_services", side_effect=side_effect
        ) as mock_init_platform,
        patch("kindling.bootstrap.is_framework_initialized", return_value=False),
        patch("kindling.bootstrap.detect_platform", return_value="databricks"),
        patch("kindling.bootstrap.get_kindling_service", side_effect=_get_service),
        patch("kindling.features.discover_runtime_features"),
        patch("kindling.bootstrap.get_feature_bool", return_value=True),
        patch("kindling.bootstrap.get_or_create_spark_session", return_value=mock_spark),
        patch("kindling.spark_session.get_or_create_spark_session", return_value=mock_spark),
        patch("kindling.watermarking.WatermarkAspect.register") as mock_watermark,
        patch("kindling.bootstrap.load_workspace_packages") as mock_load_workspace_packages,
    ):
        result = initialize_framework(config)

    return {
        "result": result,
        "logger": logger,
        "mock_install": mock_install,
        "mock_init_platform": mock_init_platform,
        "mock_watermark": mock_watermark,
        "mock_load_workspace_packages": mock_load_workspace_packages,
        "notebook_manager": notebook_manager,
        "data_app_runner": data_app_runner,
        "mock_spark": mock_spark,
    }


def test_declaration_only_suppresses_runtime_side_effects():
    result = _initialize_with_mocks(
        {
            "declaration_only": True,
            "app_name": "orders",
            "install_bootstrap_dependencies": True,
            "load_workspace_packages": True,
        },
        config_values={"kindling.bootstrap.declaration_only": True},
    )

    result["mock_install"].assert_not_called()
    result["mock_watermark"].assert_not_called()
    result["mock_load_workspace_packages"].assert_not_called()
    result["data_app_runner"].run_app.assert_not_called()
    result["result"]._get_token.assert_not_called()
    result["result"].storage.write.assert_not_called()
    result["result"].write.assert_not_called()
    result["mock_spark"].conf.set.assert_not_called()
    assert result["notebook_manager"]._notebook_cache == []


def test_declaration_only_falls_back_when_platform_service_constructor_raises():
    standalone_service = MagicMock(name="standalone_service")

    result = _initialize_with_mocks(
        {"declaration_only": True},
        config_values={"kindling.bootstrap.declaration_only": True},
        platform_side_effect=[Exception("No workspace_id provided"), standalone_service],
    )

    assert result["result"] is standalone_service
    assert result["mock_init_platform"].call_args_list == [
        call("databricks", ANY, result["logger"]),
        call("standalone", ANY, result["logger"]),
    ]
    assert any(
        warning.args[:2]
        == (
            "Platform service '%s' could not be constructed during "
            "declaration-only initialization; falling back to standalone "
            "service for declaration-time operations: %s",
            "databricks",
        )
        for warning in result["logger"].warning.call_args_list
    )


def test_without_declaration_only_app_name_still_runs_app():
    from kindling.bootstrap import initialize_framework
    from kindling.data_apps import DataAppRunner
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
            "kindling.bootstrap.declaration_only": False,
            "load_workspace_packages": "NOT_SET",
        }
    )
    runner = MagicMock(spec=DataAppRunner)
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
        if iface is DataAppRunner:
            return runner
        raise AssertionError(f"Unexpected service lookup: {iface}")

    with (
        patch("kindling.spark_config.configure_injector_with_config"),
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
        initialize_framework({"platform": "standalone", "app_name": "orders"})

    runner.run_app.assert_called_once_with("orders")
