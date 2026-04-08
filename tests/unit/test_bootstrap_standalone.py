from unittest.mock import MagicMock, patch

from kindling.injection import GlobalInjector


def _config_service_with_defaults():
    config_service = MagicMock()
    config_service.dynaconf = None

    def _get(key, default=None):
        values = {
            "kindling.required_packages": [],
            "kindling.extensions": [],
            "kindling.platform.environment": "standalone",
            "load_local_packages": "NOT_SET",
        }
        return values.get(key, default)

    config_service.get.side_effect = _get
    return config_service


def test_detect_platform_accepts_explicit_standalone():
    from kindling.bootstrap import detect_platform

    platform = detect_platform({"platform_service": "standalone"})

    assert platform == "standalone"


def test_detect_platform_can_fall_back_to_standalone():
    from kindling.bootstrap import detect_platform

    mock_spark = MagicMock()
    mock_spark.conf.get.return_value = None

    with (
        patch("kindling.bootstrap.detect_platform_from_utils", return_value=None),
        patch("kindling.bootstrap.get_or_create_spark_session", return_value=mock_spark),
    ):
        platform = detect_platform({"allow_standalone_fallback": True})

    assert platform == "standalone"


def test_initialize_framework_uses_explicit_config_files_for_standalone():
    from kindling.bootstrap import initialize_framework
    from kindling.platform_provider import PlatformServiceProvider
    from kindling.spark_config import ConfigService
    from kindling.spark_log_provider import PythonLoggerProvider

    GlobalInjector.reset()

    logger = MagicMock()
    logger_provider = MagicMock()
    logger_provider.get_logger.return_value = logger
    config_service = _config_service_with_defaults()
    platform_service_provider = MagicMock(spec=PlatformServiceProvider)
    standalone_service = MagicMock()

    def _get_service(iface):
        if iface is ConfigService:
            return config_service
        if iface is PythonLoggerProvider:
            return logger_provider
        if iface is PlatformServiceProvider:
            return platform_service_provider
        raise AssertionError(f"Unexpected service lookup: {iface}")

    with (
        patch("kindling.spark_config.configure_injector_with_config") as mock_configure,
        patch("kindling.bootstrap.download_config_files") as mock_download,
        patch("kindling.bootstrap.install_bootstrap_dependencies") as mock_install,
        patch(
            "kindling.bootstrap.initialize_platform_services", return_value=standalone_service
        ) as mock_init_platform,
        patch("kindling.bootstrap.is_framework_initialized", return_value=False),
        patch("kindling.bootstrap.get_kindling_service", side_effect=_get_service),
        patch("kindling.features.discover_runtime_features"),
    ):
        result = initialize_framework(
            {
                "platform": "standalone",
                "environment": "local",
                "config_files": ["config/settings.yaml", "config/env.local.yaml"],
            }
        )

    assert result is standalone_service
    mock_download.assert_not_called()
    mock_install.assert_not_called()
    mock_init_platform.assert_called_once_with("standalone", config_service, logger)
    mock_configure.assert_called_once()
    assert mock_configure.call_args.kwargs["config_files"] == [
        "config/settings.yaml",
        "config/env.local.yaml",
    ]
    assert mock_configure.call_args.kwargs["platform"] == "standalone"

    GlobalInjector.reset()
