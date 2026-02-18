from unittest.mock import MagicMock, patch

from kindling.bootstrap import install_bootstrap_dependencies


def test_extension_install_skips_when_installed_version_satisfies_requirement():
    logger = MagicMock()
    storage_utils = MagicMock()
    storage_utils.fs = MagicMock()

    with (
        patch("kindling.bootstrap._get_storage_utils", return_value=storage_utils),
        patch("kindling.bootstrap.importlib.util.find_spec", return_value=object()),
        patch("importlib.metadata.version", return_value="0.4.0"),
        patch("kindling.bootstrap.importlib.import_module", return_value=object()),
    ):
        install_bootstrap_dependencies(
            logger,
            {
                "required_packages": [],
                "extensions": ["kindling-otel-azure>=0.3.0"],
            },
            artifacts_storage_path="abfss://artifacts@acct/path",
        )

    storage_utils.fs.ls.assert_not_called()


def test_extension_install_attempts_when_installed_version_does_not_satisfy_requirement():
    logger = MagicMock()
    storage_utils = MagicMock()
    storage_utils.fs = MagicMock()
    storage_utils.fs.ls.return_value = []

    with (
        patch("kindling.bootstrap._get_storage_utils", return_value=storage_utils),
        patch("kindling.bootstrap.importlib.util.find_spec", return_value=object()),
        patch("importlib.metadata.version", return_value="0.1.0"),
        patch("kindling.bootstrap.importlib.import_module", return_value=object()),
    ):
        install_bootstrap_dependencies(
            logger,
            {
                "required_packages": [],
                "extensions": ["kindling-otel-azure>=0.3.0"],
            },
            artifacts_storage_path="abfss://artifacts@acct/path",
        )

    storage_utils.fs.ls.assert_called_once()
