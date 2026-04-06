from unittest.mock import MagicMock, patch

from kindling.bootstrap import install_bootstrap_dependencies


def test_extension_missing_wheel_does_not_raise_cleanup_local_variable_error():
    logger = MagicMock()

    class DBUtils:
        def __init__(self):
            self.fs = MagicMock()

    storage_utils = DBUtils()
    storage_utils.fs.ls.return_value = []

    with (
        patch("kindling.bootstrap._get_storage_utils", return_value=storage_utils),
        patch("kindling.bootstrap.importlib.util.find_spec", return_value=None),
        patch("kindling.bootstrap.importlib.import_module", return_value=object()),
    ):
        install_bootstrap_dependencies(
            logger,
            {
                "required_packages": [],
                "extensions": ["kindling-otel-azure==0.3.2"],
                "temp_path": "/Volumes/kindling/kindling/artifacts",
            },
            artifacts_storage_path="abfss://artifacts@acct/path",
        )

    error_messages = [call.args[0] for call in logger.error.call_args_list]
    assert "Extension wheel not found: kindling-otel-azure==0.3.2" in error_messages
    assert not any("local variable 'is_databricks'" in msg for msg in error_messages)
