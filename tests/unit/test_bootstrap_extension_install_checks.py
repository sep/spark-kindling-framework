from unittest.mock import MagicMock, patch

from kindling.bootstrap import (
    _resolve_initial_download_temp_path,
    _resolve_bootstrap_temp_path,
    _resolve_databricks_staging_path,
    install_bootstrap_dependencies,
)


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


class _ConfigStub:
    def __init__(self, values):
        self.values = dict(values)

    def get(self, key, default=None):
        return self.values.get(key, default)

    def set(self, key, value):
        self.values[key] = value


def test_resolve_databricks_staging_path_prefers_explicit_volume_root():
    config = _ConfigStub(
        {
            "kindling.runtime.features.databricks.volumes_enabled": True,
            "kindling.databricks.volume_staging_root": "/Volumes/medallion/default/temp",
            "kindling.storage.checkpoint_root": "/Volumes/medallion/default/other/checkpoints",
        }
    )

    assert _resolve_databricks_staging_path(config) == "/Volumes/medallion/default/temp"


def test_resolve_databricks_staging_path_derives_parent_from_checkpoint_root():
    config = _ConfigStub(
        {
            "kindling.runtime.features.databricks.volumes_enabled": True,
            "kindling.storage.checkpoint_root": "/Volumes/medallion/default/temp/checkpoints",
        }
    )

    assert _resolve_databricks_staging_path(config) == "/Volumes/medallion/default/temp"


def test_resolve_bootstrap_temp_path_updates_any_file_flag_for_databricks():
    config = _ConfigStub(
        {
            "kindling.platform.name": "databricks",
            "kindling.runtime.features.databricks.volumes_enabled": True,
            "kindling.storage.table_root": "/Volumes/medallion/default/temp/tables",
        }
    )

    resolved = _resolve_bootstrap_temp_path(config, {"platform": "databricks"})

    assert resolved == "/Volumes/medallion/default/temp"
    assert config.get("kindling.temp_path") == "/Volumes/medallion/default/temp"
    assert (
        config.get("kindling.runtime.features.databricks.any_file_required_for_bootstrap") is False
    )


def test_resolve_bootstrap_temp_path_falls_back_when_no_volume_staging_root():
    config = _ConfigStub(
        {
            "kindling.platform.name": "databricks",
            "kindling.runtime.features.databricks.volumes_enabled": False,
        }
    )

    resolved = _resolve_bootstrap_temp_path(config, {"platform": "databricks"})

    assert resolved is None
    assert (
        config.get("kindling.runtime.features.databricks.any_file_required_for_bootstrap") is True
    )


def test_resolve_initial_download_temp_path_uses_databricks_volume_parent():
    config = {
        "kindling.runtime.features.databricks.volumes_enabled": True,
        "kindling.storage.checkpoint_root": "/Volumes/medallion/default/temp/checkpoints",
    }

    resolved = _resolve_initial_download_temp_path(config, "databricks")

    assert resolved == "/Volumes/medallion/default/temp"
