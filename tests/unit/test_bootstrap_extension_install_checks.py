import site
import sys
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from kindling.bootstrap import (
    _build_run_scoped_staging_path,
    _dbfs_uri_to_local_path,
    _is_dbfs_path,
    _resolve_bootstrap_temp_path,
    _resolve_databricks_staging_path,
    _resolve_initial_download_temp_path,
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
                "extensions": ["kindling-ext-otel-azure>=0.3.0"],
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
                "extensions": ["kindling-ext-otel-azure>=0.3.0"],
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


def test_dbfs_uri_to_local_path_converts_dbfs_uri():
    assert _is_dbfs_path("dbfs:/tmp/kindling/test.whl") is True
    assert _dbfs_uri_to_local_path("dbfs:/tmp/kindling/test.whl") == "/dbfs/tmp/kindling/test.whl"


def test_build_run_scoped_staging_path_nests_under_root():
    path = _build_run_scoped_staging_path(
        "/Volumes/kindling/kindling/artifacts", "extensions", "x.whl"
    )

    assert path.startswith("/Volumes/kindling/kindling/artifacts/.kindling-bootstrap/")
    assert path.endswith("/extensions/x.whl")


def test_extension_install_uses_explicit_dbfs_temp_path_for_databricks():
    logger = MagicMock()

    class DBUtils:
        def __init__(self):
            self.fs = MagicMock()

    storage_utils = DBUtils()
    storage_utils.fs.ls.return_value = [
        SimpleNamespace(
            path="abfss://artifacts@acct/path/packages/kindling_ext_otel_azure-0.4.0-py3-none-any.whl"
        )
    ]

    with (
        patch("kindling.bootstrap._get_storage_utils", return_value=storage_utils),
        patch("kindling.bootstrap.importlib.util.find_spec", return_value=None),
        patch("os.path.exists", return_value=True),
        patch("os.path.getsize", return_value=1234),
        patch("kindling.bootstrap.subprocess.run") as subprocess_run,
        patch("kindling.bootstrap._extension_requirements", return_value=([], [])),
        patch("kindling.bootstrap.importlib.import_module", return_value=object()),
    ):
        subprocess_run.return_value = SimpleNamespace(returncode=0, stdout="", stderr="")

        install_bootstrap_dependencies(
            logger,
            {
                "required_packages": [],
                "extensions": ["kindling-ext-otel-azure>=0.3.0"],
                "temp_path": "dbfs:/tmp/kindling_extensions",
            },
            artifacts_storage_path="abfss://artifacts@acct/path",
        )

    copied_path = storage_utils.fs.cp.call_args.args[1]
    assert copied_path.startswith("dbfs:/tmp/kindling_extensions/.kindling-bootstrap/")
    assert copied_path.endswith("/extensions/kindling_ext_otel_azure-0.4.0-py3-none-any.whl")
    subprocess_run.assert_called_once()
    install_args = subprocess_run.call_args.args[0]
    assert "--ignore-installed" in install_args
    assert _dbfs_uri_to_local_path(copied_path) in install_args
    storage_utils.fs.rm.assert_called_once_with(copied_path)


def test_extension_install_accepts_platform_suffixed_extension_wheel_names():
    logger = MagicMock()

    class DBUtils:
        def __init__(self):
            self.fs = MagicMock()

    storage_utils = DBUtils()
    storage_utils.fs.ls.return_value = [
        SimpleNamespace(
            path=(
                "abfss://artifacts@acct/path/packages/"
                "kindling_ext_otel_azure_databricks-0.3.2-py3-none-any.whl"
            )
        )
    ]

    with (
        patch("kindling.bootstrap._get_storage_utils", return_value=storage_utils),
        patch("kindling.bootstrap.importlib.util.find_spec", return_value=None),
        patch("os.path.exists", return_value=True),
        patch("os.path.getsize", return_value=1234),
        patch("kindling.bootstrap.subprocess.run") as subprocess_run,
        patch("kindling.bootstrap._extension_requirements", return_value=([], [])),
        patch("kindling.bootstrap.importlib.import_module", return_value=object()),
    ):
        subprocess_run.return_value = SimpleNamespace(returncode=0, stdout="", stderr="")

        install_bootstrap_dependencies(
            logger,
            {
                "required_packages": [],
                "extensions": ["kindling-ext-otel-azure==0.3.2"],
                "temp_path": "dbfs:/tmp/kindling_extensions",
            },
            artifacts_storage_path="abfss://artifacts@acct/path",
        )

    copied_path = storage_utils.fs.cp.call_args.args[1]
    assert copied_path.endswith(
        "/extensions/kindling_ext_otel_azure_databricks-0.3.2-py3-none-any.whl"
    )
    subprocess_run.assert_called_once()


def test_extension_install_prioritizes_new_site_packages_and_clears_stale_dependency_imports():
    logger = MagicMock()

    class DBUtils:
        def __init__(self):
            self.fs = MagicMock()

    storage_utils = DBUtils()
    storage_utils.fs.ls.return_value = [
        SimpleNamespace(
            path=(
                "abfss://artifacts@acct/path/packages/"
                "kindling_ext_otel_azure_databricks-0.3.2-py3-none-any.whl"
            )
        )
    ]

    fake_typing_extensions = object()
    fake_opentelemetry = object()
    original_typing_extensions = sys.modules.get("typing_extensions")
    original_opentelemetry = sys.modules.get("opentelemetry")
    sys.modules["typing_extensions"] = fake_typing_extensions
    sys.modules["opentelemetry"] = fake_opentelemetry

    try:
        with (
            patch("kindling.bootstrap._get_storage_utils", return_value=storage_utils),
            patch("kindling.bootstrap.importlib.util.find_spec", return_value=None),
            patch("os.path.exists", return_value=True),
            patch("os.path.getsize", return_value=1234),
            patch("kindling.bootstrap.subprocess.run") as subprocess_run,
            patch("kindling.bootstrap._extension_requirements", return_value=([], [])),
            patch.object(site, "getusersitepackages", return_value="/user/site"),
            patch.object(site, "getsitepackages", return_value=["/venv/site"]),
            patch("kindling.bootstrap.importlib.import_module", return_value=object()),
        ):
            subprocess_run.return_value = SimpleNamespace(returncode=0, stdout="", stderr="")

            install_bootstrap_dependencies(
                logger,
                {
                    "required_packages": [],
                    "extensions": ["kindling-ext-otel-azure==0.3.2"],
                    "temp_path": "dbfs:/tmp/kindling_extensions",
                },
                artifacts_storage_path="abfss://artifacts@acct/path",
            )

        install_args = subprocess_run.call_args.args[0]
        assert "--ignore-installed" in install_args
        assert "typing_extensions" not in sys.modules
        assert "opentelemetry" not in sys.modules
        assert sys.path[0] == "/user/site"
        assert sys.path[1] == "/venv/site"
    finally:
        sys.modules.pop("typing_extensions", None)
        sys.modules.pop("opentelemetry", None)
        if original_typing_extensions is not None:
            sys.modules["typing_extensions"] = original_typing_extensions
        if original_opentelemetry is not None:
            sys.modules["opentelemetry"] = original_opentelemetry
        sys.path[:] = [path for path in sys.path if path not in {"/user/site", "/venv/site"}]


def _wheel_with_requirements(tmp_path, requirements):
    import zipfile

    path = tmp_path / "kindling_ext_demo-0.14.0-py3-none-any.whl"
    metadata = "Metadata-Version: 2.1\nName: kindling-ext-demo\nVersion: 0.14.0\n" + "".join(
        f"Requires-Dist: {requirement}\n" for requirement in requirements
    )
    with zipfile.ZipFile(path, "w") as wheel:
        wheel.writestr("kindling_ext_demo-0.14.0.dist-info/METADATA", metadata)
    return path


def test_extension_requirements_split_kindling_from_others(tmp_path):
    from kindling.bootstrap import _extension_requirements

    wheel = _wheel_with_requirements(
        tmp_path,
        [
            "spark-kindling<0.15,>=0.14.0",
            "spark-kindling-ext-sdp<0.15,>=0.14.0",
            "azure-monitor-opentelemetry<1.7.0,>=1.6.0",
            'pyspark<4.0.0,>=3.4.0; extra == "spark_3_x"',
            'tomli>=2; python_version < "3.0"',
        ],
    )

    others, kindling = _extension_requirements(wheel)

    assert others == ["azure-monitor-opentelemetry<1.7.0,>=1.6.0"]
    assert [r.name for r in kindling] == ["spark-kindling", "spark-kindling-ext-sdp"]


def test_require_installed_kindling_accepts_matching_minor():
    from kindling.bootstrap import _require_installed_kindling
    from packaging.requirements import Requirement

    with patch("importlib.metadata.version", return_value="0.14.2"):
        _require_installed_kindling("ext", [Requirement("spark-kindling>=0.14.0,<0.15")])


def test_require_installed_kindling_rejects_other_minor_with_both_versions():
    import pytest
    from kindling.bootstrap import _require_installed_kindling
    from packaging.requirements import Requirement

    with patch("importlib.metadata.version", return_value="0.15.0"):
        with pytest.raises(RuntimeError, match="spark-kindling 0.15.0 is installed"):
            _require_installed_kindling("ext", [Requirement("spark-kindling>=0.14.0,<0.15")])


def _only_kindling_installed(kindling_version):
    from importlib.metadata import PackageNotFoundError

    def version(name):
        if name.replace("_", "-").lower() == "spark-kindling":
            return kindling_version
        raise PackageNotFoundError(name)

    return version


def test_extension_install_never_installs_kindling_from_an_index(tmp_path):
    """The extension wheel is installed with --no-deps and only its other
    requirements follow, so pip never reinstalls the Kindling runtime the
    bootstrap installed."""
    logger = MagicMock()
    storage_utils = MagicMock()
    storage_utils.fs = MagicMock()
    wheel = _wheel_with_requirements(
        tmp_path, ["spark-kindling<0.15,>=0.14.0", "typing-extensions>=4.6.0"]
    )
    storage_utils.fs.ls.return_value = [
        SimpleNamespace(path=f"abfss://artifacts@acct/path/packages/{wheel.name}")
    ]
    storage_utils.fs.cp.side_effect = lambda src, dst, *a, **k: __import__("shutil").copy(
        wheel, dst.replace("file://", "")
    )

    with (
        patch("kindling.bootstrap._get_storage_utils", return_value=storage_utils),
        patch("kindling.bootstrap.importlib.util.find_spec", return_value=None),
        patch("importlib.metadata.version", side_effect=_only_kindling_installed("0.14.1")),
        patch("kindling.bootstrap.subprocess.run") as subprocess_run,
        patch("kindling.bootstrap.importlib.import_module", return_value=object()),
    ):
        subprocess_run.return_value = SimpleNamespace(returncode=0, stdout="", stderr="")
        install_bootstrap_dependencies(
            logger,
            {"required_packages": [], "extensions": ["kindling-ext-demo"]},
            artifacts_storage_path="abfss://artifacts@acct/path",
        )

    calls = [c.args[0] for c in subprocess_run.call_args_list]
    assert len(calls) == 2, logger.error.call_args_list
    assert "--no-deps" in calls[0] and calls[0][-1].endswith(wheel.name)
    assert calls[1][-1] == "typing-extensions>=4.6.0"
    assert not any("spark-kindling" in arg for call in calls for arg in call[1:])
