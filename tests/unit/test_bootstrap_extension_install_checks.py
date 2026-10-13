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


def test_evict_stale_modules_drops_submodules_and_namespace_parents(tmp_path, monkeypatch):
    """Evicting only a top-level name left `opentelemetry.util.types` cached
    (the Databricks `_ExtendedAttributes` import error). Stale submodules and
    their namespace-package parents go; an unchanged regular package stays
    cached and is re-attached to the recreated parent."""
    import importlib

    from kindling.bootstrap import _evict_stale_modules

    root = tmp_path / "zzns"
    (root / "util").mkdir(parents=True)  # namespace packages: no __init__.py
    (root / "util" / "types.py").write_text("VERSION = 'old'\n")
    (root / "keep").mkdir()
    (root / "keep" / "__init__.py").write_text("")
    monkeypatch.syspath_prepend(str(tmp_path))
    importlib.invalidate_caches()
    import zzns.keep  # noqa: F401
    import zzns.util.types  # noqa: F401

    kept = sys.modules["zzns.keep"]
    (root / "util" / "types.py").write_text("VERSION = 'new'\n")
    __import__("shutil").rmtree(root / "util" / "__pycache__", ignore_errors=True)
    try:
        _evict_stale_modules(MagicMock(), {"zzns.util.types"})

        assert "zzns.util.types" not in sys.modules
        assert "zzns.util" not in sys.modules
        assert sys.modules["zzns.keep"] is kept  # not stale: no second copy
        assert sys.modules["zzns"].keep is kept  # re-attached to the fresh parent
        importlib.invalidate_caches()
        assert importlib.import_module("zzns.util.types").VERSION == "new"
    finally:
        for name in [n for n in sys.modules if n == "zzns" or n.startswith("zzns.")]:
            del sys.modules[name]


def test_modules_of_changed_distributions_lists_every_module_of_changed_dists(monkeypatch):
    from types import SimpleNamespace

    import kindling.bootstrap as bootstrap

    def dist(name, version, files):
        return SimpleNamespace(metadata={"Name": name}, version=version, files=files)

    now = [
        dist(
            "opentelemetry-api",
            "1.36.0",
            [
                "opentelemetry/util/types.py",
                "opentelemetry/trace/__init__.py",
                "psutil/_psutil_linux.abi3.so",
                "opentelemetry_api-1.36.0.dist-info/RECORD",
                "../../bin/x",
            ],
        ),
        dist("azure-core", "1.34.0", ["azure/core/__init__.py"]),
    ]
    monkeypatch.setattr("importlib.metadata.distributions", lambda: iter(now))

    modules = bootstrap._modules_of_changed_distributions(
        {"opentelemetry-api": "1.32.1", "azure-core": "1.34.0"}
    )

    assert modules == {"opentelemetry.util.types", "opentelemetry.trace", "psutil._psutil_linux"}


def test_extension_import_failure_of_any_kind_is_a_warning(monkeypatch):
    """Importing a freshly installed extension can fail with more than
    ImportError (e.g. AttributeError from a stale compiled module); that must
    be a warning, like ImportError, not abort the bootstrap."""
    from kindling.bootstrap import import_distribution_modules

    monkeypatch.setattr("kindling.bootstrap.resolve_import_names", lambda name: ["broken_ext"])

    def boom(name):
        raise AttributeError("module 'psutil._psutil_linux' has no attribute 'getpagesize'")

    monkeypatch.setattr("kindling.bootstrap.importlib.util.find_spec", lambda name: object())
    monkeypatch.setattr(
        "kindling.bootstrap.importlib.import_module", boom
    )  # last: monkeypatch uses it
    logger = MagicMock()

    assert import_distribution_modules("broken-ext", logger) is False
    assert "AttributeError" in logger.warning.call_args.args[0]


def test_extension_find_spec_failure_is_a_warning(monkeypatch):
    """find_spec imports parent packages, so it can raise more than
    ImportError (the psutil AttributeError on Databricks escaped this way)."""
    from kindling.bootstrap import import_distribution_modules

    monkeypatch.setattr("kindling.bootstrap.resolve_import_names", lambda name: ["broken_ext.sub"])

    def boom(name):
        raise AttributeError("module 'psutil._psutil_linux' has no attribute 'getpagesize'")

    monkeypatch.setattr("kindling.bootstrap.importlib.util.find_spec", boom)
    logger = MagicMock()

    assert import_distribution_modules("broken-ext", logger) is False
    assert "AttributeError" in logger.warning.call_args.args[0]
