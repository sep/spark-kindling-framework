import os
import site
import sys
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest
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
        # Installed metadata: no Kindling requirement to check.
        patch("importlib.metadata.requires", return_value=[]),
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
    assert "--ignore-installed" not in install_args
    assert "--upgrade" not in install_args
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


def _install_demo_extension(tmp_path, requirements, kindling_version, results, importable=False):
    """Run install_bootstrap_dependencies for one extension wheel with the
    given Requires-Dist; ``results`` are the returncodes of successive pip
    calls. Returns (pip argument lists, logger)."""
    logger = MagicMock()
    storage_utils = MagicMock()
    storage_utils.fs = MagicMock()
    wheel = _wheel_with_requirements(tmp_path, requirements)
    storage_utils.fs.ls.return_value = [
        SimpleNamespace(path=f"abfss://artifacts@acct/path/packages/{wheel.name}")
    ]
    storage_utils.fs.cp.side_effect = lambda src, dst, *a, **k: __import__("shutil").copy(
        wheel, dst.replace("file://", "")
    )

    with (
        patch("kindling.bootstrap._get_storage_utils", return_value=storage_utils),
        patch(
            "kindling.bootstrap.importlib.util.find_spec",
            return_value=object() if importable else None,
        ),
        patch("importlib.metadata.version", side_effect=_only_kindling_installed(kindling_version)),
        patch("kindling.bootstrap.subprocess.run") as subprocess_run,
        patch("kindling.bootstrap.importlib.import_module", return_value=object()),
    ):
        subprocess_run.side_effect = [
            SimpleNamespace(returncode=code, stdout="", stderr="" if code == 0 else "boom")
            for code in results
        ]
        install_bootstrap_dependencies(
            logger,
            {"required_packages": [], "extensions": ["kindling-ext-demo"]},
            artifacts_storage_path="abfss://artifacts@acct/path",
        )
    return [c.args[0] for c in subprocess_run.call_args_list], logger


def test_extension_install_never_installs_kindling_from_an_index(tmp_path):
    """The extension's other requirements are installed first, then the
    wheel with --no-deps, so pip never reinstalls the Kindling runtime the
    bootstrap installed."""
    calls, logger = _install_demo_extension(
        tmp_path,
        ["spark-kindling<0.15,>=0.14.0", "typing-extensions>=4.6.0"],
        "0.14.1",
        [0, 0],
    )

    assert len(calls) == 2, logger.error.call_args_list
    assert calls[0][-1] == "typing-extensions>=4.6.0"
    assert "--no-deps" in calls[1] and calls[1][-1].endswith(
        "kindling_ext_demo-0.14.0-py3-none-any.whl"
    )
    assert not any("spark-kindling" in arg for call in calls for arg in call[1:])


def test_extension_wheel_is_not_installed_when_its_dependencies_fail(tmp_path):
    """Otherwise the next bootstrap finds the extension installed and never
    retries its requirements."""
    calls, logger = _install_demo_extension(
        tmp_path,
        ["spark-kindling<0.15,>=0.14.0", "typing-extensions>=4.6.0"],
        "0.14.1",
        [1],
    )

    assert len(calls) == 1
    assert calls[0][-1] == "typing-extensions>=4.6.0"
    assert not any("--no-deps" in call for call in calls)
    assert logger.error.called


def test_extension_for_another_kindling_minor_fails_the_bootstrap(tmp_path):
    from kindling.bootstrap import ExtensionCompatibilityError

    with pytest.raises(ExtensionCompatibilityError, match="0.13.5 is installed"):
        _install_demo_extension(tmp_path, ["spark-kindling<0.15,>=0.14.0"], "0.13.5", [])


def test_importable_extension_without_metadata_is_reinstalled_and_checked(tmp_path):
    """A runtime cleanup once removed extensions' .dist-info but left their
    packages: the extension imports, yet its requirements can't be read. It
    is reinstalled from the artifacts store instead of trusted, so the
    wheel's Kindling requirement is checked."""
    calls, logger = _install_demo_extension(
        tmp_path,
        ["spark-kindling<0.15,>=0.14.0", "typing-extensions>=4.6.0"],
        "0.14.1",
        [0, 0],
        importable=True,
    )
    assert len(calls) == 2, logger.error.call_args_list
    assert "--no-deps" in calls[1]

    from kindling.bootstrap import ExtensionCompatibilityError

    with pytest.raises(ExtensionCompatibilityError, match="0.13.5 is installed"):
        _install_demo_extension(
            tmp_path, ["spark-kindling<0.15,>=0.14.0"], "0.13.5", [], importable=True
        )


def test_installed_extension_is_checked_against_the_runtime(monkeypatch):
    """An extension installed earlier (it satisfies its spec, so it is not
    reinstalled) must still match the active runtime after an upgrade."""
    import importlib.metadata

    from kindling.bootstrap import ExtensionCompatibilityError

    def requires(name):
        if name.replace("_", "-") == "kindling-ext-demo":
            return ["spark-kindling<0.14,>=0.13.0", "typing-extensions>=4.6.0"]
        raise importlib.metadata.PackageNotFoundError(name)

    monkeypatch.setattr(importlib.metadata, "requires", requires)
    storage_utils = MagicMock()
    with (
        patch("kindling.bootstrap._get_storage_utils", return_value=storage_utils),
        patch("kindling.bootstrap.importlib.util.find_spec", return_value=None),
        patch(
            "importlib.metadata.version",
            side_effect=lambda name: (
                "0.14.0"
                if name.replace("_", "-").lower() == "spark-kindling"
                else "0.13.2" if name.replace("_", "-") == "kindling-ext-demo" else None
            ),
        ),
        patch("kindling.bootstrap.subprocess.run") as subprocess_run,
    ):
        with pytest.raises(ExtensionCompatibilityError, match="0.14.0 is installed"):
            install_bootstrap_dependencies(
                MagicMock(),
                {"required_packages": [], "extensions": ["kindling-ext-demo"]},
                artifacts_storage_path="abfss://artifacts@acct/path",
            )
    subprocess_run.assert_not_called()


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


def test_modules_of_changed_distributions_includes_modules_the_upgrade_removed(monkeypatch):
    """An upgrade can drop or rename a module that is already loaded; the new
    file list no longer names it, so the pre-install snapshot supplies it."""
    from types import SimpleNamespace

    import kindling.bootstrap as bootstrap

    def dist(name, version, files):
        return SimpleNamespace(metadata={"Name": name}, version=version, files=files)

    old = [dist("opentelemetry-api", "1.32.1", ["opentelemetry/util/_old.py"])]
    new = [dist("opentelemetry-api", "1.36.0", ["opentelemetry/util/types.py"])]
    monkeypatch.setitem(sys.modules, "opentelemetry.util._old", object())

    monkeypatch.setattr("importlib.metadata.distributions", lambda: iter(old))
    before = bootstrap._installed_distribution_snapshot()
    monkeypatch.setattr("importlib.metadata.distributions", lambda: iter(new))

    modules = bootstrap._modules_of_changed_distributions(before)

    assert modules == {"opentelemetry.util.types", "opentelemetry.util._old"}


def _fake_dist(name, version, files, location="/venv/site"):
    return SimpleNamespace(
        metadata={"Name": name},
        version=version,
        files=files,
        locate_file=lambda path, location=location: f"{location}/{path}".rstrip("/"),
    )


def test_key_distribution_constraints_pin_installed_key_packages_only():
    from kindling.bootstrap import _key_distribution_constraints

    constraints = _key_distribution_constraints(
        {
            "opentelemetry-api": ("1.32.1", frozenset()),
            "azure-core": "1.30.2",
            "azure-identity": "2023.12.01-conda",  # not a PEP 440 version
            "requests": "2.32.3",  # not a key package
        }
    )

    assert constraints == ["azure-core==1.30.2", "opentelemetry-api==1.32.1"]


def _completed(returncode=0, stdout="", stderr=""):
    return SimpleNamespace(returncode=returncode, stdout=stdout, stderr=stderr)


def _run_pip_install(monkeypatch, results, installed):
    """Run _pip_install_keeping_installed with subprocess.run returning
    `results` in turn; returns (result, [(command, constraints file text)])."""
    import kindling.bootstrap as bootstrap

    calls = []
    pending = list(results)

    def run(command, **kwargs):
        constraints = None
        if "-c" in command:
            with open(command[command.index("-c") + 1], encoding="utf-8") as handle:
                constraints = handle.read()
        calls.append((command, constraints))
        return pending.pop(0)

    monkeypatch.setattr(bootstrap, "_installed_distribution_versions", lambda: dict(installed))
    monkeypatch.setattr(bootstrap.subprocess, "run", run)
    result = bootstrap._pip_install_keeping_installed(MagicMock(), ["/tmp/ext.whl"])
    return result, calls


def test_pip_install_keeps_installed_packages_and_pins_key_ones(monkeypatch):
    result, calls = _run_pip_install(
        monkeypatch,
        [_completed()],
        {"opentelemetry-sdk": "1.32.1", "azure-core": "1.34.0", "requests": "2.32.3"},
    )

    assert result.returncode == 0
    [(command, constraints)] = calls
    assert command[:4] == [sys.executable, "-m", "pip", "install"]
    assert command[-1] == "/tmp/ext.whl"
    for flag in ("--ignore-installed", "--upgrade", "--upgrade-strategy", "--force-reinstall"):
        assert flag not in command
    assert constraints == "azure-core==1.34.0\nopentelemetry-sdk==1.32.1\n"


def test_pip_install_retries_with_ignore_installed_when_pip_cannot_uninstall(monkeypatch):
    uninstall_error = _completed(
        1, stderr="error: uninstall-no-record-file\n× Cannot uninstall typing_extensions 4.9.0"
    )
    result, calls = _run_pip_install(
        monkeypatch, [uninstall_error, _completed()], {"azure-core": "1.30.2"}
    )

    assert result.returncode == 0
    assert "--ignore-installed" not in calls[0][0]
    assert "--ignore-installed" in calls[1][0]
    assert calls[1][1] == "azure-core==1.30.2\n"  # key packages stay pinned


def test_pip_install_drops_key_pins_only_when_requirements_cannot_be_met(monkeypatch):
    conflict = _completed(1, stderr="ERROR: ResolutionImpossible: ...")
    result, calls = _run_pip_install(
        monkeypatch, [conflict, _completed()], {"opentelemetry-api": "1.16.0"}
    )

    assert result.returncode == 0
    assert calls[0][1] == "opentelemetry-api==1.16.0\n"
    assert "-c" not in calls[1][0]


@pytest.mark.parametrize(
    "stderr",
    [
        "ERROR: Could not install packages due to an OSError: [Errno 28] No space left on device",
        "ERROR: THESE PACKAGES DO NOT MATCH THE HASHES FROM THE REQUIREMENTS FILE.",
        "WARNING: Retrying (Retry(total=4)) after connection broken by 'ReadTimeoutError'",
    ],
)
def test_pip_install_keeps_pins_for_failures_that_are_not_conflicts(monkeypatch, stderr):
    """Only a resolver conflict drops the runtime pins: retrying unpinned after
    an unrelated failure could replace the packages the pins protect."""
    failure = _completed(1, stderr=stderr)
    result, calls = _run_pip_install(monkeypatch, [failure], {"azure-core": "1.30.2"})

    assert result.returncode == 1
    assert len(calls) == 1


def test_pip_install_returns_the_failure_once_fallbacks_are_exhausted(monkeypatch):
    failure = _completed(1, stderr="ERROR: No matching distribution found for x")
    result, calls = _run_pip_install(monkeypatch, [failure, failure], {"numpy": "1.26.4"})

    assert result.returncode == 1
    assert len(calls) == 2  # pinned, then unpinned

    result, calls = _run_pip_install(monkeypatch, [failure], {})
    assert result.returncode == 1
    assert len(calls) == 1  # nothing to unpin


def test_colocate_reinstalls_regular_package_owner_next_to_added_modules(monkeypatch):
    """azure-core-tracing-opentelemetry adds azure/core/tracing/ext/... to the
    job's virtualenv while azure-core stays in the runtime's site-packages;
    azure.core resolves to the latter only, so the plugin can't be imported
    until azure-core (same version) is installed next to it. A plugin that had
    installed into azure-core there moves with it, or the new copy would hide
    it. Loaded azure.core packages are kept (a preloaded SDK's exception
    classes stay the ones later imports get) and gain the new directory."""
    import types

    import kindling.bootstrap as bootstrap

    azure_core_files = [
        "azure/core/__init__.py",
        "azure/core/exceptions.py",
        "azure/core/tracing/__init__.py",
        "azure/core/tracing/ext/__init__.py",
    ]
    before = [
        _fake_dist(
            "azure-core-tracing-opentelemetry",
            "1.0.0b13",
            ["azure/core/tracing/ext/opentelemetry_span/__init__.py"],
            location="/venv/site",
        ),
        _fake_dist("azure-core", "1.34.0", azure_core_files, location="/system/site"),
        _fake_dist(
            "azure-core-plugin",
            "2.0",
            ["azure/core/plugin/__init__.py"],
            location="/system/site",
        ),
        _fake_dist("unrelated", "1.0", ["unrelated/__init__.py"], location="/system/site"),
    ]
    after = [
        before[0],
        _fake_dist("azure-core", "1.34.0", azure_core_files, location="/venv/site"),
        _fake_dist(
            "azure-core-plugin", "2.0", ["azure/core/plugin/__init__.py"], location="/venv/site"
        ),
        *before[1:],
    ]
    run = MagicMock(return_value=_completed())
    monkeypatch.setattr(
        "importlib.metadata.distributions", lambda: iter(after if run.called else before)
    )
    monkeypatch.setattr(bootstrap.subprocess, "run", run)

    azure_core = types.ModuleType("azure.core")
    azure_core.__path__ = ["/system/site/azure/core"]
    tracing_ext = types.ModuleType("azure.core.tracing.ext")
    tracing_ext.__path__ = ["/system/site/azure/core/tracing/ext"]
    exceptions = types.ModuleType("azure.core.exceptions")
    exceptions.HttpResponseError = type("HttpResponseError", (Exception,), {})
    preloaded_error = exceptions.HttpResponseError
    monkeypatch.setitem(sys.modules, "azure.core", azure_core)
    monkeypatch.setitem(sys.modules, "azure.core.tracing.ext", tracing_ext)
    monkeypatch.setitem(sys.modules, "azure.core.exceptions", exceptions)

    result = bootstrap._colocate_extended_packages(
        MagicMock(), {"azure-core": "1.34.0", "azure-core-plugin": "2.0", "unrelated": "1.0"}
    )

    assert result is None
    command = run.call_args.args[0]
    assert command[-2:] == ["azure-core==1.34.0", "azure-core-plugin==2.0"]
    assert "--no-deps" in command and "--ignore-installed" in command
    assert sys.modules["azure.core"] is azure_core
    assert sys.modules["azure.core.exceptions"].HttpResponseError is preloaded_error
    assert azure_core.__path__ == ["/venv/site/azure/core", "/system/site/azure/core"]
    assert tracing_ext.__path__ == [
        "/venv/site/azure/core/tracing/ext",
        "/system/site/azure/core/tracing/ext",
    ]


def test_colocate_lets_unloaded_subpackages_of_a_preloaded_owner_reach_the_plugin(
    monkeypatch, tmp_path
):
    """Only the owner's top package is loaded: its subpackages, imported
    later, must come from the new copy, or the plugin module next to them in
    it is still not found."""
    import importlib

    import kindling.bootstrap as bootstrap

    owner_files = [
        "kpkg_colocate/core/__init__.py",
        "kpkg_colocate/core/tracing/__init__.py",
        "kpkg_colocate/core/tracing/ext/__init__.py",
    ]
    plugin_files = ["kpkg_colocate/core/tracing/ext/span/__init__.py"]
    system, venv = tmp_path / "system", tmp_path / "venv"
    for root, files in ((system, owner_files), (venv, owner_files + plugin_files)):
        for path in files:
            (root / path).parent.mkdir(parents=True, exist_ok=True)
            (root / path).write_text("")
    system, venv = os.path.realpath(system), os.path.realpath(venv)
    plugin = _fake_dist("kpkg-plugin", "1.0", plugin_files, location=venv)
    before = [plugin, _fake_dist("kpkg-core", "2.0", owner_files, location=system)]
    after = [plugin, _fake_dist("kpkg-core", "2.0", owner_files, location=venv), before[1]]
    run = MagicMock(return_value=_completed())
    monkeypatch.setattr(
        "importlib.metadata.distributions", lambda: iter(after if run.called else before)
    )
    monkeypatch.setattr(bootstrap.subprocess, "run", run)

    monkeypatch.syspath_prepend(system)  # the runtime's copy, loaded first
    for name in [m for m in sys.modules if m.startswith("kpkg_colocate")]:
        monkeypatch.delitem(sys.modules, name)
    owner = importlib.import_module("kpkg_colocate.core")
    try:
        bootstrap._colocate_extended_packages(MagicMock(), {"kpkg-core": "2.0"})
        importlib.invalidate_caches()

        span = importlib.import_module("kpkg_colocate.core.tracing.ext.span")

        assert sys.modules["kpkg_colocate.core"] is owner
        assert span.__file__.startswith(venv)
    finally:
        for name in [m for m in sys.modules if m.startswith("kpkg_colocate")]:
            del sys.modules[name]


def test_colocate_leaves_packages_installed_in_one_place_alone(monkeypatch):
    import kindling.bootstrap as bootstrap

    dists = [
        _fake_dist(
            "azure-core-tracing-opentelemetry",
            "1.0.0b13",
            ["azure/core/tracing/ext/opentelemetry_span/__init__.py"],
        ),
        _fake_dist("azure-core", "1.30.2", ["azure/core/__init__.py"]),
        # unchanged by the install, so not checked even though split
        _fake_dist("old-plugin", "1.0", ["azure/core/x/__init__.py"], location="/other"),
    ]
    monkeypatch.setattr("importlib.metadata.distributions", lambda: iter(dists))
    run = MagicMock()
    monkeypatch.setattr(bootstrap.subprocess, "run", run)

    bootstrap._colocate_extended_packages(
        MagicMock(), {"azure-core": "1.30.2", "old-plugin": "1.0"}
    )

    run.assert_not_called()


def test_report_warns_about_each_replaced_installed_package(monkeypatch):
    import kindling.bootstrap as bootstrap

    monkeypatch.setattr(
        bootstrap,
        "_installed_distribution_versions",
        lambda: {"opentelemetry-api": "1.36.0", "azure-core": "1.34.0", "new-dep": "1.0"},
    )
    logger = MagicMock()

    bootstrap._report_replaced_distributions(
        logger,
        {"opentelemetry-api": ("1.32.1", frozenset()), "azure-core": ("1.34.0", frozenset())},
        "spark-kindling-ext-otel-azure",
    )

    logger.warning.assert_called_once_with(
        "Installing spark-kindling-ext-otel-azure replaced installed opentelemetry-api "
        "1.32.1 with 1.36.0"
    )
