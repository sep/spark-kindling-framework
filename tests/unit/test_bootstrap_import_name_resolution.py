"""Import-name resolution and rename-alias behavior for extension loading.

Distribution names are not import names (spark-kindling-ext-sdp installs
kindling_ext_sdp), and the kindling-ext-* -> spark-kindling-ext-* rename
means configs and lake wheels may carry either naming generation. These
tests pin the resolution rules bootstrap uses for both.
"""

import importlib.metadata as importlib_metadata
import sys
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from kindling.bootstrap import (
    dist_name_candidates,
    find_installed_distribution,
    import_distribution_modules,
    install_bootstrap_dependencies,
    normalize_dist_name,
    resolve_import_names,
)


def _fake_distribution(top_level=None, files=None, name="x", version="1.0.0"):
    dist = SimpleNamespace(metadata={"Name": name}, version=version)
    dist.read_text = lambda filename: top_level if filename == "top_level.txt" else None
    dist.files = files
    return dist


def test_normalize_dist_name_applies_pep503_rules():
    assert normalize_dist_name("Spark_Kindling.Ext--SDP") == "spark-kindling-ext-sdp"


def test_dist_name_candidates_maps_between_naming_generations():
    assert dist_name_candidates("spark-kindling-ext-sdp") == [
        "spark-kindling-ext-sdp",
        "kindling-ext-sdp",
    ]
    assert dist_name_candidates("kindling-ext-temporal") == [
        "kindling-ext-temporal",
        "spark-kindling-ext-temporal",
    ]
    assert dist_name_candidates("spark-kindling") == ["spark-kindling"]


def test_resolve_import_names_prefers_top_level_metadata():
    dist = _fake_distribution(top_level="kindling_ext_sdp\n")

    with patch("importlib.metadata.distribution", return_value=dist):
        names = resolve_import_names("spark-kindling-ext-sdp")

    assert names[0] == "kindling_ext_sdp"


def test_resolve_import_names_falls_back_to_file_listing():
    files = [
        SimpleNamespace(parts=("spark_kindling_ext_sdp-0.3.1.dist-info", "METADATA")),
        SimpleNamespace(parts=("kindling_ext_sdp", "__init__.py")),
        SimpleNamespace(parts=("kindling_ext_sdp", "declaration_plan.py")),
    ]
    dist = _fake_distribution(top_level=None, files=files)

    with patch("importlib.metadata.distribution", return_value=dist):
        names = resolve_import_names("spark-kindling-ext-sdp")

    assert names[0] == "kindling_ext_sdp"


def test_resolve_import_names_heuristic_covers_both_aliases_when_not_installed():
    with patch(
        "importlib.metadata.distribution",
        side_effect=importlib_metadata.PackageNotFoundError,
    ):
        names = resolve_import_names("spark-kindling-ext-temporal")

    assert "spark_kindling_ext_temporal" in names
    assert "kindling_ext_temporal" in names


def test_find_installed_distribution_matches_legacy_alias():
    def fake_distribution(name):
        if name == "kindling-ext-temporal":
            return _fake_distribution(name="kindling-ext-temporal", version="0.2.4")
        raise importlib_metadata.PackageNotFoundError(name)

    with patch("importlib.metadata.distribution", side_effect=fake_distribution):
        name, version = find_installed_distribution("spark-kindling-ext-temporal")

    assert name == "kindling-ext-temporal"
    assert version == "0.2.4"


def test_import_distribution_modules_imports_metadata_resolved_module():
    logger = MagicMock()
    # A module name no other test imports, so sys.modules can't short-circuit.
    dist = _fake_distribution(top_level="fake_ext_module\n")

    with (
        patch("importlib.metadata.distribution", return_value=dist),
        patch("kindling.bootstrap.importlib.util.find_spec", return_value=object()),
        patch("kindling.bootstrap.importlib.import_module", return_value=object()) as import_mod,
    ):
        assert import_distribution_modules("spark-kindling-ext-fake", logger) is True

    assert import_mod.call_args_list[0].args[0] == "fake_ext_module"


def test_import_distribution_modules_false_when_nothing_importable():
    logger = MagicMock()

    with (
        patch(
            "importlib.metadata.distribution",
            side_effect=importlib_metadata.PackageNotFoundError,
        ),
        patch("kindling.bootstrap.importlib.util.find_spec", return_value=None),
    ):
        assert import_distribution_modules("spark-kindling-ext-nope", logger) is False


def _run_install(extensions, wheel_filenames, find_spec=None):
    """Drive install_bootstrap_dependencies against a fake lake listing."""
    logger = MagicMock()

    class DBUtils:
        def __init__(self):
            self.fs = MagicMock()

    storage_utils = DBUtils()
    storage_utils.fs.ls.return_value = [
        SimpleNamespace(path=f"abfss://artifacts@acct/path/packages/{name}")
        for name in wheel_filenames
    ]

    with (
        patch("kindling.bootstrap._get_storage_utils", return_value=storage_utils),
        patch("kindling.bootstrap.importlib.util.find_spec", return_value=find_spec),
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
                "extensions": extensions,
                "temp_path": "dbfs:/tmp/kindling_extensions",
            },
            artifacts_storage_path="abfss://artifacts@acct/path",
        )

    return storage_utils


def test_new_name_spec_finds_legacy_wheel_in_lake():
    storage_utils = _run_install(
        ["spark-kindling-ext-otel-azure==0.4.0"],
        ["kindling_ext_otel_azure-0.4.0-py3-none-any.whl"],
    )

    copied_path = storage_utils.fs.cp.call_args.args[1]
    assert copied_path.endswith("/extensions/kindling_ext_otel_azure-0.4.0-py3-none-any.whl")


def test_legacy_name_spec_finds_renamed_wheel_in_lake():
    storage_utils = _run_install(
        ["kindling-ext-otel-azure==0.4.0"],
        ["spark_kindling_ext_otel_azure-0.4.0-py3-none-any.whl"],
    )

    copied_path = storage_utils.fs.cp.call_args.args[1]
    assert copied_path.endswith("/extensions/spark_kindling_ext_otel_azure-0.4.0-py3-none-any.whl")


def test_install_skipped_when_distribution_installed_under_legacy_name():
    logger = MagicMock()
    storage_utils = MagicMock()
    storage_utils.fs = MagicMock()

    def fake_version(name):
        if name == "kindling-ext-otel-azure":
            return "0.4.0"
        raise importlib_metadata.PackageNotFoundError(name)

    with (
        patch("kindling.bootstrap._get_storage_utils", return_value=storage_utils),
        patch("importlib.metadata.version", side_effect=fake_version),
        patch("kindling.bootstrap.importlib.util.find_spec", return_value=object()),
        patch("kindling.bootstrap.importlib.import_module", return_value=object()),
    ):
        install_bootstrap_dependencies(
            logger,
            {
                "required_packages": [],
                "extensions": ["spark-kindling-ext-otel-azure>=0.3.0"],
            },
            artifacts_storage_path="abfss://artifacts@acct/path",
        )

    storage_utils.fs.ls.assert_not_called()
