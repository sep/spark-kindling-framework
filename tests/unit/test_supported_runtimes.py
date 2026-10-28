"""kindling.supported_runtimes: the system-tested runtimes and matching the
running one against them."""

import importlib.util
from pathlib import Path

import pytest
from kindling.supported_runtimes import SUPPORTED_RUNTIMES, match_runtime

SYNAPSE = {
    name: version
    for runtime in SUPPORTED_RUNTIMES
    if runtime.name == "synapse3.5"
    for name, version in runtime.packages.items()
}


def test_a_listed_runtime_is_tested():
    match = match_runtime("synapse", None, SYNAPSE, "3.11.11")
    assert (match.status, match.runtime, match.differences) == ("tested", "synapse3.5", ())
    assert not match.should_warn
    assert match.label() == "tested:synapse3.5"


def test_patch_differences_are_recorded_but_still_tested():
    match = match_runtime("synapse", None, {**SYNAPSE, "azure-core": "1.38.9"}, "3.11.2")
    assert match.status == "tested"
    ((name, tested, installed, major_minor),) = [tuple(vars(d).values()) for d in match.differences]
    assert (name, tested, installed, major_minor) == ("azure-core", "1.38.2", "1.38.9", False)


def test_a_major_minor_difference_is_partial_and_named():
    match = match_runtime("synapse", None, {**SYNAPSE, "pyarrow": "24.0.0"}, "3.11.2")
    assert match.status == "partial" and match.should_warn
    message = match.describe("synapse", None)
    assert "synapse3.5" in message and "pyarrow 24.0.0 (tested 23.0.1)" in message


def test_python_counts_at_major_minor():
    match = match_runtime("synapse", None, SYNAPSE, "3.12.1")
    assert match.status == "partial"
    assert "python 3.12 (tested 3.11)" in match.describe("synapse", None)


def test_vendor_builds_compare_by_base_release():
    installed = {**SYNAPSE, "pyspark": "3.5.1.5.4.20240407"}
    assert match_runtime("synapse", None, installed, "3.11.1").differences == ()


def test_conda_date_versions_and_missing_packages_are_skipped():
    installed = {"msal": "2026.06.01", "pyspark": "3.5.1"}
    assert match_runtime("synapse", None, installed, "3.11.1").differences == ()


def test_databricks_matches_by_runtime_version():
    match = match_runtime("databricks", "17.3.x-scala2.13", {"pyspark": "4.0.0"}, "3.12.3")
    assert match.label() == "tested:dbr17.3"


def test_another_databricks_runtime_is_untested():
    match = match_runtime("databricks", "16.4.x-scala2.12", {"pyspark": "3.5.2"}, "3.12.3")
    assert match.label() == "untested" and match.should_warn
    assert "databricks 16.4.x-scala2.12" in match.describe("databricks", "16.4.x-scala2.12")


def test_databricks_without_a_version_matches_by_packages():
    assert match_runtime("databricks", None, {}, "3.12.3").label() == "tested:dbr17.3"


# Not "platform": tests/system/conftest.py turns a "platform" parameter into
# a marker, which breaks collection in the single-process run.
@pytest.mark.parametrize("platform_name", ["standalone", None, ""])
def test_standalone_and_unknown_are_unchecked(platform_name):
    match = match_runtime(platform_name, None, {"pyspark": "9.9.9"}, "3.8.0")
    assert match.label() == "unchecked" and not match.should_warn


def test_a_platform_with_no_tested_runtime_is_untested():
    assert match_runtime("emr", None, {}, "3.11.0").label() == "untested"


def test_the_ci_check_uses_this_list():
    script = Path(__file__).resolve().parents[2] / "scripts/check_extension_runtime_packages.py"
    spec = importlib.util.spec_from_file_location("check_runtimes_for_list_test", script)
    check = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(check)
    assert check.RUNTIMES == {r.name: (r.python, dict(r.packages)) for r in SUPPORTED_RUNTIMES}
