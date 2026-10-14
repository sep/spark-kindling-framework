"""The Spark 4 lane's plumbing: the venv requirements filter and the
known-failures list (tests/spark4_known_failures.txt).

Both run in every environment (no Spark 4 needed): the filter decides which
uv.lock pins reach .venv-spark4, and a malformed known-failures entry must
fail here, in the default suites, rather than only inside the Spark 4 lane.
"""

import importlib.util
from pathlib import Path

import pytest

from tests.conftest import (
    SPARK4_KNOWN_FAILURES_FILE,
    _match_spark4_known_failures,
    load_spark4_known_failures,
)

_SCRIPT_PATH = Path(__file__).resolve().parents[2] / "scripts" / "ensure_spark4_runtime.py"
_spec = importlib.util.spec_from_file_location("ensure_spark4_runtime", _SCRIPT_PATH)
ensure_spark4_runtime = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(ensure_spark4_runtime)

EXPORTED = """\
-e .
-e ./packages/kindling_cli
delta-spark==3.3.2
numpy==2.3.4 ; python_full_version >= '3.11'
py4j==0.10.9.9
pyspark==3.5.8
Pyspark_Extras==1.0
pandas==2.3.3
"""


def test_filter_drops_only_the_spark_35_stack():
    kept = ensure_spark4_runtime.filter_locked_requirements(EXPORTED).splitlines()
    assert kept == [
        "-e .",
        "-e ./packages/kindling_cli",
        "numpy==2.3.4 ; python_full_version >= '3.11'",
        "Pyspark_Extras==1.0",
        "pandas==2.3.3",
    ]


def test_spark4_pins_are_exact_and_cover_the_replaced_packages():
    names = set()
    for requirement in ensure_spark4_runtime.SPARK4_REQUIREMENTS:
        name, sep, version = requirement.partition("==")
        assert sep and version, f"{requirement!r} must be an exact pin"
        names.add(name.split("[")[0])
    assert {"pyspark", "delta-spark"} <= names


def test_known_failures_file_parses_and_has_unique_entries():
    entries = load_spark4_known_failures()
    nodeids = [nodeid for _modes, nodeid, _reason in entries]
    assert len(nodeids) == len(set(nodeids))
    for modes, nodeid, reason in entries:
        assert modes <= {"classic", "connect"} and modes
        assert nodeid.startswith("tests/") and reason
        assert Path(SPARK4_KNOWN_FAILURES_FILE.parent.parent, nodeid.split("::")[0]).is_file()


@pytest.mark.parametrize(
    "line",
    [
        "sometimes tests/unit/test_x.py::test_y | BUG: bad mode",
        "all tests/unit/test_x.py::test_y",
        "all | BUG: no nodeid",
    ],
)
def test_known_failures_rejects_malformed_lines(tmp_path, line):
    path = tmp_path / "known.txt"
    path.write_text(f"# comment\n\n{line}\n")
    with pytest.raises(ValueError, match="known.txt:3"):
        load_spark4_known_failures(path)


def test_known_failure_matching_covers_prefixes_and_reports_stale_entries():
    entries = [
        ({"classic"}, "tests/unit/test_a.py::test_one", "BUG: one"),
        ({"classic"}, "tests/unit/test_b.py::TestB", "HARNESS: class"),
        ({"classic"}, "tests/unit/test_gone.py::test_renamed", "BUG: stale"),
    ]
    nodeids = [
        "tests/unit/test_a.py::test_one",
        "tests/unit/test_a.py::test_one_more",
        "tests/unit/test_b.py::TestB::test_x[1]",
    ]
    matches, unmatched = _match_spark4_known_failures(nodeids, entries)
    assert matches == {
        "tests/unit/test_a.py::test_one": "BUG: one",
        "tests/unit/test_b.py::TestB::test_x[1]": "HARNESS: class",
    }
    assert unmatched == ["tests/unit/test_gone.py::test_renamed"]
