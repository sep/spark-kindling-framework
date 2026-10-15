"""Version bumps: extensions follow Kindling's major.minor."""

import importlib.util
from pathlib import Path

try:
    import tomllib
except ModuleNotFoundError:  # Python 3.10
    import tomli as tomllib

import pytest

_REPO_ROOT = Path(__file__).resolve().parents[2]
_spec = importlib.util.spec_from_file_location(
    "bump_version", _REPO_ROOT / "scripts" / "bump_version.py"
)
bump = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(bump)


@pytest.mark.parametrize(
    "current, bump_type, expected",
    [
        ("0.13.2", "rc", "0.14.0rc1"),
        ("0.14.0rc1", "rc", "0.14.0rc2"),
        ("0.14.0rc2", "release", "0.14.0"),
        ("0.14.0", "patch", "0.14.1"),
        ("0.14.1", "minor", "0.15.0"),
    ],
)
def test_bump_version(current, bump_type, expected):
    assert bump.bump_version(current, bump_type) == expected


@pytest.mark.parametrize(
    "version, starts, kindling_range",
    [
        ("0.14.0rc1", True, ">=0.14.0rc1,<0.15"),
        ("0.14.0", True, ">=0.14.0,<0.15"),
        ("1.2.0", True, ">=1.2.0,<1.3"),
        ("0.14.1", False, None),
    ],
)
def test_minor_line_start_and_range(version, starts, kindling_range):
    assert bump.starts_minor_line(version) is starts
    if kindling_range:
        assert bump.kindling_range(version) == kindling_range


def _extension(root, name, dependencies, init_version=None):
    package = root / "packages" / "extensions" / f"kindling_ext_{name}"
    (package / f"kindling_ext_{name}").mkdir(parents=True)
    (package / "pyproject.toml").write_text(
        f'[project]\nname = "spark-kindling-ext-{name}"\nversion = "0.2.0"\n'
        f"dependencies = {dependencies}\n\n"
        '[project.optional-dependencies]\nspark_3_x = ["pyspark>=3.4.0,<4.0.0"]\n\n'
        '[dependency-groups]\ndev = ["pytest>=7.0.0"]\n',
        encoding="utf-8",
    )
    if init_version:
        (package / f"kindling_ext_{name}" / "__init__.py").write_text(
            f'__version__ = "{init_version}"\n', encoding="utf-8"
        )
    return package / "pyproject.toml"


def test_align_extensions_at_minor_start(tmp_path):
    empty = _extension(tmp_path, "cosmos", "[]", init_version="0.2.0")
    ranged = _extension(
        tmp_path,
        "databricks",
        '[\n    "spark-kindling>=0.12.39",\n    # builds on the SDP engine\n'
        '    "spark-kindling-ext-sdp>=0.3.2",\n    "requests>=2",\n]',
    )
    inline = _extension(tmp_path, "viz", '["matplotlib>=3.7.0"]')

    changed = bump.align_extensions("0.14.0rc1", tmp_path)

    assert empty in changed and ranged in changed and inline in changed
    data = {path: tomllib.loads(path.read_text()) for path in (empty, ranged, inline)}
    assert {d["project"]["version"] for d in data.values()} == {"0.14.0rc1"}
    assert data[empty]["project"]["dependencies"] == ["spark-kindling>=0.14.0rc1,<0.15"]
    assert data[ranged]["project"]["dependencies"] == [
        "spark-kindling>=0.14.0rc1,<0.15",
        "spark-kindling-ext-sdp>=0.14.0rc1,<0.15",
        "requests>=2",
    ]
    assert "# builds on the SDP engine" in ranged.read_text()
    assert data[inline]["project"]["dependencies"] == [
        "spark-kindling>=0.14.0rc1,<0.15",
        "matplotlib>=3.7.0",
    ]
    # Other arrays are untouched.
    assert data[empty]["project"]["optional-dependencies"]["spark_3_x"] == ["pyspark>=3.4.0,<4.0.0"]
    assert data[empty]["dependency-groups"]["dev"] == ["pytest>=7.0.0"]
    init = empty.parent / "kindling_ext_cosmos" / "__init__.py"
    assert init.read_text() == '__version__ = "0.14.0rc1"\n'


def test_align_extensions_leaves_patch_releases_alone(tmp_path):
    path = _extension(tmp_path, "cosmos", "[]")
    before = path.read_text()

    assert bump.align_extensions("0.14.1", tmp_path) == []
    assert path.read_text() == before


def test_real_extensions_align_cleanly(tmp_path):
    """Every extension in the repo parses and aligns (run on a copy)."""
    import shutil

    shutil.copytree(_REPO_ROOT / "packages" / "extensions", tmp_path / "packages" / "extensions")
    bump.align_extensions("0.14.0", tmp_path)

    for pyproject in (tmp_path / "packages" / "extensions").glob("*/pyproject.toml"):
        project = tomllib.loads(pyproject.read_text())["project"]
        assert project["version"] == "0.14.0", pyproject
        assert "spark-kindling>=0.14.0,<0.15" in project["dependencies"], pyproject
