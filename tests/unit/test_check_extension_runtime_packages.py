"""Tests for scripts/check_extension_runtime_packages.py (loaded by path)."""

import importlib.util
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

_SCRIPT_PATH = (
    Path(__file__).parent.parent.parent / "scripts" / "check_extension_runtime_packages.py"
)
_spec = importlib.util.spec_from_file_location("check_extension_runtime_packages", _SCRIPT_PATH)
check = importlib.util.module_from_spec(_spec)
sys.modules[_spec.name] = check
_spec.loader.exec_module(check)


def _write_project(directory, name, dependencies, optional=""):
    directory.mkdir(parents=True, exist_ok=True)
    (directory / "pyproject.toml").write_text(
        f'[project]\nname = "{name}"\ndependencies = {dependencies}\n{optional}'
    )


def test_extension_requirements_follow_kindling_distributions(tmp_path, monkeypatch):
    # pip resolves an extension's Kindling requirements too, so their own
    # requirements (from the repo copies, extras included) are checked.
    _write_project(
        tmp_path,
        "spark-kindling",
        '["injector>=0.20.1"]',
        '[project.optional-dependencies]\nadx = ["azure-kusto-data>=4.0"]\n',
    )
    _write_project(
        tmp_path / "packages" / "extensions" / "sdp",
        "spark-kindling-ext-sdp",
        '["spark-kindling[adx]>=0.1", "pyyaml>=6.0"]',
    )
    extension = tmp_path / "packages" / "extensions" / "x"
    _write_project(
        extension,
        "spark-kindling-ext-x",
        '["spark-kindling>=0.14.0,<0.15", "spark_kindling_ext_sdp>=0.3", "matplotlib>=3.7.0"]',
    )
    monkeypatch.setattr(check, "REPO_ROOT", tmp_path)

    assert sorted(check.extension_requirements(extension)) == [
        "azure-kusto-data>=4.0",
        "injector>=0.20.1",
        "matplotlib>=3.7.0",
        "pyyaml>=6.0",
    ]


def test_extension_requirements_reject_an_unknown_kindling_distribution(tmp_path, monkeypatch):
    _write_project(tmp_path, "spark-kindling", "[]")
    extension = tmp_path / "packages" / "extensions" / "x"
    _write_project(extension, "spark-kindling-ext-x", '["spark-kindling-ext-missing>=1.0"]')
    monkeypatch.setattr(check, "REPO_ROOT", tmp_path)

    with pytest.raises(SystemExit, match="spark-kindling-ext-missing"):
        check.extension_requirements(extension)


def test_resolve_pins_the_runtime_key_packages(monkeypatch):
    seen = {}

    def run(command, **kwargs):
        constraints = command[command.index("-c") + 1]
        seen["constraints"] = Path(constraints).read_text()
        seen["python"] = command[command.index("--python-version") + 1]
        seen["requirements"] = Path(command[-1]).read_text()
        return SimpleNamespace(returncode=0, stdout="", stderr="")

    monkeypatch.setattr(check.subprocess, "run", run)

    check.resolve(["azure-monitor-opentelemetry>=1.6"], "3.12", {"b": "2", "a": "1"})

    assert seen == {
        "constraints": "a==1\nb==2\n",
        "python": "3.12",
        "requirements": "azure-monitor-opentelemetry>=1.6\n",
    }


def test_main_fails_when_a_runtime_needs_a_key_package_replaced(monkeypatch, capsys):
    def resolve(requirements, python_version, pins):
        if pins.get("azure-monitor-opentelemetry") == "1.8.2":
            return SimpleNamespace(returncode=1, stdout="", stderr="No solution found")
        return SimpleNamespace(
            returncode=0,
            stdout="azure-core==1.34.0\nazure-monitor-opentelemetry==1.6.7\n",
            stderr="",
        )

    monkeypatch.setattr(check, "resolve", resolve)

    assert check.main(["--extension", "otel_azure", "--runtime", "dbr17.3"]) == 0
    assert "keeps    azure-core==1.34.0" in capsys.readouterr().out
    assert check.main(["--extension", "otel-azure"]) == 1
    assert "FAIL  kindling_ext_otel_azure on synapse3.5" in capsys.readouterr().out


def test_main_rejects_an_unknown_extension(capsys):
    """A typo must fail rather than check nothing and pass."""
    import pytest

    with pytest.raises(SystemExit) as excinfo:
        check.main(["--extension", "otel_azur"])
    assert excinfo.value.code == 2
    assert "unknown extension(s): kindling_ext_otel_azur" in capsys.readouterr().err
