"""Tests for scripts/check_extension_runtime_packages.py (loaded by path)."""

import importlib.util
import sys
from pathlib import Path
from types import SimpleNamespace

_SCRIPT_PATH = (
    Path(__file__).parent.parent.parent / "scripts" / "check_extension_runtime_packages.py"
)
_spec = importlib.util.spec_from_file_location("check_extension_runtime_packages", _SCRIPT_PATH)
check = importlib.util.module_from_spec(_spec)
sys.modules[_spec.name] = check
_spec.loader.exec_module(check)


def test_extension_requirements_leave_out_kindling_distributions(tmp_path):
    (tmp_path / "pyproject.toml").write_text(
        "[project]\n"
        'name = "x"\n'
        'dependencies = ["spark-kindling>=0.14.0,<0.15", "spark_kindling_ext_sdp>=0.3",'
        ' "matplotlib>=3.7.0"]\n'
    )

    assert check.extension_requirements(tmp_path) == ["matplotlib>=3.7.0"]


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
