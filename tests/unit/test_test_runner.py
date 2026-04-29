import os
from unittest.mock import MagicMock

from kindling_cli import test_runner


def test_set_system_coverage_file_uses_platform_and_pid(monkeypatch):
    monkeypatch.delenv("COVERAGE_FILE", raising=False)
    monkeypatch.setattr(test_runner.os, "getpid", lambda: 4242)

    test_runner.set_system_coverage_file("synapse")

    assert os.environ["COVERAGE_FILE"] == ".coverage.system.synapse.4242"


def test_set_system_coverage_file_respects_existing_override(monkeypatch):
    monkeypatch.setenv("COVERAGE_FILE", ".coverage.preconfigured")

    test_runner.set_system_coverage_file("fabric")

    assert os.environ["COVERAGE_FILE"] == ".coverage.preconfigured"


def test_resolve_system_test_workers_local_defaults_to_serial(monkeypatch):
    monkeypatch.delenv("KINDLING_SYSTEM_TEST_WORKERS", raising=False)

    assert test_runner.resolve_workers("synapse", explicit_workers=None, ci=False) == ""


def test_resolve_system_test_workers_local_uses_env_override(monkeypatch):
    monkeypatch.setenv("KINDLING_SYSTEM_TEST_WORKERS", "3")

    assert test_runner.resolve_workers("synapse", explicit_workers=None, ci=False) == "3"


def test_resolve_system_test_workers_ci_uses_platform_defaults(monkeypatch):
    monkeypatch.delenv("KINDLING_SYSTEM_TEST_CI_WORKERS", raising=False)

    assert test_runner.resolve_workers("synapse", explicit_workers=None, ci=True) == "2"
    assert test_runner.resolve_workers("fabric", explicit_workers=None, ci=True) == "4"


def test_ensure_xdist_available_allows_serial_workers(monkeypatch):
    monkeypatch.setattr(test_runner, "find_spec", lambda name: None)

    test_runner.ensure_xdist_available("1")


def test_ensure_xdist_available_raises_clear_error_when_missing(monkeypatch):
    monkeypatch.setattr(test_runner, "find_spec", lambda name: None)

    try:
        test_runner.ensure_xdist_available("2")
    except RuntimeError as exc:
        assert "pytest-xdist is required" in str(exc)
    else:
        raise AssertionError("Expected RuntimeError when xdist is missing")


def test_ensure_xdist_available_accepts_installed_plugin(monkeypatch):
    monkeypatch.setattr(test_runner, "find_spec", lambda name: object())

    test_runner.ensure_xdist_available("2")


def test_run_system_tests_adds_local_workers_when_requested(monkeypatch):
    captured = {}

    def fake_run(args):
        captured["args"] = args
        return MagicMock(returncode=0)

    monkeypatch.setattr(test_runner, "load_dotenv", lambda path: None)
    monkeypatch.setattr(test_runner, "set_system_coverage_file", lambda platform: None)
    monkeypatch.setattr(test_runner.subprocess, "run", fake_run)

    result = test_runner.run_tests(
        test_runner.TestRunOptions(
            suite="system",
            paths=["tests/system/core"],
            platform="synapse",
            test_filter="name_mapper",
            workers="3",
        )
    )

    assert result == 0

    assert captured["args"] == [
        "pytest",
        "tests/system/core",
        "-v",
        "-s",
        "--platform",
        "synapse",
        "-k",
        "name_mapper",
        "-n",
        "3",
    ]


def test_run_system_tests_ci_adds_reports_preflight_and_workers(monkeypatch, tmp_path):
    captured = {}

    def fake_run(args):
        captured["args"] = args
        return MagicMock(returncode=0)

    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(test_runner, "load_dotenv", lambda path: None)
    monkeypatch.setattr(test_runner, "set_system_coverage_file", lambda platform: None)
    monkeypatch.setattr(test_runner, "run_preflight", lambda mode, platform: 0)
    monkeypatch.setattr(test_runner.subprocess, "run", fake_run)

    result = test_runner.run_tests(
        test_runner.TestRunOptions(
            suite="system",
            paths=["tests/system/core"],
            platform="fabric",
            test_filter="name_mapper",
            ci=True,
            preflight="system",
        )
    )

    assert result == 0

    assert captured["args"] == [
        "pytest",
        "tests/system/core",
        "-v",
        "-s",
        "--platform",
        "fabric",
        "-k",
        "name_mapper",
        "--junit-xml=test-results/system-test-results-fabric.xml",
        "--json-report",
        "--json-report-file=test-results/system-test-report-fabric.json",
        "--maxfail=1",
        "-n",
        "4",
    ]


def test_build_pytest_args_supports_explicit_paths_and_passthrough():
    args = test_runner.build_pytest_args(
        test_runner.TestRunOptions(
            suite="unit",
            paths=["pkg/tests/unit", "shared/tests"],
            marker="unit and not slow",
            coverage=("kindling",),
            pytest_args=("--tb=short",),
        )
    )

    assert args == [
        "pytest",
        "pkg/tests/unit",
        "shared/tests",
        "-v",
        "-s",
        "-m",
        "unit and not slow",
        "--cov=kindling",
        "--tb=short",
    ]
