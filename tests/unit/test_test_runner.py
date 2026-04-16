import os
from unittest.mock import MagicMock

from scripts import test_runner


def test_set_system_coverage_file_uses_platform_and_pid(monkeypatch):
    monkeypatch.delenv("COVERAGE_FILE", raising=False)
    monkeypatch.setattr(test_runner.os, "getpid", lambda: 4242)

    test_runner._set_system_coverage_file("synapse")

    assert os.environ["COVERAGE_FILE"] == ".coverage.system.synapse.4242"


def test_set_system_coverage_file_respects_existing_override(monkeypatch):
    monkeypatch.setenv("COVERAGE_FILE", ".coverage.preconfigured")

    test_runner._set_system_coverage_file("fabric")

    assert os.environ["COVERAGE_FILE"] == ".coverage.preconfigured"


def test_resolve_system_test_workers_local_defaults_to_serial(monkeypatch):
    monkeypatch.delenv("KINDLING_SYSTEM_TEST_WORKERS", raising=False)

    assert test_runner._resolve_system_test_workers("synapse", ci=False) == ""


def test_resolve_system_test_workers_local_uses_env_override(monkeypatch):
    monkeypatch.setenv("KINDLING_SYSTEM_TEST_WORKERS", "3")

    assert test_runner._resolve_system_test_workers("synapse", ci=False) == "3"


def test_resolve_system_test_workers_ci_uses_platform_defaults(monkeypatch):
    monkeypatch.delenv("KINDLING_SYSTEM_TEST_CI_WORKERS", raising=False)

    assert test_runner._resolve_system_test_workers("synapse", ci=True) == "2"
    assert test_runner._resolve_system_test_workers("fabric", ci=True) == "4"


def test_ensure_xdist_available_allows_serial_workers(monkeypatch):
    monkeypatch.setattr(test_runner, "find_spec", lambda name: None)

    test_runner._ensure_xdist_available("1")


def test_ensure_xdist_available_raises_clear_error_when_missing(monkeypatch):
    monkeypatch.setattr(test_runner, "find_spec", lambda name: None)

    try:
        test_runner._ensure_xdist_available("2")
    except SystemExit as exc:
        assert "pytest-xdist is required" in str(exc)
    else:
        raise AssertionError("Expected SystemExit when xdist is missing")


def test_ensure_xdist_available_accepts_installed_plugin(monkeypatch):
    monkeypatch.setattr(test_runner, "find_spec", lambda name: object())

    test_runner._ensure_xdist_available("2")


def test_run_system_tests_adds_local_workers_when_requested(monkeypatch):
    captured = {}

    def fake_run(args):
        captured["args"] = args
        return MagicMock(returncode=0)

    monkeypatch.setattr(test_runner, "_load_dotenv", lambda: None)
    monkeypatch.setattr(test_runner, "_set_system_coverage_file", lambda platform: None)
    monkeypatch.setattr(test_runner.subprocess, "run", fake_run)

    try:
        test_runner.run_system_tests(platform="synapse", test="name_mapper", workers="3")
    except SystemExit as exc:
        assert exc.code == 0

    assert captured["args"] == [
        "pytest",
        "tests/system/core/",
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
    monkeypatch.setattr(test_runner, "_load_dotenv", lambda: None)
    monkeypatch.setattr(test_runner, "_set_system_coverage_file", lambda platform: None)
    monkeypatch.setattr(test_runner, "_run_system_test_preflight", lambda platform: 0)
    monkeypatch.setattr(test_runner.subprocess, "run", fake_run)

    try:
        test_runner.run_system_tests_ci(platform="fabric", test="name_mapper")
    except SystemExit as exc:
        assert exc.code == 0

    assert captured["args"] == [
        "pytest",
        "tests/system/core/",
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
