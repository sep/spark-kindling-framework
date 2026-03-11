import os

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
