from types import SimpleNamespace

from tests.system import conftest as system_conftest


def _make_report(nodeid: str, when: str, *, passed=False, failed=False, skipped=False):
    terminal = SimpleNamespace(lines=[])
    terminal.write_line = terminal.lines.append
    config = SimpleNamespace(
        _kindling_system_total_items=3,
        _kindling_system_completed_items=set(),
        _kindling_system_progress_counts={"passed": 0, "skipped": 0, "failed": 0},
        pluginmanager=SimpleNamespace(
            get_plugin=lambda name: terminal if name == "terminalreporter" else None
        ),
    )
    report = SimpleNamespace(
        nodeid=nodeid,
        when=when,
        passed=passed,
        failed=failed,
        skipped=skipped,
        config=config,
    )
    return report, config, terminal


def test_count_completed_reports_for_call_phase():
    report, _, _ = _make_report("test_example", "call", passed=True)
    assert system_conftest._should_count_report_as_completed(report) is True


def test_count_completed_reports_for_setup_skip():
    report, _, _ = _make_report("test_example", "setup", skipped=True)
    assert system_conftest._should_count_report_as_completed(report) is True


def test_ignore_non_terminal_setup_reports():
    report, _, _ = _make_report("test_example", "setup", passed=True)
    assert system_conftest._should_count_report_as_completed(report) is False


def test_progress_reporter_tracks_completed_and_deduplicates():
    report, config, terminal = _make_report("test_example", "call", passed=True)

    system_conftest.pytest_runtest_logreport(report)
    system_conftest.pytest_runtest_logreport(report)

    assert config._kindling_system_progress_counts == {"passed": 1, "skipped": 0, "failed": 0}
    assert terminal.lines == ["Progress: 1/3 completed (passed=1 skipped=0 failed=0)"]


def test_progress_reporter_tracks_setup_skip_as_completed():
    report, config, terminal = _make_report("test_example", "setup", skipped=True)

    system_conftest.pytest_runtest_logreport(report)

    assert config._kindling_system_progress_counts == {"passed": 0, "skipped": 1, "failed": 0}
    assert terminal.lines == ["Progress: 1/3 completed (passed=0 skipped=1 failed=0)"]


def test_progress_reporter_uses_module_config_when_report_lacks_config():
    report, config, terminal = _make_report("test_example", "call", passed=True)
    report = SimpleNamespace(
        nodeid=report.nodeid,
        when=report.when,
        passed=report.passed,
        failed=report.failed,
        skipped=report.skipped,
    )

    previous_config = system_conftest._KINDLING_SYSTEM_CONFIG
    system_conftest._KINDLING_SYSTEM_CONFIG = config
    try:
        system_conftest.pytest_runtest_logreport(report)
    finally:
        system_conftest._KINDLING_SYSTEM_CONFIG = previous_config

    assert config._kindling_system_progress_counts == {"passed": 1, "skipped": 0, "failed": 0}
    assert terminal.lines == ["Progress: 1/3 completed (passed=1 skipped=0 failed=0)"]


def test_progress_reporter_never_prints_zero_total_once_completed():
    report, config, terminal = _make_report("test_example", "call", passed=True)
    config._kindling_system_total_items = 0

    system_conftest.pytest_runtest_logreport(report)

    assert terminal.lines == ["Progress: 1/1 completed (passed=1 skipped=0 failed=0)"]
