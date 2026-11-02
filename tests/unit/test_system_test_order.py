from pathlib import Path
from types import SimpleNamespace

from tests.system import conftest as system_conftest

SYSTEM_CORE = Path(__file__).resolve().parents[1] / "system" / "core"


def _item(module):
    return SimpleNamespace(
        path=SYSTEM_CORE / f"{module}.py", keywords={}, add_marker=lambda marker: None
    )


def test_every_ranked_module_exists():
    # A renamed module would silently fall back to running last.
    modules = {path.stem for path in SYSTEM_CORE.glob("test_*.py")}

    assert set(system_conftest._LONGEST_FIRST) <= modules


def test_collection_starts_the_longest_modules_first(monkeypatch):
    monkeypatch.delenv("TEST_PLATFORM", raising=False)
    config = SimpleNamespace(getoption=lambda *args, **kwargs: None)
    collected = [
        "test_complex_tracing",
        "test_view_and_csv_roundtrip",
        "test_config_secrets",
        "test_streaming_pipes_orchestrator",
        "test_streaming_system",
        "test_streaming_system",
    ]
    items = [_item(module) for module in collected]

    system_conftest.pytest_collection_modifyitems(config, items)

    assert [item.path.stem for item in items] == [
        "test_streaming_pipes_orchestrator",
        "test_streaming_system",
        "test_streaming_system",
        "test_view_and_csv_roundtrip",
        # Unlisted modules keep their collection order.
        "test_complex_tracing",
        "test_config_secrets",
    ]
