import ast
import time
from pathlib import Path
from unittest.mock import MagicMock


def _load_app_function(name, **app_globals):
    """Compile one nested function from the app, with the app-level names it uses."""
    app_path = (
        Path(__file__).resolve().parents[1] / "data-apps" / "streaming-pipes-test-app" / "app.py"
    )
    module_ast = ast.parse(app_path.read_text(), filename=str(app_path))

    for node in ast.walk(module_ast):
        if isinstance(node, ast.FunctionDef) and node.name == name:
            isolated_module = ast.Module(body=[node], type_ignores=[])
            namespace = dict(app_globals)
            exec(compile(isolated_module, str(app_path), "exec"), namespace)
            return namespace[name]

    raise AssertionError(f"{name} not found in streaming-pipes test app")


def _load_is_auto_cluster_columns():
    return _load_app_function("_is_auto_cluster_columns")


def test_is_auto_cluster_columns_detects_auto_string():
    is_auto_cluster_columns = _load_is_auto_cluster_columns()

    assert is_auto_cluster_columns("auto") is True
    assert is_auto_cluster_columns(["auto"]) is True
    assert is_auto_cluster_columns(["id"]) is False


def _query(active=True):
    query = MagicMock(isActive=active)
    # One micro-batch's worth: what made the old progress check never pass.
    query.lastProgress = {"batchId": 4, "numInputRows": 20}
    return query


def _load_wait(counts_by_entity):
    """counts_by_entity: entity -> row counts returned by successive reads."""
    reads = {entity: iter(counts) for entity, counts in counts_by_entity.items()}
    entity_provider = MagicMock()
    entity_provider.read_entity.side_effect = lambda entity: MagicMock(
        count=MagicMock(return_value=next(reads[entity]))
    )
    return _load_app_function(
        "wait_for_query_progress",
        time=time,
        logger=MagicMock(),
        test_id="t",
        entity_provider=entity_provider,
    )


class TestWaitForQueryProgress:
    def test_returns_once_every_output_holds_the_expected_rows(self):
        wait = _load_wait({"silver": [40, 100, 100], "gold": [100, 100]})

        start = time.time()
        assert wait(
            [_query(), _query()], ["silver", "gold"], 100, timeout_seconds=30, poll_interval=0
        )
        assert time.time() - start < 5, "must not wait out the timeout"

    def test_times_out_while_an_output_is_short(self):
        wait = _load_wait({"silver": [100] * 10**6, "gold": [60] * 10**6})

        assert not wait([_query()], ["silver", "gold"], 100, timeout_seconds=0.2, poll_interval=0)

    def test_a_dead_query_fails_immediately(self):
        wait = _load_wait({"silver": [], "gold": []})
        dead = _query(active=False)
        dead.exception.return_value = RuntimeError("boom")

        assert not wait([_query(), dead], ["silver", "gold"], 100, timeout_seconds=30)
