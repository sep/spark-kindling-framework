import ast
from pathlib import Path


def _load_is_auto_cluster_columns():
    app_path = (
        Path(__file__).resolve().parents[1] / "data-apps" / "streaming-pipes-test-app" / "main.py"
    )
    module_ast = ast.parse(app_path.read_text(), filename=str(app_path))

    for node in ast.walk(module_ast):
        if isinstance(node, ast.FunctionDef) and node.name == "_is_auto_cluster_columns":
            isolated_module = ast.Module(body=[node], type_ignores=[])
            namespace = {}
            exec(compile(isolated_module, str(app_path), "exec"), namespace)
            return namespace["_is_auto_cluster_columns"]

    raise AssertionError("_is_auto_cluster_columns not found in streaming-pipes test app")


def test_is_auto_cluster_columns_detects_auto_string():
    is_auto_cluster_columns = _load_is_auto_cluster_columns()

    assert is_auto_cluster_columns("auto") is True
    assert is_auto_cluster_columns(["auto"]) is True
    assert is_auto_cluster_columns(["id"]) is False
