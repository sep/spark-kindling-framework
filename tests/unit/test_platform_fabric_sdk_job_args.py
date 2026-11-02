import ast
import base64
import json
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

import pytest
from kindling_sdk.platform_fabric import FabricAPI

_BOOTSTRAP = Path(__file__).resolve().parents[2] / "runtime" / "scripts" / "kindling_bootstrap.py"


def _bootstrap_parse_value():
    module = ast.parse(_BOOTSTRAP.read_text())
    (node,) = [
        n
        for n in module.body
        if isinstance(n, ast.FunctionDef) and n.name == "_parse_cli_config_value"
    ]
    namespace = {"json": json, "Any": Any}
    exec(compile(ast.Module(body=[node], type_ignores=[]), str(_BOOTSTRAP), "exec"), namespace)
    return namespace["_parse_cli_config_value"]


def _fabric_api():
    api = FabricAPI.__new__(FabricAPI)
    api.workspace_id = "workspace-123"
    api.lakehouse_id = "lakehouse-123"
    api.storage_account = "mystorageaccount"
    api.container = "artifacts"
    api.base_path = None
    api.base_url = "https://api.fabric.microsoft.com/v1"
    api._make_request = MagicMock(
        return_value=MagicMock(json=lambda: {"id": "job-123", "displayName": "job"})
    )
    return api


def _config_the_bootstrap_receives(config_overrides):
    api = _fabric_api()
    api.create_job("job", {"config_overrides": config_overrides})
    payload = api._make_request.call_args.kwargs["json"]
    definition = json.loads(base64.b64decode(payload["definition"]["parts"][0]["payload"]))

    parse_value = _bootstrap_parse_value()
    config = {}
    # What Fabric does to commandLineArguments (an argv probe job showed it:
    # split on whitespace, quotes kept), then the bootstrap's own handling.
    for arg in definition["commandLineArguments"].split():
        if len(arg) >= 2 and arg[0] == arg[-1] and arg[0] in ("'", '"'):
            arg = arg[1:-1]
        if arg.startswith("config:") and "=" in arg:
            key, value = arg[len("config:") :].split("=", 1)
            config[key] = parse_value(value)
    return config


@pytest.mark.parametrize(
    "value, expected",
    [
        (["a b", "c"], ["a b", "c"]),
        ("@secret my-secret", "@secret my-secret"),
        ('{"k": "v w"}', {"k": "v w"}),
        ("  padded value ", "  padded value "),
    ],
)
def test_values_with_spaces_reach_the_bootstrap_whole(value, expected):
    assert _config_the_bootstrap_receives({"some_setting": value})["some_setting"] == expected


def test_values_without_spaces_are_sent_unchanged():
    api = _fabric_api()
    api.create_job("job", {"config_overrides": {"some_setting": "6e206823", "flag": True}})
    payload = api._make_request.call_args.kwargs["json"]
    args = json.loads(base64.b64decode(payload["definition"]["parts"][0]["payload"]))[
        "commandLineArguments"
    ]

    assert "config:some_setting=6e206823" in args.split()
    assert "config:flag=true" in args.split()


def test_spark_configs_keep_their_dotted_keys_whole():
    config = _config_the_bootstrap_receives(
        {"spark_configs": {"spark.synapse.vegas.useCache": "false", "spark.sql.x": "a b"}}
    )

    assert config["spark_configs"] == {
        "spark.synapse.vegas.useCache": "false",
        "spark.sql.x": "a b",
    }
