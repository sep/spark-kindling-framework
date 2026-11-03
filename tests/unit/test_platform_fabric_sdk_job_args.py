import ast
import base64
import json
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

import pytest
from kindling_sdk.platform_fabric import FabricAPI
from kindling_sdk.platform_provider import PlatformAPIRegistry

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
        ("customer's data", "customer's data"),
        ("customer's", "customer's"),
        (["it's", "b"], ["it's", "b"]),
    ],
)
def test_values_reach_the_bootstrap_whole(value, expected):
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


def test_fabric_is_registered_as_the_fabric_platform_api():
    assert PlatformAPIRegistry.get("fabric") is FabricAPI


class _FakeClock:
    def __init__(self):
        self.now = 1000.0

    def time(self):
        return self.now

    def sleep(self, seconds):
        self.now += seconds


def _stream_after_completion(monkeypatch, chunks):
    """Seconds FabricAPI.stream_stdout_logs spends once the job is COMPLETED,
    each poll returning the next chunk of lines ([] when nothing is new)."""
    clock = _FakeClock()
    monkeypatch.setattr("kindling_sdk.platform_fabric.time.time", clock.time)
    monkeypatch.setattr("kindling_sdk.platform_fabric.time.sleep", clock.sleep)

    api = _fabric_api()
    api.get_job_status = MagicMock(return_value={"status": "COMPLETED"})
    api.get_livy_session_info = MagicMock(return_value=None)
    pending = iter(chunks)
    api._read_stdout_chunk = MagicMock(
        side_effect=lambda job_id, run_id, session, offset: (lambda new: (new, offset + len(new)))(
            next(pending, [])
        )
    )

    start = clock.now
    lines = api.stream_stdout_logs("job", "run", poll_interval=10.0)
    assert lines == [line for chunk in chunks for line in chunk]
    return clock.now - start


_APP_END = "WARN: (KindlingBootstrap) App 'streaming-pipes-test-app' completed successfully"


def test_stream_keeps_reading_after_completion_until_the_app_end_arrives(monkeypatch):
    """On Fabric Runtime 2.0 the streaming-pipes app's last 10 s of stdout
    arrived after the job was COMPLETED, and the single final read missed it."""
    chunks = [
        ["INFO: (streaming-pipes-test-app) TEST_ID=t1 test=executor_streaming status=PASSED"],
        [],
        ["INFO: (streaming-pipes-test-app) TEST_ID=t1 test=bronze_data status=PASSED"],
        [_APP_END],
    ]

    assert _stream_after_completion(monkeypatch, chunks) == 30.0


def test_stream_stops_at_once_when_the_app_end_is_already_there(monkeypatch):
    assert _stream_after_completion(monkeypatch, [["TEST_ID=t1 status=COMPLETED", _APP_END]]) == 0


def test_stream_gives_up_after_the_grace_period(monkeypatch):
    seconds = _stream_after_completion(monkeypatch, [["TEST_ID=t1 status=STARTED"]])

    assert seconds == FabricAPI.TERMINAL_LOG_GRACE_SECONDS
