from unittest.mock import MagicMock

import requests
from kindling_sdk.platform_synapse import SynapseAPI


def test_get_job_logs_keeps_complex_tracing_app_lines(monkeypatch):
    api = SynapseAPI.__new__(SynapseAPI)
    api.base_url = "https://example.dev.azuresynapse.net"
    api.spark_pool_name = "sparkpool"
    api.storage_account = None
    api.container = None
    api._storage_client = None
    api.workspace_name = "example"
    api.credential = None

    api._make_request = MagicMock(
        return_value=MagicMock(
            json=lambda: {
                "appId": "application_123",
                "log": [],
            }
        )
    )
    api._get_access_token = MagicMock(return_value="token")

    responses = iter(
        [
            MagicMock(
                status_code=200,
                text="INFO: (complex-tracing-test) Complex Tracing System Test\n"
                "INFO: (complex-tracing-test) Pipeline pipeline-1 completed successfully\n"
                "INFO: (complex-tracing-test)   Pipelines completed: 3\n"
                "INFO: (complex-tracing-test) 🎉 Complex tracing test completed successfully!\n",
            ),
            MagicMock(status_code=200, text=""),
        ]
    )

    monkeypatch.setattr(requests, "get", lambda *args, **kwargs: next(responses))

    result = api.get_job_logs("123")

    assert result["source"] == "sparkhistory_driverlog"
    assert any("Complex Tracing System Test" in line for line in result["log"])
    assert any("Pipelines completed: 3" in line for line in result["log"])
    assert any("completed successfully" in line for line in result["log"])


def test_make_request_retries_on_synapse_throttling(monkeypatch):
    api = SynapseAPI.__new__(SynapseAPI)
    api._last_request_time = 0
    api._min_request_interval = 0
    api._get_access_token = MagicMock(return_value="token")

    throttled = MagicMock(
        status_code=429,
        headers={"Retry-After": "1"},
        json=lambda: {
            "error": {
                "message": "Your request has hit layered throttling rate-limit of 2 requests per 1 second(s)."
            }
        },
        text="throttled",
    )
    success = MagicMock(status_code=200, headers={}, json=lambda: {"ok": True}, text="ok")

    responses = iter([throttled, success])
    request_mock = MagicMock(side_effect=lambda *args, **kwargs: next(responses))
    sleep_mock = MagicMock()

    monkeypatch.setattr(requests, "request", request_mock)
    monkeypatch.setattr("kindling_sdk.platform_synapse.time.sleep", sleep_mock)

    response = api._make_request("POST", "https://example.invalid/jobs", json={"name": "job"})

    assert response is success
    assert request_mock.call_count == 2
    sleep_mock.assert_called_with(1.0)
