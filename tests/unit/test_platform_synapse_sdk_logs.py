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


class _FakeClock:
    def __init__(self):
        self.now = 1000.0

    def time(self):
        return self.now

    def sleep(self, seconds):
        self.now += seconds


def _stream_after_completion(monkeypatch, log_lines, later_chunks=()):
    """Seconds stream_stdout_logs spends after the job is already COMPLETED.

    later_chunks: lines each later poll returns ([] for a poll with nothing new).
    """
    clock = _FakeClock()
    monkeypatch.setattr("kindling_sdk.platform_synapse.time.time", clock.time)
    monkeypatch.setattr("kindling_sdk.platform_synapse.time.sleep", clock.sleep)

    api = SynapseAPI.__new__(SynapseAPI)
    api._diag_root_missing = set()
    api.get_job_status = MagicMock(return_value={"status": "COMPLETED", "app_id": "app_1"})
    chunks = iter([log_lines, *later_chunks])
    api._read_stdout_chunk_synapse = MagicMock(
        side_effect=lambda run_id, app_id, offset: (lambda new: (new, offset + len(new)))(
            next(chunks, [])
        )
    )

    start = clock.now
    lines = api.stream_stdout_logs("job", "42", poll_interval=10.0)
    assert lines == [line for chunk in (log_lines, *later_chunks) for line in chunk]
    return clock.now - start


def test_stream_stops_once_the_bootstrap_reports_the_app_finished(monkeypatch):
    # The fi-static app prints no TEST_ID marker; its log ends with the
    # bootstrap's own completion line.
    log = [
        "🚀 BOOTSTRAP FUNCTION CALLED - 2026-11-02 18:50:46.486",
        "[stderr] INFO KindlingBootstrap: INFO: (KindlingBootstrap) Auto-running app: fi",
        "[stderr] WARN KindlingBootstrap: WARN: (KindlingBootstrap) App 'fi' completed successfully",
    ]

    assert _stream_after_completion(monkeypatch, log) < 60


def test_stream_stops_once_the_bootstrap_reports_the_app_failed(monkeypatch):
    log = [
        "[stderr] ERROR KindlingBootstrap: ERROR: (KindlingBootstrap) App execution failed: boom",
    ]

    assert _stream_after_completion(monkeypatch, log) < 60


def test_stream_waits_for_app_output_after_only_the_batch_payload(monkeypatch):
    # Batch payload lines alone don't show the app's log has materialized yet.
    log = ["stdout: ", "stderr: ", "YARN Diagnostics: "]

    assert _stream_after_completion(monkeypatch, log) >= 600


def test_stream_keeps_waiting_past_other_completed_successfully_lines(monkeypatch):
    # A pip install also "completes successfully"; the app's own output arrives later.
    log = ["[stderr] INFO: (PipManager) Pip command completed successfully in 4.2s"]
    later = [[], [], [], ["WARN: (KindlingBootstrap) App 'fi' completed successfully"]]

    # The assertion inside the helper: every line, the late one included, was read.
    _stream_after_completion(monkeypatch, log, later)
