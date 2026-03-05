"""System tests for EventHubEntityProvider.

Runs a real Spark job on supported platforms and validates EventHubEntityProvider
batch and streaming reads against a live Azure Event Hub.
"""

import base64
import hashlib
import hmac
import json
import os
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid
from pathlib import Path

import pytest

from tests.system.eventhub_test_resource import (
    EventHubResourceResolutionError,
    resolve_eventhub_test_resource,
)


@pytest.fixture
def eventhub_provider_test_app_path():
    """Fixture providing path to Event Hub provider test app."""
    app_path = Path(__file__).parent.parent.parent / "data-apps" / "eventhub-provider-test-app"
    if not app_path.exists():
        pytest.skip(f"EventHub provider test app not found at {app_path}")
    return app_path


@pytest.fixture
def eventhub_test_resource():
    """Resolve Event Hub test resource from env vars or Terraform outputs."""
    iac_dir = os.getenv("EVENTHUB_TEST_IAC_DIR", "iac/azure/eventhub")
    try:
        return resolve_eventhub_test_resource(iac_dir)
    except EventHubResourceResolutionError as exc:
        pytest.skip(f"Event Hub test resource not configured: {exc}")


def _parse_connection_string(connection_string: str) -> dict[str, str]:
    parts: dict[str, str] = {}
    for segment in connection_string.split(";"):
        if not segment or "=" not in segment:
            continue
        key, value = segment.split("=", 1)
        parts[key.strip()] = value.strip()

    required = ["Endpoint", "SharedAccessKeyName", "SharedAccessKey"]
    missing = [key for key in required if not parts.get(key)]
    if missing:
        raise ValueError(f"Connection string missing required fields: {', '.join(missing)}")

    return parts


def _build_sas_token(resource_uri: str, key_name: str, key: str, ttl_seconds: int = 3600) -> str:
    expiry = int(time.time()) + ttl_seconds
    encoded_uri = urllib.parse.quote_plus(resource_uri)
    string_to_sign = f"{encoded_uri}\n{expiry}".encode("utf-8")
    signature = hmac.new(key.encode("utf-8"), string_to_sign, hashlib.sha256).digest()
    encoded_signature = urllib.parse.quote_plus(base64.b64encode(signature))

    return (
        "SharedAccessSignature "
        f"sr={encoded_uri}&sig={encoded_signature}&se={expiry}&"
        f"skn={urllib.parse.quote_plus(key_name)}"
    )


def _publish_event(connection_string: str, eventhub_name: str, payload: dict) -> None:
    parsed = _parse_connection_string(connection_string)
    endpoint = parsed["Endpoint"]
    key_name = parsed["SharedAccessKeyName"]
    key = parsed["SharedAccessKey"]

    namespace_host = endpoint.replace("sb://", "", 1).rstrip("/")
    resource_uri = f"https://{namespace_host}/{eventhub_name}"
    sas_token = _build_sas_token(resource_uri, key_name, key)

    request = urllib.request.Request(
        url=f"{resource_uri}/messages",
        data=json.dumps(payload).encode("utf-8"),
        method="POST",
        headers={
            "Authorization": sas_token,
            "Content-Type": "application/json; charset=utf-8",
        },
    )

    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            status_code = response.getcode()
            if status_code not in (200, 201, 202):
                raise RuntimeError(f"Event publish failed with HTTP status {status_code}")
    except urllib.error.HTTPError as exc:
        response_body = exc.read().decode("utf-8", errors="ignore") if exc.fp else ""
        raise RuntimeError(f"Event publish failed: HTTP {exc.code} {response_body}") from exc


@pytest.mark.system
@pytest.mark.slow
@pytest.mark.azure
class TestEventHubProvider:
    """System tests for EventHubEntityProvider across supported platforms."""

    def test_eventhub_provider_batch_and_stream_read(
        self,
        platform_client,
        app_packager,
        eventhub_provider_test_app_path,
        eventhub_test_resource,
        stdout_validator,
    ):
        api_client, platform_name = platform_client

        test_id = uuid.uuid4().hex
        unique_suffix = test_id[:8]
        app_name = f"eventhub-provider-test-app-{unique_suffix}"
        marker = f"kindling-eventhub-marker-{unique_suffix}"

        initial_payload = {
            "marker": marker,
            "phase": "pre_run",
            "test_id": test_id,
            "published_at": int(time.time()),
        }

        _publish_event(
            connection_string=eventhub_test_resource.connection_string,
            eventhub_name=eventhub_test_resource.eventhub_name,
            payload=initial_payload,
        )

        job_config = {
            "job_name": f"systest-eventhub-provider-{unique_suffix}",
            "app_name": app_name,
            "entry_point": "main.py",
            "test_id": test_id,
            "config_overrides": {
                "kindling": {
                    "eventhub_test": {
                        "connection_string": eventhub_test_resource.connection_string,
                        "eventhub_name": eventhub_test_resource.eventhub_name,
                        "marker": marker,
                    }
                }
            },
        }

        print(f"\n[START] [{platform_name.upper()}] Event Hub provider system test")

        app_files = app_packager.prepare_app_files(str(eventhub_provider_test_app_path))
        api_client.deploy_app(app_name, app_files)
        result = api_client.create_job(job_name=job_config["job_name"], job_config=job_config)
        job_id = result["job_id"]
        print(f"[INFO] App deployed and job created: {job_id}")

        publisher_stop = threading.Event()
        publisher_stats = {"attempted": 0, "succeeded": 0, "failed": 0}

        def publish_during_run():
            deadline = time.time() + 120
            while time.time() < deadline and not publisher_stop.is_set():
                publisher_stats["attempted"] += 1
                payload = {
                    "marker": marker,
                    "phase": "during_run",
                    "test_id": test_id,
                    "published_at": int(time.time()),
                    "attempt": publisher_stats["attempted"],
                }
                try:
                    _publish_event(
                        connection_string=eventhub_test_resource.connection_string,
                        eventhub_name=eventhub_test_resource.eventhub_name,
                        payload=payload,
                    )
                    publisher_stats["succeeded"] += 1
                except Exception:
                    publisher_stats["failed"] += 1
                time.sleep(8)

        publisher_thread = None

        try:
            run_id = api_client.run_job(job_id=job_id, parameters={"test_run": "true"})
            assert run_id is not None, "run_id is None"
            print(f"[INFO] Job started: {run_id}")

            publisher_thread = threading.Thread(target=publish_during_run, daemon=True)
            publisher_thread.start()

            stdout_validator.stream_with_callback(
                job_id=job_id,
                run_id=run_id,
                print_lines=True,
                poll_interval=10.0,
                max_wait=900.0,
            )

            status_result = api_client.get_job_status(run_id=run_id)
            final_status = str(status_result.get("status", "UNKNOWN")).upper()
            assert final_status in ["TERMINATED", "COMPLETED", "SUCCESS"], (
                f"Unexpected final status: {final_status}. " f"Publisher stats: {publisher_stats}"
            )

            expected_tests = [
                "provider_resolution",
                "check_entity_exists",
                "batch_read",
                "stream_read",
                "pipe_to_table",
            ]

            validation_results = stdout_validator.validate_tests(test_id, expected_tests)
            completion_result = stdout_validator.validate_completion(test_id)

            assert publisher_stats["succeeded"] > 0, (
                "No events were published during run, cannot validate stream-read behavior. "
                f"Publisher stats: {publisher_stats}"
            )

            failed_tests = [
                name for name, result in validation_results.items() if not result["passed"]
            ]
            assert not failed_tests, (
                f"Event Hub provider checks failed: {failed_tests}. "
                f"Validation results: {validation_results}"
            )
            assert completion_result["passed"], (
                "Event Hub provider test did not complete successfully: "
                f"{completion_result}. Publisher stats: {publisher_stats}"
            )

            print(f"[OK] Event Hub provider test passed. Publisher stats: {publisher_stats}")

        finally:
            publisher_stop.set()
            if publisher_thread is not None:
                publisher_thread.join(timeout=5)

            if os.environ.get("SKIP_TEST_CLEANUP", "").lower() == "true":
                print(
                    f"[SKIP] Skipping cleanup (SKIP_TEST_CLEANUP=true) - "
                    f"job: {job_id}, app: {app_name}"
                )
            else:
                try:
                    api_client.delete_job(job_id)
                    print(f"[CLEANUP] Deleted job: {job_id}")
                except Exception as exc:
                    print(f"[WARN] Failed to delete job {job_id}: {exc}")

                try:
                    api_client.cleanup_app(app_name)
                    print(f"[CLEANUP] Cleaned up app: {app_name}")
                except Exception as exc:
                    print(f"[WARN] Failed to cleanup app {app_name}: {exc}")
