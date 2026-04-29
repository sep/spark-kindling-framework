import os

import pytest

from tests.system.test_helpers import (
    StdoutStreamValidator,
    apply_env_config_overrides,
    assert_no_fatal_system_test_log_lines,
    find_fatal_system_test_log_lines,
    get_system_test_completion_timeout,
    get_system_test_poll_interval,
    get_system_test_stream_max_wait,
)


def _clear_config_env(monkeypatch):
    for key in list(os.environ):
        if key.startswith("CONFIG__"):
            monkeypatch.delenv(key, raising=False)


def test_apply_env_config_overrides_adds_databricks_uc_bootstrap_paths(monkeypatch):
    _clear_config_env(monkeypatch)
    monkeypatch.setenv("KINDLING_DATABRICKS_SYSTEM_TEST_MODE", "uc")
    monkeypatch.setenv("KINDLING_DATABRICKS_RUNTIME_VOLUME_CATALOG", "kindling")
    monkeypatch.setenv("KINDLING_DATABRICKS_RUNTIME_VOLUME_SCHEMA", "kindling")
    monkeypatch.setenv("KINDLING_DATABRICKS_RUNTIME_TEMP_VOLUME", "artifacts")

    merged = apply_env_config_overrides({"job_name": "job", "test_id": "abc123"}, "databricks")

    kindling = merged["config_overrides"]["kindling"]
    assert kindling["temp_path"] == "/Volumes/kindling/kindling/artifacts/abc123"
    assert kindling["storage"]["table_root"] == "/Volumes/kindling/kindling/artifacts/abc123/tables"
    assert (
        kindling["storage"]["checkpoint_root"]
        == "/Volumes/kindling/kindling/artifacts/abc123/checkpoints"
    )
    assert (
        kindling["databricks"]["volume_staging_root"]
        == "/Volumes/kindling/kindling/artifacts/abc123"
    )
    assert kindling["delta"]["access_mode"] == "catalog"


def test_apply_env_config_overrides_adds_databricks_classic_bootstrap_paths(monkeypatch):
    _clear_config_env(monkeypatch)
    monkeypatch.setenv("KINDLING_DATABRICKS_SYSTEM_TEST_MODE", "classic")

    merged = apply_env_config_overrides({"job_name": "job", "test_id": "abc123"}, "databricks")

    kindling = merged["config_overrides"]["kindling"]
    assert kindling["temp_path"] == "dbfs:/tmp/kindling_system_tests/abc123"
    assert kindling["storage"]["table_root"] == "dbfs:/tmp/kindling_system_tests/abc123/tables"
    assert (
        kindling["storage"]["checkpoint_root"]
        == "dbfs:/tmp/kindling_system_tests/abc123/checkpoints"
    )
    assert kindling["delta"]["access_mode"] == "storage"
    assert "databricks" not in kindling or "volume_staging_root" not in kindling["databricks"]


def test_apply_env_config_overrides_classic_ignores_uc_env_path_defaults(monkeypatch):
    _clear_config_env(monkeypatch)
    monkeypatch.setenv("KINDLING_DATABRICKS_SYSTEM_TEST_MODE", "classic")
    monkeypatch.setenv(
        "CONFIG__platform_databricks__kindling__temp_path",
        "/Volumes/kindling/kindling/artifacts/temp",
    )
    monkeypatch.setenv(
        "CONFIG__platform_databricks__kindling__databricks__volume_staging_root",
        "/Volumes/kindling/kindling/artifacts/temp",
    )
    monkeypatch.setenv(
        "CONFIG__platform_databricks__kindling__storage__table_root",
        "/Volumes/kindling/kindling/artifacts/temp/tables",
    )
    monkeypatch.setenv(
        "CONFIG__platform_databricks__kindling__storage__checkpoint_root",
        "/Volumes/kindling/kindling/artifacts/temp/checkpoints",
    )

    merged = apply_env_config_overrides({"job_name": "job", "test_id": "abc123"}, "databricks")

    kindling = merged["config_overrides"]["kindling"]
    assert kindling["temp_path"] == "dbfs:/tmp/kindling_system_tests/abc123"
    assert kindling["storage"]["table_root"] == "dbfs:/tmp/kindling_system_tests/abc123/tables"
    assert (
        kindling["storage"]["checkpoint_root"]
        == "dbfs:/tmp/kindling_system_tests/abc123/checkpoints"
    )
    assert "databricks" not in kindling or "volume_staging_root" not in kindling["databricks"]


def test_apply_env_config_overrides_keeps_explicit_job_overrides_winning(monkeypatch):
    _clear_config_env(monkeypatch)
    monkeypatch.setenv("KINDLING_DATABRICKS_SYSTEM_TEST_MODE", "uc")
    monkeypatch.setenv("KINDLING_DATABRICKS_RUNTIME_VOLUME_CATALOG", "kindling")
    monkeypatch.setenv("KINDLING_DATABRICKS_RUNTIME_VOLUME_SCHEMA", "kindling")
    monkeypatch.setenv("KINDLING_DATABRICKS_RUNTIME_TEMP_VOLUME", "artifacts")

    merged = apply_env_config_overrides(
        {
            "job_name": "job",
            "test_id": "abc123",
            "config_overrides": {
                "kindling": {
                    "temp_path": "/Volumes/custom/schema/temp",
                    "storage": {"table_root": "/Volumes/custom/schema/temp/custom_tables"},
                }
            },
        },
        "databricks",
    )

    kindling = merged["config_overrides"]["kindling"]
    assert kindling["temp_path"] == "/Volumes/custom/schema/temp"
    assert kindling["storage"]["table_root"] == "/Volumes/custom/schema/temp/custom_tables"


def test_apply_env_config_overrides_always_enables_azure_monitor_for_databricks(monkeypatch):
    _clear_config_env(monkeypatch)
    monkeypatch.setenv("KINDLING_DATABRICKS_SYSTEM_TEST_MODE", "uc")
    monkeypatch.setenv("KINDLING_DATABRICKS_RUNTIME_VOLUME_CATALOG", "kindling")
    monkeypatch.setenv("KINDLING_DATABRICKS_RUNTIME_VOLUME_SCHEMA", "kindling")
    monkeypatch.setenv("KINDLING_DATABRICKS_RUNTIME_TEMP_VOLUME", "artifacts")
    monkeypatch.setenv(
        "APPLICATIONINSIGHTS_CONNECTION_STRING",
        "InstrumentationKey=test-key;IngestionEndpoint=https://example.invalid/",
    )

    merged = apply_env_config_overrides({"job_name": "job", "test_id": "abc123"}, "databricks")

    kindling = merged["config_overrides"]["kindling"]
    assert "extensions" in kindling
    assert any(spec.startswith("kindling-otel-azure==") for spec in kindling["extensions"])
    assert kindling["telemetry"]["azure_monitor"]["enable_logging"] is True
    assert kindling["telemetry"]["azure_monitor"]["enable_tracing"] is True
    assert (
        kindling["telemetry"]["azure_monitor"]["connection_string"]
        == "InstrumentationKey=test-key;IngestionEndpoint=https://example.invalid/"
    )


def test_apply_env_config_overrides_explicitly_enables_azure_monitor_for_non_databricks(
    monkeypatch,
):
    _clear_config_env(monkeypatch)
    monkeypatch.setenv(
        "APPLICATIONINSIGHTS_CONNECTION_STRING",
        "InstrumentationKey=test-key;IngestionEndpoint=https://example.invalid/",
    )

    merged = apply_env_config_overrides(
        {
            "job_name": "job",
            "test_id": "abc123",
            "enable_system_test_azure_monitor": True,
        },
        "fabric",
    )

    kindling = merged["config_overrides"]["kindling"]
    assert "extensions" in kindling
    assert any(spec.startswith("kindling-otel-azure==") for spec in kindling["extensions"])
    assert kindling["telemetry"]["azure_monitor"]["enable_logging"] is True
    assert kindling["telemetry"]["azure_monitor"]["enable_tracing"] is True


def test_find_fatal_system_test_log_lines_detects_extension_install_failures():
    content = "\n".join(
        [
            "INFO normal line",
            "❌ No wheel found matching 'kindling_otel_azure' or 'kindling-otel-azure'",
            "WARN: (KindlingBootstrap) Failed to import extension kindling_otel_azure: No module named 'kindling_otel_azure'",
        ]
    )

    matches = find_fatal_system_test_log_lines(content)

    assert len(matches) == 2
    assert any("No wheel found matching" in line for line in matches)
    assert any("Failed to import extension kindling_otel_azure" in line for line in matches)


def test_find_fatal_system_test_log_lines_parses_structured_log_payloads():
    content = str(
        {
            "id": "abc123",
            "log": [
                "INFO normal line",
                "ERROR: (KindlingBootstrap) Extension wheel not found: kindling-otel-azure==0.3.2",
                "❌ Failed to import extension kindling_otel_azure: No module named 'kindling_otel_azure'",
            ],
        }
    )

    matches = find_fatal_system_test_log_lines(content)

    assert len(matches) == 2
    assert matches[0].startswith("ERROR: (KindlingBootstrap) Extension wheel not found:")
    assert matches[1].startswith("❌ Failed to import extension kindling_otel_azure:")


def test_find_fatal_system_test_log_lines_ignores_non_bootstrap_error_messages():
    content = str(
        {
            "id": "abc123",
            "log": [
                "WARN: (complex-tracing-test) Batch 1 is slow, applying optimization...",
                "ERROR: (complex-tracing-test) Operation failed: Simulated network timeout",
            ],
        }
    )

    assert find_fatal_system_test_log_lines(content) == []


def test_assert_no_fatal_system_test_log_lines_raises_for_extension_failures():
    with pytest.raises(AssertionError, match="Fatal errors found in system test logs"):
        assert_no_fatal_system_test_log_lines(
            "ERROR: (KindlingBootstrap) Failed to install extension kindling-otel-azure==0.3.2"
        )


def test_system_test_timing_helpers_ignore_blank_env_vars(monkeypatch):
    monkeypatch.setenv("KINDLING_SYSTEM_TEST_POLL_INTERVAL", "")
    monkeypatch.setenv("KINDLING_SYSTEM_TEST_STREAM_MAX_WAIT", "")
    monkeypatch.setenv("KINDLING_SYSTEM_TEST_COMPLETION_TIMEOUT", "")

    assert get_system_test_poll_interval(10.0) == 10.0
    assert get_system_test_stream_max_wait(600.0) == 600.0
    assert get_system_test_completion_timeout(600.0) == 600.0


def test_system_test_timing_helpers_fall_back_to_legacy_env_vars(monkeypatch):
    monkeypatch.setenv("KINDLING_SYSTEM_TEST_POLL_INTERVAL", "")
    monkeypatch.setenv("KINDLING_SYSTEM_TEST_STREAM_MAX_WAIT", "")
    monkeypatch.setenv("KINDLING_SYSTEM_TEST_COMPLETION_TIMEOUT", "")
    monkeypatch.setenv("POLL_INTERVAL", "15")
    monkeypatch.setenv("TEST_TIMEOUT", "900")

    assert get_system_test_poll_interval(10.0) == 15.0
    assert get_system_test_stream_max_wait(600.0) == 900.0
    assert get_system_test_completion_timeout(600.0) == 900.0


def test_stdout_validator_validate_completion_fails_when_fatal_log_lines_present():
    validator = StdoutStreamValidator(api_client=None)
    validator.captured_lines = [
        "TEST_ID=abc123 status=COMPLETED result=PASSED",
        "❌ Failed to install extension kindling-otel-azure==0.3.2",
    ]

    result = validator.validate_completion("abc123")

    assert result["passed"] is False
    assert result["status"] == "FAILED"
    assert "Fatal errors found in stdout" in result["message"]
