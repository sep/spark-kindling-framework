import os

from tests.system.test_helpers import apply_env_config_overrides


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
    assert kindling["temp_path"] == "/Volumes/kindling/kindling/artifacts"
    assert kindling["storage"]["table_root"] == "/Volumes/kindling/kindling/artifacts/tables"
    assert (
        kindling["storage"]["checkpoint_root"] == "/Volumes/kindling/kindling/artifacts/checkpoints"
    )
    assert kindling["databricks"]["volume_staging_root"] == "/Volumes/kindling/kindling/artifacts"
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
