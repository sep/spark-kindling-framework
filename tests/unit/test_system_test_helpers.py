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
        kindling["storage"]["checkpoint_root"]
        == "/Volumes/kindling/kindling/artifacts/checkpoints"
    )
    assert kindling["databricks"]["volume_staging_root"] == "/Volumes/kindling/kindling/artifacts"
    assert kindling["delta"]["tablerefmode"] == "forName"


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
    assert kindling["delta"]["tablerefmode"] == "forPath"
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
