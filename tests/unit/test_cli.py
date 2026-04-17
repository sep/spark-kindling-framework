import base64
import json
from pathlib import Path

import yaml
from click.testing import CliRunner
from kindling_cli.cli import (
    _build_fabric_notebook_payload,
    _generate_bootstrap_notebook,
    _generate_sample_notebook,
    _render_environment_bootstrap_source,
    _render_starter_notebook_source,
    cli,
)


def test_config_init_writes_settings_file():
    runner = CliRunner()
    with runner.isolated_filesystem():
        result = runner.invoke(cli, ["config", "init", "--name", "demo-app"])

        assert result.exit_code == 0
        settings_path = Path("settings.yaml")
        assert settings_path.exists()
        content = settings_path.read_text(encoding="utf-8")
        assert "name: demo-app" in content
        assert "kindling:" in content


def test_config_init_refuses_to_overwrite_without_force():
    runner = CliRunner()
    with runner.isolated_filesystem():
        settings_path = Path("settings.yaml")
        settings_path.write_text("original", encoding="utf-8")

        result = runner.invoke(cli, ["config", "init"])

        assert result.exit_code != 0
        assert "Refusing to overwrite existing file" in result.output
        assert settings_path.read_text(encoding="utf-8") == "original"


def test_config_init_overwrites_with_force():
    runner = CliRunner()
    with runner.isolated_filesystem():
        settings_path = Path("settings.yaml")
        settings_path.write_text("original", encoding="utf-8")

        result = runner.invoke(cli, ["config", "init", "--force", "--name", "forced-app"])

        assert result.exit_code == 0
        content = settings_path.read_text(encoding="utf-8")
        assert "name: forced-app" in content
        assert "kindling:" in content


def test_env_check_passes_for_generated_settings_file():
    runner = CliRunner()
    with runner.isolated_filesystem():
        init_result = runner.invoke(cli, ["config", "init"])
        assert init_result.exit_code == 0

        result = runner.invoke(cli, ["env", "check"])

        assert result.exit_code == 0
        assert "Environment check passed." in result.output


# ---------------------------------------------------------------------------
# config set
# ---------------------------------------------------------------------------


class TestConfigSetBasic:
    def test_creates_file_when_missing(self):
        runner = CliRunner()
        with runner.isolated_filesystem():
            result = runner.invoke(
                cli,
                [
                    "config",
                    "set",
                    "kindling.telemetry.logging.level",
                    "DEBUG",
                ],
            )
            assert result.exit_code == 0
            data = yaml.safe_load(Path("settings.yaml").read_text())
            assert data["kindling"]["telemetry"]["logging"]["level"] == "DEBUG"

    def test_updates_existing_file_preserves_other_keys(self):
        runner = CliRunner()
        with runner.isolated_filesystem():
            runner.invoke(cli, ["config", "init", "--name", "test-app"])
            result = runner.invoke(
                cli,
                [
                    "config",
                    "set",
                    "kindling.telemetry.logging.level",
                    "ERROR",
                ],
            )
            assert result.exit_code == 0
            data = yaml.safe_load(Path("settings.yaml").read_text())
            assert data["kindling"]["telemetry"]["logging"]["level"] == "ERROR"
            assert data["name"] == "test-app"

    def test_adds_new_nested_key(self):
        runner = CliRunner()
        with runner.isolated_filesystem():
            result = runner.invoke(
                cli,
                [
                    "config",
                    "set",
                    "kindling.new_section.nested.key",
                    "value",
                ],
            )
            assert result.exit_code == 0
            data = yaml.safe_load(Path("settings.yaml").read_text())
            assert data["kindling"]["new_section"]["nested"]["key"] == "value"

    def test_output_shows_confirmation(self):
        runner = CliRunner()
        with runner.isolated_filesystem():
            result = runner.invoke(
                cli,
                [
                    "config",
                    "set",
                    "kindling.telemetry.logging.level",
                    "WARN",
                ],
            )
            assert result.exit_code == 0
            assert "kindling.telemetry.logging.level" in result.output
            assert "settings.yaml" in result.output


class TestConfigSetValueCoercion:
    def test_boolean_true(self):
        runner = CliRunner()
        with runner.isolated_filesystem():
            runner.invoke(cli, ["config", "set", "kindling.flag", "true"])
            data = yaml.safe_load(Path("settings.yaml").read_text())
            assert data["kindling"]["flag"] is True

    def test_boolean_false(self):
        runner = CliRunner()
        with runner.isolated_filesystem():
            runner.invoke(cli, ["config", "set", "kindling.flag", "false"])
            data = yaml.safe_load(Path("settings.yaml").read_text())
            assert data["kindling"]["flag"] is False

    def test_integer(self):
        runner = CliRunner()
        with runner.isolated_filesystem():
            runner.invoke(cli, ["config", "set", "kindling.retries", "5"])
            data = yaml.safe_load(Path("settings.yaml").read_text())
            assert data["kindling"]["retries"] == 5
            assert isinstance(data["kindling"]["retries"], int)

    def test_float(self):
        runner = CliRunner()
        with runner.isolated_filesystem():
            runner.invoke(cli, ["config", "set", "kindling.timeout", "3.14"])
            data = yaml.safe_load(Path("settings.yaml").read_text())
            assert data["kindling"]["timeout"] == 3.14

    def test_null(self):
        runner = CliRunner()
        with runner.isolated_filesystem():
            runner.invoke(cli, ["config", "set", "kindling.val", "null"])
            data = yaml.safe_load(Path("settings.yaml").read_text())
            assert data["kindling"]["val"] is None

    def test_forced_string_with_quotes(self):
        runner = CliRunner()
        with runner.isolated_filesystem():
            runner.invoke(cli, ["config", "set", "kindling.label", '"true"'])
            data = yaml.safe_load(Path("settings.yaml").read_text())
            assert data["kindling"]["label"] == "true"
            assert isinstance(data["kindling"]["label"], str)


class TestConfigSetLevelRouting:
    def test_platform_level(self):
        runner = CliRunner()
        with runner.isolated_filesystem():
            result = runner.invoke(
                cli,
                [
                    "config",
                    "set",
                    "kindling.secrets.scope",
                    "fabric-scope",
                    "--level",
                    "platform",
                    "--platform",
                    "fabric",
                ],
            )
            assert result.exit_code == 0
            path = Path("platform_fabric.yaml")
            assert path.exists()
            data = yaml.safe_load(path.read_text())
            assert data["kindling"]["secrets"]["scope"] == "fabric-scope"

    def test_env_level(self):
        runner = CliRunner()
        with runner.isolated_filesystem():
            result = runner.invoke(
                cli,
                [
                    "config",
                    "set",
                    "kindling.telemetry.logging.level",
                    "ERROR",
                    "--level",
                    "env",
                    "--env",
                    "prod",
                ],
            )
            assert result.exit_code == 0
            path = Path("env_prod.yaml")
            assert path.exists()
            data = yaml.safe_load(path.read_text())
            assert data["kindling"]["telemetry"]["logging"]["level"] == "ERROR"

    def test_platform_level_requires_platform(self):
        runner = CliRunner()
        with runner.isolated_filesystem():
            result = runner.invoke(
                cli,
                [
                    "config",
                    "set",
                    "kindling.x",
                    "y",
                    "--level",
                    "platform",
                ],
            )
            assert result.exit_code != 0
            assert "--platform is required" in result.output

    def test_env_level_requires_env(self):
        runner = CliRunner()
        with runner.isolated_filesystem():
            result = runner.invoke(
                cli,
                [
                    "config",
                    "set",
                    "kindling.x",
                    "y",
                    "--level",
                    "env",
                ],
            )
            assert result.exit_code != 0
            assert "--env is required" in result.output


class TestConfigSetAppScope:
    def test_app_base(self):
        runner = CliRunner()
        with runner.isolated_filesystem():
            result = runner.invoke(
                cli,
                [
                    "config",
                    "set",
                    "name",
                    "my-app",
                    "--app",
                    "my-data-app",
                ],
            )
            assert result.exit_code == 0
            path = Path("data-apps/my-data-app/settings.yaml")
            assert path.exists()
            data = yaml.safe_load(path.read_text())
            assert data["name"] == "my-app"

    def test_app_with_platform_level(self):
        runner = CliRunner()
        with runner.isolated_filesystem():
            result = runner.invoke(
                cli,
                [
                    "config",
                    "set",
                    "kindling.secrets.scope",
                    "db-scope",
                    "--app",
                    "ingest-app",
                    "--level",
                    "platform",
                    "--platform",
                    "databricks",
                ],
            )
            assert result.exit_code == 0
            path = Path("data-apps/ingest-app/platform_databricks.yaml")
            assert path.exists()
            data = yaml.safe_load(path.read_text())
            assert data["kindling"]["secrets"]["scope"] == "db-scope"


class TestConfigSetConfigDir:
    def test_custom_config_dir(self):
        runner = CliRunner()
        with runner.isolated_filesystem():
            result = runner.invoke(
                cli,
                [
                    "config",
                    "set",
                    "kindling.x",
                    "y",
                    "--config-dir",
                    "my/configs",
                ],
            )
            assert result.exit_code == 0
            assert Path("my/configs/settings.yaml").exists()


def test_generate_bootstrap_notebook_embeds_runtime_script():
    notebook = _generate_bootstrap_notebook(
        "fabric", {"kindling": {"secrets": {"secret_scope": "demo-scope"}}}
    )

    embedded_cell = "".join(notebook["cells"][1]["source"])
    assert "def bootstrap_notebook(bootstrap_config):" in embedded_cell
    assert "kindling_bootstrap.py" in embedded_cell
    assert 'globals()["BOOTSTRAP_CONFIG"] = bootstrap_config' in embedded_cell
    markdown = "".join(notebook["cells"][0]["source"])
    assert "Suggested secret scope: `demo-scope`" in markdown


def test_generate_sample_notebook_embeds_bootstrap_before_hello_world():
    notebook = _generate_sample_notebook("databricks")

    config_cell = "".join(notebook["cells"][1]["source"])
    embedded_cell = "".join(notebook["cells"][2]["source"])
    hello_cell = "".join(notebook["cells"][3]["source"])

    assert '"app_name": "hello-world"' in config_cell
    assert "%run /Shared/kindling/environment_bootstrap" in embedded_cell
    assert 'logger.info("Hello World from Kindling")' in hello_cell


def test_build_fabric_notebook_payload_contains_encoded_notebook_content():
    notebook_data = {"nbformat": 4, "cells": [{"cell_type": "code", "source": ["print('hi')\n"]}]}

    payload = _build_fabric_notebook_payload("demo", notebook_data)

    assert payload["displayName"] == "demo"
    assert payload["type"] == "Notebook"
    parts = payload["definition"]["parts"]
    assert len(parts) == 1
    assert parts[0]["path"] == "notebook-content.ipynb"
    assert parts[0]["payloadType"] == "InlineBase64"
    decoded = json.loads(base64.b64decode(parts[0]["payload"]).decode("utf-8"))
    assert decoded == notebook_data


def test_render_environment_bootstrap_source_uses_platform_default():
    source = _render_environment_bootstrap_source("synapse")

    assert 'DEFAULT_PLATFORM_ENVIRONMENT = "synapse"' in source
    assert 'BOOTSTRAP_SCRIPT_NAME = "kindling_bootstrap.py"' in source
    assert "def execute_bootstrap_script(bootstrap_config):" in source
    assert "Shared Kindling notebook bootstrap helper." in source
    assert "Artifacts path:" in source


def test_render_starter_notebook_source_points_to_environment_bootstrap():
    source = _render_starter_notebook_source("fabric")

    assert '"platform": "fabric"' in source
    assert "%run environment_bootstrap" in source
    assert 'logger.info("Kindling starter notebook ready")' in source
    assert 'BOOTSTRAP_CONFIG["app_name"]' in source
    assert '"extensions": ["kindling-otel-azure>=0.3.0"]' in source


# ---------------------------------------------------------------------------
# env check --local
# ---------------------------------------------------------------------------


class TestEnvCheckLocal:
    def test_local_flag_adds_java_check(self):
        runner = CliRunner()
        with runner.isolated_filesystem():
            runner.invoke(cli, ["config", "init"])
            result = runner.invoke(cli, ["env", "check", "--local"])

        assert "java" in result.output

    def test_local_flag_adds_pyspark_check(self):
        runner = CliRunner()
        with runner.isolated_filesystem():
            runner.invoke(cli, ["config", "init"])
            result = runner.invoke(cli, ["env", "check", "--local"])

        assert "pyspark" in result.output

    def test_local_flag_adds_delta_spark_check(self):
        runner = CliRunner()
        with runner.isolated_filesystem():
            runner.invoke(cli, ["config", "init"])
            result = runner.invoke(cli, ["env", "check", "--local"])

        assert "delta_spark" in result.output

    def test_local_flag_adds_hadoop_jar_check(self):
        runner = CliRunner()
        with runner.isolated_filesystem():
            runner.invoke(cli, ["config", "init"])
            result = runner.invoke(cli, ["env", "check", "--local"])

        assert "hadoop_azure_jars" in result.output

    def test_without_local_flag_no_java_check(self):
        runner = CliRunner()
        with runner.isolated_filesystem():
            runner.invoke(cli, ["config", "init"])
            result = runner.invoke(cli, ["env", "check"])

        # java check should only appear with --local
        assert "[PASS] java" not in result.output
        assert "[FAIL] java" not in result.output

    def test_local_check_reports_missing_jars_when_dir_absent(self, tmp_path, monkeypatch):
        """When /tmp/hadoop-jars doesn't exist the jar check fails gracefully."""
        import kindling_cli.cli as cli_mod

        monkeypatch.setattr(cli_mod, "_HADOOP_JAR_DIR", tmp_path / "no-such-dir")

        runner = CliRunner()
        with runner.isolated_filesystem():
            runner.invoke(cli, ["config", "init"])
            result = runner.invoke(cli, ["env", "check", "--local"])

        assert "[FAIL] hadoop_azure_jars" in result.output
