import base64
import json
import sys
import types
import zipfile
from pathlib import Path

import click
import yaml
from click.testing import CliRunner
from kindling_cli.cli import (
    _build_fabric_notebook_payload,
    _find_wheels,
    _generate_bootstrap_notebook,
    _generate_sample_notebook,
    _load_app_module,
    _render_environment_bootstrap_source,
    _render_starter_notebook_source,
    _resolve_account_url,
    cli,
)


def test_config_init_writes_settings_file():
    runner = CliRunner()
    with runner.isolated_filesystem():
        result = runner.invoke(cli, ["config", "init", "--name", "demo-app"])

        assert result.exit_code == 0, result.output
        # Default output is config/settings.yaml (matches scaffolded project layout)
        settings_path = Path("config/settings.yaml")
        assert settings_path.exists()
        content = settings_path.read_text(encoding="utf-8")
        assert "name: demo-app" in content
        assert "kindling:" in content


def test_top_level_commands_follow_noun_verb_grammar():
    result = CliRunner().invoke(cli, ["--help"])

    assert result.exit_code == 0, result.output
    assert "\n  run " not in result.output
    assert "\n  validate " not in result.output
    assert "\n  new " not in result.output
    assert "\n  project " not in result.output


def test_top_level_commands_do_not_expose_job_group():
    result = CliRunner().invoke(cli, ["--help"])

    assert result.exit_code == 0, result.output
    assert "\n  job " not in result.output


def test_resolve_account_url_uses_blob_suffix_env(monkeypatch):
    monkeypatch.setenv("AZURE_STORAGE_BLOB_ENDPOINT_SUFFIX", "blob.core.usgovcloudapi.net")

    assert _resolve_account_url("acct") == "https://acct.blob.core.usgovcloudapi.net"


def test_resolve_account_url_uses_azure_environment(monkeypatch):
    monkeypatch.delenv("AZURE_STORAGE_BLOB_ENDPOINT_SUFFIX", raising=False)
    monkeypatch.setenv("AZURE_CLOUD", "AzureUSGovernment")

    assert _resolve_account_url("govacct") == "https://govacct.blob.core.usgovcloudapi.net"


def test_resolve_account_url_preserves_explicit_endpoint(monkeypatch):
    monkeypatch.setenv("AZURE_STORAGE_BLOB_ENDPOINT_SUFFIX", "blob.core.usgovcloudapi.net")

    assert _resolve_account_url("acct.blob.custom.example") == "https://acct.blob.custom.example"
    assert (
        _resolve_account_url("https://acct.blob.custom.example")
        == "https://acct.blob.custom.example"
    )


def test_config_init_refuses_to_overwrite_without_force():
    runner = CliRunner()
    with runner.isolated_filesystem():
        settings_path = Path("config/settings.yaml")
        settings_path.parent.mkdir(parents=True, exist_ok=True)
        settings_path.write_text("original", encoding="utf-8")

        result = runner.invoke(cli, ["config", "init"])

        assert result.exit_code != 0
        assert "Refusing to overwrite existing file" in result.output
        assert settings_path.read_text(encoding="utf-8") == "original"


def test_config_init_overwrites_with_force():
    runner = CliRunner()
    with runner.isolated_filesystem():
        settings_path = Path("config/settings.yaml")
        settings_path.parent.mkdir(parents=True, exist_ok=True)
        settings_path.write_text("original", encoding="utf-8")

        result = runner.invoke(cli, ["config", "init", "--force", "--name", "forced-app"])

        assert result.exit_code == 0, result.output
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
# env check --platform  (ki-0nq)
# ---------------------------------------------------------------------------


def test_env_check_platform_missing_vars_exits_nonzero(monkeypatch):
    """--platform with all vars unset should exit 1 and report MISSING."""
    for var in (
        "SYNAPSE_WORKSPACE_NAME",
        "SYNAPSE_SPARK_POOL_NAME",
        "AZURE_TENANT_ID",
        "AZURE_CLIENT_ID",
        "AZURE_CLIENT_SECRET",
    ):
        monkeypatch.delenv(var, raising=False)
    runner = CliRunner()
    with runner.isolated_filesystem():
        result = runner.invoke(cli, ["env", "check", "--platform", "synapse"])

    assert result.exit_code != 0
    assert "Platform: synapse" in result.output
    assert "MISSING" in result.output
    assert "missing" in result.output


def test_env_check_platform_all_vars_set_exits_zero(monkeypatch):
    """--platform with all required vars set should exit 0 and report SET."""
    monkeypatch.setenv("SYNAPSE_WORKSPACE_NAME", "ws")
    monkeypatch.setenv("SYNAPSE_SPARK_POOL_NAME", "pool")
    monkeypatch.setenv("AZURE_TENANT_ID", "tid")
    monkeypatch.setenv("AZURE_CLIENT_ID", "cid")
    monkeypatch.setenv("AZURE_CLIENT_SECRET", "secret")
    runner = CliRunner()
    with runner.isolated_filesystem():
        result = runner.invoke(cli, ["env", "check", "--platform", "synapse"])

    assert result.exit_code == 0
    assert "Platform: synapse" in result.output
    assert "MISSING" not in result.output
    output_lines = [l.strip() for l in result.output.splitlines() if l.strip()]
    set_lines = [l for l in output_lines if "SET" in l]
    assert len(set_lines) == 5


def test_env_check_platform_missing_shows_export_hint(monkeypatch):
    """Missing vars should include 'export VAR=<your-...' hint."""
    for var in ("DATABRICKS_HOST", "DATABRICKS_TOKEN"):
        monkeypatch.delenv(var, raising=False)
    runner = CliRunner()
    with runner.isolated_filesystem():
        result = runner.invoke(cli, ["env", "check", "--platform", "databricks"])

    assert "export DATABRICKS_HOST=" in result.output
    assert "export DATABRICKS_TOKEN=" in result.output


def test_env_check_platform_partial_set_reports_correctly(monkeypatch):
    """If only some vars are set, SET and MISSING are both reported."""
    monkeypatch.setenv("FABRIC_WORKSPACE_ID", "wid")
    for var in ("AZURE_TENANT_ID", "AZURE_CLIENT_ID", "AZURE_CLIENT_SECRET"):
        monkeypatch.delenv(var, raising=False)
    runner = CliRunner()
    with runner.isolated_filesystem():
        result = runner.invoke(cli, ["env", "check", "--platform", "fabric"])

    assert result.exit_code != 0
    assert "FABRIC_WORKSPACE_ID" in result.output
    assert "SET" in result.output
    assert "MISSING" in result.output


def test_env_check_platform_help_shows_flag():
    """--help output should mention --platform."""
    runner = CliRunner()
    result = runner.invoke(cli, ["env", "check", "--help"])
    assert "--platform" in result.output


def test_test_run_passes_explicit_layout_to_runner(monkeypatch):
    captured = {}

    def fake_run_tests(options):
        captured["options"] = options
        return 0

    monkeypatch.setattr("kindling_cli.test_runner.run_tests", fake_run_tests)

    result = CliRunner().invoke(
        cli,
        [
            "test",
            "run",
            "--suite",
            "system",
            "--path",
            "custom/system",
            "--platform",
            "fabric",
            "--test",
            "name_mapper",
            "--ci",
            "--preflight",
            "system",
            "--workers",
            "4",
            "--pytest-arg",
            "--tb=short",
        ],
    )

    assert result.exit_code == 0
    options = captured["options"]
    assert options.suite == "system"
    assert [str(path) for path in options.paths] == ["custom/system"]
    assert options.platform == "fabric"
    assert options.test_filter == "name_mapper"
    assert options.ci is True
    assert options.preflight == "system"
    assert options.workers == "4"
    assert options.pytest_args == ["--tb=short"]


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
            # Write to an explicit path so config set can find it in the same directory
            runner.invoke(
                cli, ["config", "init", "--name", "test-app", "--output", "settings.yaml"]
            )
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
# runtime artifact + remote lifecycle commands
# ---------------------------------------------------------------------------


def test_find_wheels_prefers_combined_runtime_wheel(tmp_path):
    combined = tmp_path / "spark_kindling-0.9.1-py3-none-any.whl"
    legacy = tmp_path / "kindling_synapse-0.9.1-py3-none-any.whl"
    combined.write_text("", encoding="utf-8")
    legacy.write_text("", encoding="utf-8")

    assert _find_wheels(tmp_path, "synapse") == [combined]


def test_find_wheels_falls_back_to_legacy_platform_wheel(tmp_path):
    legacy = tmp_path / "kindling_fabric-0.9.1-py3-none-any.whl"
    legacy.write_text("", encoding="utf-8")

    assert _find_wheels(tmp_path, "fabric") == [legacy]


def test_app_package_creates_kda_archive():
    runner = CliRunner()
    with runner.isolated_filesystem():
        app_dir = Path("demo_app")
        (app_dir / "nested").mkdir(parents=True)
        (app_dir / "app.py").write_text("print('hello')\n", encoding="utf-8")
        (app_dir / "nested" / "settings.yaml").write_text("name: demo\n", encoding="utf-8")

        result = runner.invoke(cli, ["app", "package", "--local-folder", str(app_dir)])

        assert result.exit_code == 0, result.output
        package_path = Path("dist/demo_app.kda")
        assert package_path.exists()
        with zipfile.ZipFile(package_path, "r") as archive:
            assert sorted(archive.namelist()) == ["app.py", "nested/settings.yaml"]


def test_app_package_accepts_positional_path():
    runner = CliRunner()
    with runner.isolated_filesystem():
        app_dir = Path("demo_app")
        app_dir.mkdir()
        (app_dir / "app.py").write_text("print('hello')\n", encoding="utf-8")

        result = runner.invoke(cli, ["app", "package", str(app_dir)], catch_exceptions=False)

        assert result.exit_code == 0, result.output
        assert "deprecated" not in result.output.lower()


def test_app_package_json_output_is_machine_readable():
    runner = CliRunner()
    with runner.isolated_filesystem():
        app_dir = Path("demo_app")
        app_dir.mkdir()
        (app_dir / "app.py").write_text("print('hello')\n", encoding="utf-8")

        result = runner.invoke(cli, ["app", "package", "--local-folder", str(app_dir), "--json"])

        assert result.exit_code == 0, result.output
        payload = json.loads(result.output)
        assert payload["file_count"] == 1
        assert payload["files"] == ["app.py"]
        assert payload["package_path"].endswith("dist/demo_app.kda")


def test_app_deploy_rejects_unsafe_kda_paths():
    runner = CliRunner()
    with runner.isolated_filesystem():
        package_path = Path("demo_app.kda")
        with zipfile.ZipFile(package_path, "w") as archive:
            archive.writestr("../escape.py", "print('nope')\n")

        result = runner.invoke(
            cli,
            ["app", "deploy", "--kda-package", str(package_path), "--platform", "fabric"],
        )

        assert result.exit_code != 0
        assert "unsafe relative path traversal" in result.output


def test_app_deploy_rejects_both_source_flags(monkeypatch):
    monkeypatch.setattr(
        "kindling_cli.cli._create_platform_api",
        lambda platform: (object(), platform),
    )
    runner = CliRunner()
    with runner.isolated_filesystem():
        app_dir = Path("demo_app")
        app_dir.mkdir()
        (app_dir / "app.py").write_text("", encoding="utf-8")
        kda = Path("demo_app.kda")
        with zipfile.ZipFile(kda, "w") as archive:
            archive.writestr("app.py", "")

        result = runner.invoke(
            cli,
            [
                "app",
                "deploy",
                "--local-folder",
                str(app_dir),
                "--kda-package",
                str(kda),
                "--platform",
                "fabric",
            ],
        )

        assert result.exit_code != 0
        assert "mutually exclusive" in result.output


def test_app_deploy_requires_source_flag():
    runner = CliRunner()
    result = runner.invoke(cli, ["app", "deploy", "--platform", "fabric"])
    assert result.exit_code != 0
    assert "--local-folder" in result.output or "source" in result.output


def test_app_deploy_fails_fast_when_platform_vars_missing(monkeypatch):
    monkeypatch.delenv("FABRIC_WORKSPACE_ID", raising=False)
    monkeypatch.delenv("FABRIC_LAKEHOUSE_ID", raising=False)

    runner = CliRunner()
    with runner.isolated_filesystem():
        app_dir = Path("demo_app")
        app_dir.mkdir()
        (app_dir / "app.py").write_text("print('hello')\n", encoding="utf-8")

        result = runner.invoke(
            cli,
            ["app", "deploy", "--local-folder", str(app_dir), "--platform", "fabric"],
        )

    assert result.exit_code != 0
    assert "Missing required environment variables for fabric" in result.output
    assert "FABRIC_WORKSPACE_ID" in result.output
    assert "FABRIC_LAKEHOUSE_ID" in result.output


def test_app_deploy_uses_platform_sdk(monkeypatch):
    class FakeAPI:
        def __init__(self):
            self.calls = []

        def deploy_app(self, app_name, app_files):
            self.calls.append((app_name, app_files))
            return "abfss://artifacts@acct.dfs.core.windows.net/dev/data-apps/demo"

    fake_api = FakeAPI()
    monkeypatch.setattr(
        "kindling_cli.cli._create_platform_api",
        lambda platform: (fake_api, platform),
    )

    runner = CliRunner()
    with runner.isolated_filesystem():
        app_dir = Path("demo_app")
        (app_dir / "pipelines").mkdir(parents=True)
        (app_dir / "app.py").write_text("print('hello')\n", encoding="utf-8")
        (app_dir / "pipelines" / "job.yml").write_text("job_name: demo\n", encoding="utf-8")

        result = runner.invoke(
            cli, ["app", "deploy", "--local-folder", str(app_dir), "--platform", "fabric"]
        )

        assert result.exit_code == 0, result.output
        assert fake_api.calls[0][0] == "demo_app"
        assert sorted(fake_api.calls[0][1]) == ["app.py", "pipelines/job.yml"]
        assert "Deployed app `demo_app`" in result.output


def test_app_deploy_kda_package_flag(monkeypatch):
    class FakeAPI:
        def deploy_app(self, app_name, app_files):
            return "abfss://artifacts@acct.dfs.core.windows.net/dev/data-apps/demo"

    monkeypatch.setattr(
        "kindling_cli.cli._create_platform_api",
        lambda platform: (FakeAPI(), platform),
    )

    runner = CliRunner()
    with runner.isolated_filesystem():
        kda = Path("demo_app.kda")
        with zipfile.ZipFile(kda, "w") as archive:
            archive.writestr("app.py", "print('hello')\n")

        result = runner.invoke(
            cli, ["app", "deploy", "--kda-package", str(kda), "--platform", "fabric"]
        )

        assert result.exit_code == 0, result.output
        assert "Deployed app `demo_app`" in result.output


def test_app_deploy_json_output_includes_storage_path(monkeypatch):
    class FakeAPI:
        def deploy_app(self, app_name, app_files):
            return "abfss://artifacts@acct.dfs.core.windows.net/dev/data-apps/demo"

    monkeypatch.setattr(
        "kindling_cli.cli._create_platform_api",
        lambda platform: (FakeAPI(), platform),
    )

    runner = CliRunner()
    with runner.isolated_filesystem():
        app_dir = Path("demo_app")
        app_dir.mkdir()
        (app_dir / "app.py").write_text("print('hello')\n", encoding="utf-8")

        result = runner.invoke(
            cli,
            ["app", "deploy", "--local-folder", str(app_dir), "--platform", "fabric", "--json"],
        )

        assert result.exit_code == 0, result.output
        payload = json.loads(result.output)
        assert payload["app_name"] == "demo_app"
        assert payload["platform"] == "fabric"
        assert payload["storage_path"].startswith("abfss://")


def test_app_run_requires_app_argument(monkeypatch):
    monkeypatch.setattr(
        "kindling_cli.cli._create_platform_api",
        lambda platform: (object(), platform),
    )
    runner = CliRunner()
    result = runner.invoke(cli, ["app", "run", "--platform", "fabric"])
    assert result.exit_code != 0
    assert "Missing argument 'APP'" in result.output


def test_app_run_deployed_name_creates_and_runs_job(monkeypatch):
    class FakeAPI:
        def __init__(self):
            self.submitted = []

        def get_runner_status(self, platform):
            return {"runner_id": "kindling-runner", "state": "installed"}

        def submit_app_run(self, app_name, environment=None, parameters=None):
            self.submitted.append((app_name, environment, parameters))
            return "run-1"

    fake_api = FakeAPI()

    monkeypatch.setattr(
        "kindling_cli.cli._create_platform_api",
        lambda platform: (fake_api, platform),
    )
    runner = CliRunner()
    result = runner.invoke(cli, ["app", "run", "orders", "--platform", "fabric", "--no-wait"])
    assert result.exit_code == 0, result.output
    assert "run-1" in result.output
    assert fake_api.submitted == [("orders", None, None)]


def test_app_run_with_local_path_deploys_creates_and_runs(monkeypatch):
    class FakeAPI:
        def __init__(self):
            self.deployed = []
            self.submitted = []

        def deploy_app(self, app_name, app_files):
            self.deployed.append((app_name, app_files))
            return "abfss://artifacts@acct.dfs.core.windows.net/dev/data-apps/orders"

        def get_runner_status(self, platform):
            return {"runner_id": "kindling-runner", "state": "installed"}

        def submit_app_run(self, app_name, environment=None, parameters=None):
            self.submitted.append((app_name, environment, parameters))
            return "run-1"

    fake_api = FakeAPI()
    monkeypatch.setattr(
        "kindling_cli.cli._create_platform_api",
        lambda platform: (fake_api, platform),
    )

    runner = CliRunner()
    with runner.isolated_filesystem():
        app_dir = Path("orders")
        app_dir.mkdir()
        (app_dir / "app.py").write_text("", encoding="utf-8")

        result = runner.invoke(
            cli,
            [
                "app",
                "run",
                str(app_dir),
                "--platform",
                "fabric",
                "--no-wait",
            ],
        )

    assert result.exit_code == 0, result.output
    assert "run-1" in result.output
    assert fake_api.deployed[0][0] == "orders"
    assert sorted(fake_api.deployed[0][1]) == ["app.py"]
    assert fake_api.submitted[0][0] == "orders"


def test_app_cleanup_positional_name(monkeypatch):
    class FakeAPI:
        def cleanup_app(self, app_name):
            assert app_name == "orders"
            return True

    monkeypatch.setattr(
        "kindling_cli.cli._create_platform_api",
        lambda platform: (FakeAPI(), platform),
    )

    runner = CliRunner()
    result = runner.invoke(cli, ["app", "cleanup", "orders", "--platform", "fabric"])
    assert result.exit_code == 0, result.output
    assert "orders" in result.output


def test_app_cleanup_local_folder_infers_name(monkeypatch):
    class FakeAPI:
        def cleanup_app(self, app_name):
            assert app_name == "orders"
            return True

    monkeypatch.setattr(
        "kindling_cli.cli._create_platform_api",
        lambda platform: (FakeAPI(), platform),
    )

    runner = CliRunner()
    with runner.isolated_filesystem():
        app_dir = Path("orders")
        app_dir.mkdir()
        (app_dir / "app.py").write_text("", encoding="utf-8")

        result = runner.invoke(
            cli, ["app", "cleanup", "--local-folder", str(app_dir), "--platform", "fabric"]
        )

        assert result.exit_code == 0, result.output
        assert "orders" in result.output


def test_app_cleanup_kda_package_infers_name(monkeypatch):
    class FakeAPI:
        def cleanup_app(self, app_name):
            assert app_name == "orders"
            return True

    monkeypatch.setattr(
        "kindling_cli.cli._create_platform_api",
        lambda platform: (FakeAPI(), platform),
    )

    runner = CliRunner()
    with runner.isolated_filesystem():
        kda = Path("orders.kda")
        with zipfile.ZipFile(kda, "w") as archive:
            archive.writestr("app.py", "")

        result = runner.invoke(
            cli, ["app", "cleanup", "--kda-package", str(kda), "--platform", "fabric"]
        )

        assert result.exit_code == 0, result.output
        assert "orders" in result.output


def test_app_cleanup_rejects_both_source_flags(monkeypatch):
    runner = CliRunner()
    with runner.isolated_filesystem():
        app_dir = Path("orders")
        app_dir.mkdir()
        kda = Path("orders.kda")
        with zipfile.ZipFile(kda, "w") as archive:
            archive.writestr("app.py", "")

        result = runner.invoke(
            cli,
            [
                "app",
                "cleanup",
                "--local-folder",
                str(app_dir),
                "--kda-package",
                str(kda),
                "--platform",
                "fabric",
            ],
        )

        assert result.exit_code != 0
        assert "mutually exclusive" in result.output


def test_app_cleanup_requires_identifier():
    runner = CliRunner()
    result = runner.invoke(cli, ["app", "cleanup", "--platform", "fabric"])
    assert result.exit_code != 0


def test_app_cleanup_fails_fast_when_platform_vars_missing(monkeypatch):
    monkeypatch.delenv("FABRIC_WORKSPACE_ID", raising=False)
    monkeypatch.delenv("FABRIC_LAKEHOUSE_ID", raising=False)

    result = CliRunner().invoke(cli, ["app", "cleanup", "orders", "--platform", "fabric"])
    assert result.exit_code != 0
    assert "Missing required environment variables for fabric" in result.output
    assert "FABRIC_WORKSPACE_ID" in result.output
    assert "FABRIC_LAKEHOUSE_ID" in result.output


def test_app_logs_rejects_negative_from_line():
    result = CliRunner().invoke(
        cli,
        ["app", "logs", "run-123", "--platform", "fabric", "--from-line", "-1"],
    )

    assert result.exit_code != 0
    assert "--from-line must be greater than or equal to 0." in result.output


def test_app_status_uses_runner_aligned_sdk(monkeypatch):
    from unittest.mock import MagicMock

    import kindling_cli.cli as cli_mod

    api = MagicMock()
    api.get_app_run_status.return_value = {"state": "SUCCEEDED", "runner_id": "kindling-runner"}
    monkeypatch.setattr(cli_mod, "_create_platform_api", lambda p: (api, p))
    monkeypatch.setenv("FABRIC_WORKSPACE_ID", "ws")

    result = CliRunner().invoke(cli, ["app", "status", "run-1", "--platform", "fabric"])

    assert result.exit_code == 0, result.output
    api.get_app_run_status.assert_called_once_with("run-1")
    assert "SUCCEEDED" in result.output


def test_app_cancel_uses_runner_aligned_sdk(monkeypatch):
    from unittest.mock import MagicMock

    import kindling_cli.cli as cli_mod

    api = MagicMock()
    api.cancel_app_run.return_value = True
    monkeypatch.setattr(cli_mod, "_create_platform_api", lambda p: (api, p))
    monkeypatch.setenv("FABRIC_WORKSPACE_ID", "ws")

    result = CliRunner().invoke(cli, ["app", "cancel", "run-1", "--platform", "fabric"])

    assert result.exit_code == 0, result.output
    api.cancel_app_run.assert_called_once_with("run-1")


def test_app_logs_stream_does_not_require_job_id(monkeypatch):
    from unittest.mock import MagicMock

    import kindling_cli.cli as cli_mod

    api = MagicMock()
    api.stream_app_run_logs.return_value = []
    monkeypatch.setattr(cli_mod, "_create_platform_api", lambda p: (api, p))
    monkeypatch.setenv("FABRIC_WORKSPACE_ID", "ws")

    result = CliRunner().invoke(cli, ["app", "logs", "run-1", "--platform", "fabric", "--stream"])

    assert result.exit_code == 0, result.output
    api.stream_app_run_logs.assert_called_once()


def test_app_logs_non_stream_uses_runner_aligned_sdk(monkeypatch):
    from unittest.mock import MagicMock

    import kindling_cli.cli as cli_mod

    api = MagicMock()
    api.get_app_run_logs.return_value = {"lines": ["line1"], "total": 1}
    monkeypatch.setattr(cli_mod, "_create_platform_api", lambda p: (api, p))
    monkeypatch.setenv("FABRIC_WORKSPACE_ID", "ws")

    result = CliRunner().invoke(cli, ["app", "logs", "run-1", "--platform", "fabric"])

    assert result.exit_code == 0, result.output
    api.get_app_run_logs.assert_called_once_with("run-1", from_line=0, size=1000)


def test_workspace_deploy_fails_when_config_missing_unless_allowed(monkeypatch):
    monkeypatch.setattr("kindling_cli.cli._get_blob_service_client", lambda account: object())

    runner = CliRunner()
    with runner.isolated_filesystem():
        result = runner.invoke(
            cli,
            [
                "workspace",
                "deploy",
                "--platform",
                "synapse",
                "--storage-account",
                "acct",
                "--skip-wheels",
                "--skip-bootstrap-script",
            ],
        )

    assert result.exit_code != 0
    assert "Use --skip-config or --allow-missing-config" in result.output


def test_workspace_deploy_fails_when_bootstrap_missing_unless_allowed(monkeypatch):
    monkeypatch.setattr("kindling_cli.cli._get_blob_service_client", lambda account: object())

    runner = CliRunner()
    with runner.isolated_filesystem():
        Path("settings.yaml").write_text("kindling: {}\n", encoding="utf-8")
        result = runner.invoke(
            cli,
            [
                "workspace",
                "deploy",
                "--platform",
                "synapse",
                "--storage-account",
                "acct",
                "--skip-wheels",
                "--skip-config",
            ],
        )

    assert result.exit_code != 0
    assert "Use --skip-bootstrap-script or --allow-missing-bootstrap-script" in result.output


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


class TestAppRunCommand:
    """Tests for `kindling app run` — local standalone and remote app workflows."""

    def _make_mock_api(self):
        from unittest.mock import MagicMock

        api = MagicMock()
        api.deploy_app.return_value = "abfss://container@acct.dfs.core.windows.net/data-apps/my-app"
        api.get_runner_status.return_value = {"runner_id": "kindling-runner", "state": "installed"}
        api.submit_app_run.return_value = "run-42"
        api.get_app_run_status.return_value = {
            "state": "SUCCEEDED",
            "runner_id": "kindling-runner",
        }
        api.stream_app_run_logs.return_value = []
        return api

    def test_standalone_default_runs_app_as_subprocess(self, tmp_path, monkeypatch):
        import subprocess

        app_dir = tmp_path / "myapp"
        app_dir.mkdir()
        (app_dir / "app.py").write_text("# stub\n", encoding="utf-8")

        calls = []

        def fake_run(cmd, env=None, **kwargs):
            calls.append(cmd)
            return subprocess.CompletedProcess(cmd, returncode=0)

        monkeypatch.setattr(subprocess, "run", fake_run)

        runner = CliRunner()
        result = runner.invoke(cli, ["app", "run", str(app_dir)])

        assert result.exit_code == 0, result.output
        assert "standalone" in result.output
        assert len(calls) == 1
        assert str(app_dir / "app.py") in calls[0][-1]

    def test_standalone_json_keeps_stdout_machine_readable(self, tmp_path, monkeypatch):
        import subprocess

        app_dir = tmp_path / "myapp"
        app_dir.mkdir()
        (app_dir / "app.py").write_text("# stub\n")

        def fake_run(cmd, env=None, **kwargs):
            return subprocess.CompletedProcess(cmd, returncode=0)

        monkeypatch.setattr(subprocess, "run", fake_run)

        runner = CliRunner()
        result = runner.invoke(cli, ["app", "run", str(app_dir), "--json"])

        assert result.exit_code == 0, result.output
        payload = json.loads(result.stdout)
        assert payload["platform"] == "standalone"
        assert payload["succeeded"] is True
        assert "Running app locally" not in result.stdout
        assert "Running app locally" in result.stderr

    def test_standalone_accepts_config_and_quiet(self, tmp_path, monkeypatch):
        import subprocess

        app_dir = tmp_path / "myapp"
        app_dir.mkdir()
        (app_dir / "app.py").write_text("# stub\n")
        config_dir = tmp_path / "config"
        config_dir.mkdir()

        captured_env = {}

        def fake_run(cmd, env=None, **kwargs):
            captured_env.update(env or {})
            return subprocess.CompletedProcess(cmd, returncode=0)

        monkeypatch.setattr(subprocess, "run", fake_run)

        result = CliRunner().invoke(
            cli,
            [
                "app",
                "run",
                str(app_dir),
                "--env",
                "dev",
                "--config",
                str(config_dir),
                "--quiet",
            ],
        )

        assert result.exit_code == 0, result.output
        assert captured_env["KINDLING_ENV"] == "dev"
        assert captured_env["KINDLING_SPARK_ENABLE_DELTA"] == "true"
        assert captured_env["KINDLING_CONFIG_DIR"] == str(config_dir.resolve())
        assert captured_env["KINDLING_LOG_LEVEL"] == "WARNING"

    def test_standalone_preserves_delta_env_override(self, tmp_path, monkeypatch):
        import subprocess

        app_dir = tmp_path / "myapp"
        app_dir.mkdir()
        (app_dir / "app.py").write_text("# stub\n")
        captured_env = {}

        def fake_run(cmd, env=None, **kwargs):
            captured_env.update(env or {})
            return subprocess.CompletedProcess(cmd, returncode=0)

        monkeypatch.setenv("KINDLING_SPARK_ENABLE_DELTA", "false")
        monkeypatch.setattr(subprocess, "run", fake_run)

        result = CliRunner().invoke(cli, ["app", "run", str(app_dir)])

        assert result.exit_code == 0, result.output
        assert captured_env["KINDLING_SPARK_ENABLE_DELTA"] == "false"

    def test_standalone_rejects_remote_only_options(self, tmp_path):
        app_dir = tmp_path / "myapp"
        app_dir.mkdir()
        (app_dir / "app.py").write_text("def initialize(env=None):\n    return None\n")

        runner = CliRunner()
        result = runner.invoke(cli, ["app", "run", str(app_dir), "--no-wait"])

        assert result.exit_code != 0
        assert "remote app runs" in result.output

    def test_submit_no_wait_returns_run_id(self, tmp_path, monkeypatch):
        import kindling_cli.cli as cli_mod

        mock_api = self._make_mock_api()
        monkeypatch.setattr(cli_mod, "_create_platform_api", lambda p: (mock_api, p))

        app_dir = tmp_path / "myapp"
        app_dir.mkdir()
        (app_dir / "app.py").write_text("# app")

        runner = CliRunner()
        result = runner.invoke(
            cli,
            ["app", "run", str(app_dir), "--platform", "synapse", "--no-wait"],
        )

        assert result.exit_code == 0, result.output
        assert "run-42" in result.output
        mock_api.deploy_app.assert_called_once()
        mock_api.get_runner_status.assert_called_once()
        mock_api.submit_app_run.assert_called_once()

    def test_remote_json_keeps_stdout_machine_readable(self, tmp_path, monkeypatch):
        import kindling_cli.cli as cli_mod

        mock_api = self._make_mock_api()
        monkeypatch.setattr(cli_mod, "_create_platform_api", lambda p: (mock_api, p))

        app_dir = tmp_path / "myapp"
        app_dir.mkdir()
        (app_dir / "app.py").write_text("# app")

        result = CliRunner().invoke(
            cli,
            ["app", "run", str(app_dir), "--platform", "fabric", "--no-wait", "--json"],
        )

        assert result.exit_code == 0, result.output
        payload = json.loads(result.stdout)
        assert payload["run_id"] == "run-42"
        assert payload["platform"] == "fabric"
        assert "[1/3]" not in result.stdout
        assert "[1/3]" in result.stderr

    def test_remote_rejects_standalone_only_options(self, tmp_path, monkeypatch):
        import kindling_cli.cli as cli_mod

        mock_api = self._make_mock_api()
        monkeypatch.setattr(cli_mod, "_create_platform_api", lambda p: (mock_api, p))

        app_dir = tmp_path / "myapp"
        app_dir.mkdir()
        (app_dir / "app.py").write_text("# app")

        result = CliRunner().invoke(
            cli,
            ["app", "run", str(app_dir), "--platform", "fabric", "--config", "config"],
        )

        assert result.exit_code != 0
        assert "--config is only valid for standalone app runs" in result.output

    def test_submit_waits_for_completion_by_default(self, tmp_path, monkeypatch):
        import kindling_cli.cli as cli_mod

        mock_api = self._make_mock_api()
        monkeypatch.setattr(cli_mod, "_create_platform_api", lambda p: (mock_api, p))

        app_dir = tmp_path / "myapp"
        app_dir.mkdir()
        (app_dir / "app.py").write_text("# app")

        runner = CliRunner()
        result = runner.invoke(
            cli,
            ["app", "run", str(app_dir), "--platform", "synapse"],
        )

        assert result.exit_code == 0, result.output
        assert "SUCCEEDED" in result.output
        mock_api.get_app_run_status.assert_called()

    def test_submit_fails_on_error_by_default(self, tmp_path, monkeypatch):
        import kindling_cli.cli as cli_mod

        mock_api = self._make_mock_api()
        mock_api.get_app_run_status.return_value = {"state": "FAILED"}
        monkeypatch.setattr(cli_mod, "_create_platform_api", lambda p: (mock_api, p))

        app_dir = tmp_path / "myapp"
        app_dir.mkdir()
        (app_dir / "app.py").write_text("# app")

        runner = CliRunner()
        result = runner.invoke(
            cli,
            ["app", "run", str(app_dir), "--platform", "synapse"],
        )

        assert result.exit_code != 0
        assert "FAILED" in result.output

    def test_submit_no_fail_on_error_exits_zero(self, tmp_path, monkeypatch):
        import kindling_cli.cli as cli_mod

        mock_api = self._make_mock_api()
        mock_api.get_app_run_status.return_value = {"state": "FAILED"}
        monkeypatch.setattr(cli_mod, "_create_platform_api", lambda p: (mock_api, p))

        app_dir = tmp_path / "myapp"
        app_dir.mkdir()
        (app_dir / "app.py").write_text("# app")

        runner = CliRunner()
        result = runner.invoke(
            cli,
            ["app", "run", str(app_dir), "--platform", "synapse", "--no-fail-on-error"],
        )

        assert result.exit_code == 0, result.output

    def test_remote_submit_requires_platform_or_env_var(self, tmp_path, monkeypatch):
        import kindling_cli.cli as cli_mod

        monkeypatch.delenv("SYNAPSE_WORKSPACE_NAME", raising=False)
        monkeypatch.delenv("FABRIC_WORKSPACE_ID", raising=False)
        monkeypatch.delenv("DATABRICKS_HOST", raising=False)

        app_dir = tmp_path / "myapp"
        app_dir.mkdir()
        (app_dir / "app.py").write_text("# app")

        runner = CliRunner()
        result = runner.invoke(
            cli,
            [
                "app",
                "run",
                str(app_dir),
                "--platform",
                "synapse",
            ],
        )

        assert result.exit_code != 0
        assert "SYNAPSE_WORKSPACE_NAME" in result.output


# ---------------------------------------------------------------------------
# env check --platform
# ---------------------------------------------------------------------------


_BLANK_PLATFORM_DETECT_VARS = {
    "FABRIC_WORKSPACE_ID": "",
    "SYNAPSE_WORKSPACE_NAME": "",
    "DATABRICKS_HOST": "",
    # Blank out SP creds so real .env values don't leak into platform env-check tests
    "AZURE_TENANT_ID": "",
    "AZURE_CLIENT_ID": "",
    "AZURE_CLIENT_SECRET": "",
}


class TestEnvCheckPlatform:
    def _run_with_env(self, platform: str, env: dict):
        runner = CliRunner()
        merged = {**_BLANK_PLATFORM_DETECT_VARS, **env}
        with runner.isolated_filesystem():
            runner.invoke(cli, ["config", "init"])
            result = runner.invoke(
                cli, ["env", "check", "--platform", platform], env=merged, catch_exceptions=False
            )
        return result

    def test_databricks_all_vars_set_passes(self):
        result = self._run_with_env(
            "databricks",
            {
                "DATABRICKS_HOST": "https://adb-123.azuredatabricks.net",
                "DATABRICKS_TOKEN": "dapi-abc",
            },
        )
        # New human-readable format: SET / MISSING
        assert "DATABRICKS_HOST" in result.output
        assert "SET" in result.output
        assert "MISSING" not in result.output
        assert result.exit_code == 0

    def test_databricks_missing_token_fails(self):
        result = self._run_with_env(
            "databricks",
            {"DATABRICKS_HOST": "https://adb-123.azuredatabricks.net"},
        )
        assert "DATABRICKS_TOKEN" in result.output
        assert "MISSING" in result.output
        assert result.exit_code != 0

    def test_fabric_all_vars_set_passes(self):
        result = self._run_with_env(
            "fabric",
            {
                "FABRIC_WORKSPACE_ID": "ws-1",
                "AZURE_TENANT_ID": "tenant-1",
                "AZURE_CLIENT_ID": "client-1",
                "AZURE_CLIENT_SECRET": "secret-1",
            },
        )
        assert "FABRIC_WORKSPACE_ID" in result.output
        assert "AZURE_TENANT_ID" in result.output
        assert "AZURE_CLIENT_ID" in result.output
        assert "MISSING" not in result.output
        assert result.exit_code == 0

    def test_fabric_missing_vars_fails(self):
        result = self._run_with_env("fabric", {})
        assert "FABRIC_WORKSPACE_ID" in result.output
        assert "MISSING" in result.output
        assert result.exit_code != 0

    def test_synapse_all_vars_set_passes(self):
        result = self._run_with_env(
            "synapse",
            {
                "SYNAPSE_WORKSPACE_NAME": "my-ws",
                "SYNAPSE_SPARK_POOL_NAME": "my-pool",
                "AZURE_TENANT_ID": "tenant-1",
                "AZURE_CLIENT_ID": "client-1",
                "AZURE_CLIENT_SECRET": "secret-1",
            },
        )
        assert "SYNAPSE_WORKSPACE_NAME" in result.output
        assert "AZURE_TENANT_ID" in result.output
        assert "MISSING" not in result.output
        assert result.exit_code == 0

    def test_synapse_missing_workspace_name_fails(self):
        result = self._run_with_env(
            "synapse",
            {
                "AZURE_TENANT_ID": "tenant-1",
                "AZURE_CLIENT_ID": "client-1",
            },
        )
        assert "SYNAPSE_WORKSPACE_NAME" in result.output
        assert "MISSING" in result.output
        assert result.exit_code != 0

    def test_platform_label_shown_in_output(self):
        result = self._run_with_env(
            "databricks",
            {
                "DATABRICKS_HOST": "https://adb-123.azuredatabricks.net",
                "DATABRICKS_TOKEN": "dapi-abc",
            },
        )
        # New format uses "Platform: <name>" header
        assert "Platform: databricks" in result.output

    def test_no_platform_flag_no_env_var_checks(self):
        runner = CliRunner()
        with runner.isolated_filesystem():
            runner.invoke(cli, ["config", "init"])
            result = runner.invoke(
                cli,
                ["env", "check"],
                env=dict(_BLANK_PLATFORM_DETECT_VARS),
                catch_exceptions=False,
            )
        assert "env:DATABRICKS_HOST" not in result.output
        assert "env:FABRIC_WORKSPACE_ID" not in result.output
        assert "env:SYNAPSE_WORKSPACE_NAME" not in result.output

    def test_auto_detect_databricks_from_env(self):
        runner = CliRunner()
        with runner.isolated_filesystem():
            runner.invoke(cli, ["config", "init"])
            result = runner.invoke(
                cli,
                ["env", "check"],
                env={
                    **_BLANK_PLATFORM_DETECT_VARS,
                    "DATABRICKS_HOST": "https://adb-123.azuredatabricks.net",
                    "DATABRICKS_TOKEN": "dapi-abc",
                },
                catch_exceptions=False,
            )
        assert "[PASS] platform: databricks" in result.output
        assert "env:DATABRICKS_HOST" in result.output


# ---------------------------------------------------------------------------
# pipeline run — actionable error messages (kindling-6m3)
# ---------------------------------------------------------------------------


def test_run_missing_config_gives_actionable_message():
    runner = CliRunner()
    with runner.isolated_filesystem():
        result = runner.invoke(cli, ["pipeline", "run", "my_pipe"])
        assert result.exit_code != 0
        assert "Config file not found at config/settings.yaml" in result.output
        assert "kindling config init" in result.output
        assert "--config" in result.output


def test_load_app_module_syntax_error_gives_actionable_message(tmp_path):
    app_py = tmp_path / "app.py"
    app_py.write_text("def foo(:\n    pass\n", encoding="utf-8")
    try:
        _load_app_module(app_py, "local")
        assert False, "expected ClickException"
    except click.ClickException as exc:
        msg = exc.format_message()
        assert "Failed to import app.py" in msg
        assert str(app_py) in msg
        assert "Hint:" in msg


def test_load_app_module_import_error_gives_actionable_message(tmp_path):
    app_py = tmp_path / "app.py"
    app_py.write_text("from nonexistent_package_xyz import something\n", encoding="utf-8")
    try:
        _load_app_module(app_py, "local")
        assert False, "expected ClickException"
    except click.ClickException as exc:
        msg = exc.format_message()
        assert "Failed to import app.py" in msg
        assert str(app_py) in msg
        assert "Hint:" in msg


def test_run_missing_pipe_gives_actionable_message(monkeypatch, tmp_path):
    config_dir = tmp_path / "config"
    config_dir.mkdir()
    (config_dir / "settings.yaml").write_text("name: test-app\n", encoding="utf-8")

    app_py = tmp_path / "app.py"
    app_py.write_text("def initialize(env='local'): pass\n", encoding="utf-8")

    fake_registry = types.SimpleNamespace(
        get_pipe_definition=lambda pipe_id: None,
        get_pipe_ids=lambda: ["bronze_to_silver", "silver_to_gold"],
    )
    fake_injector = types.SimpleNamespace(get=lambda cls: fake_registry)

    fake_kindling_pipes = types.ModuleType("kindling.data_pipes")
    fake_kindling_pipes.DataPipesRegistry = object
    fake_kindling_pipes.DataPipesExecution = object
    fake_kindling_injection = types.ModuleType("kindling.injection")
    fake_kindling_injection.GlobalInjector = fake_injector

    monkeypatch.setitem(sys.modules, "kindling.data_pipes", fake_kindling_pipes)
    monkeypatch.setitem(sys.modules, "kindling.injection", fake_kindling_injection)
    monkeypatch.setattr("kindling_cli.cli._load_app_module", lambda *a, **kw: None)

    runner = CliRunner()
    with runner.isolated_filesystem():
        Path("config").mkdir()
        Path("config/settings.yaml").write_text("name: test-app\n", encoding="utf-8")
        result = runner.invoke(cli, ["pipeline", "run", "slver_to_gold", "--app", str(app_py)])

    assert result.exit_code != 0
    assert "Pipe 'slver_to_gold' not found" in result.output
    assert "bronze_to_silver" in result.output
    assert "silver_to_gold" in result.output
    assert "kindling pipeline list" in result.output or "kindling app validate" in result.output


# ---------------------------------------------------------------------------
# runner command group — ki-sag
# ---------------------------------------------------------------------------


class _FakeRunnerAPI:
    """Minimal fake platform API for runner tests."""

    def ensure_runner(self, platform):
        return {"runner_id": "runner-001", "state": "HEALTHY", "version": "1.2.3"}

    def get_runner_status(self, platform):
        return {"runner_id": "runner-001", "state": "HEALTHY", "version": "1.2.3"}

    def repair_runner(self, platform):
        return {"runner_id": "runner-001", "state": "HEALTHY", "version": "1.2.3"}

    def delete_runner(self, platform):
        return True

    def run_job(self, job_id, parameters=None):
        return "run-999"


def test_runner_help_lists_all_subcommands():
    runner = CliRunner()
    result = runner.invoke(cli, ["runner", "--help"])
    assert result.exit_code == 0, result.output
    for sub in ("ensure", "status", "repair", "delete", "invoke"):
        assert sub in result.output


def test_runner_ensure_succeeds(monkeypatch):
    monkeypatch.setenv("SYNAPSE_WORKSPACE_NAME", "ws-test")
    monkeypatch.setenv("SYNAPSE_DEV_ENDPOINT", "https://dev.endpoint")
    monkeypatch.setattr("kindling_cli.cli._create_platform_api", lambda p: (_FakeRunnerAPI(), p))
    runner = CliRunner()
    result = runner.invoke(cli, ["runner", "ensure", "--platform", "synapse"])
    assert result.exit_code == 0, result.output
    assert "runner-001" in result.output


def test_runner_ensure_json_output(monkeypatch):
    monkeypatch.setenv("SYNAPSE_WORKSPACE_NAME", "ws-test")
    monkeypatch.setenv("SYNAPSE_DEV_ENDPOINT", "https://dev.endpoint")
    monkeypatch.setattr("kindling_cli.cli._create_platform_api", lambda p: (_FakeRunnerAPI(), p))
    runner = CliRunner()
    result = runner.invoke(cli, ["runner", "ensure", "--platform", "synapse", "--json"])
    assert result.exit_code == 0, result.output
    payload = json.loads(result.output)
    assert payload["runner_id"] == "runner-001"
    assert payload["platform"] == "synapse"


def test_runner_ensure_requires_platform_or_env():
    runner = CliRunner()
    result = runner.invoke(cli, ["runner", "ensure"], env=dict(_BLANK_PLATFORM_DETECT_VARS))
    assert result.exit_code != 0
    assert "Unable to determine platform" in result.output


def test_runner_ensure_fails_when_platform_vars_missing(monkeypatch):
    # DATABRICKS_HOST set but TOKEN absent
    monkeypatch.setenv("DATABRICKS_HOST", "https://adb-123.azuredatabricks.net")
    monkeypatch.delenv("DATABRICKS_TOKEN", raising=False)
    runner = CliRunner()
    result = runner.invoke(cli, ["runner", "ensure", "--platform", "databricks"])
    assert result.exit_code != 0
    assert "Missing required environment variables" in result.output


def test_runner_status_summary(monkeypatch):
    monkeypatch.setattr("kindling_cli.cli._create_platform_api", lambda p: (_FakeRunnerAPI(), p))
    runner = CliRunner()
    result = runner.invoke(cli, ["runner", "status", "--platform", "synapse"])
    assert result.exit_code == 0, result.output
    assert "runner-001" in result.output
    assert "HEALTHY" in result.output


def test_runner_status_verbose(monkeypatch):
    monkeypatch.setattr("kindling_cli.cli._create_platform_api", lambda p: (_FakeRunnerAPI(), p))
    runner = CliRunner()
    result = runner.invoke(cli, ["runner", "status", "--platform", "synapse", "--verbose"])
    assert result.exit_code == 0, result.output
    payload = json.loads(result.output)
    assert payload["runner_id"] == "runner-001"


def test_runner_status_json(monkeypatch):
    monkeypatch.setattr("kindling_cli.cli._create_platform_api", lambda p: (_FakeRunnerAPI(), p))
    runner = CliRunner()
    result = runner.invoke(cli, ["runner", "status", "--platform", "fabric", "--json"])
    assert result.exit_code == 0, result.output
    payload = json.loads(result.output)
    assert payload["runner_id"] == "runner-001"
    assert payload["platform"] == "fabric"


def test_runner_status_requires_platform_or_env():
    runner = CliRunner()
    result = runner.invoke(cli, ["runner", "status"], env=dict(_BLANK_PLATFORM_DETECT_VARS))
    assert result.exit_code != 0
    assert "Unable to determine platform" in result.output


def test_runner_repair_succeeds(monkeypatch):
    monkeypatch.setenv("SYNAPSE_WORKSPACE_NAME", "ws-test")
    monkeypatch.setenv("SYNAPSE_DEV_ENDPOINT", "https://dev.endpoint")
    monkeypatch.setattr("kindling_cli.cli._create_platform_api", lambda p: (_FakeRunnerAPI(), p))
    runner = CliRunner()
    result = runner.invoke(cli, ["runner", "repair", "--platform", "synapse"])
    assert result.exit_code == 0, result.output
    assert "runner-001" in result.output


def test_runner_repair_requires_platform_or_env():
    runner = CliRunner()
    result = runner.invoke(cli, ["runner", "repair"], env=dict(_BLANK_PLATFORM_DETECT_VARS))
    assert result.exit_code != 0
    assert "Unable to determine platform" in result.output


def test_runner_delete_prompts_when_yes_not_supplied(monkeypatch):
    monkeypatch.setattr("kindling_cli.cli._create_platform_api", lambda p: (_FakeRunnerAPI(), p))
    runner = CliRunner()
    # Supply 'n' to the confirmation prompt
    result = runner.invoke(cli, ["runner", "delete", "--platform", "fabric"], input="n\n")
    assert result.exit_code == 0, result.output
    assert "Aborted" in result.output


def test_runner_delete_skips_prompt_with_yes_flag(monkeypatch):
    monkeypatch.setattr("kindling_cli.cli._create_platform_api", lambda p: (_FakeRunnerAPI(), p))
    runner = CliRunner()
    result = runner.invoke(cli, ["runner", "delete", "--platform", "fabric", "--yes"])
    assert result.exit_code == 0, result.output
    assert "Deleted" in result.output


def test_runner_delete_confirms_and_proceeds(monkeypatch):
    monkeypatch.setattr("kindling_cli.cli._create_platform_api", lambda p: (_FakeRunnerAPI(), p))
    runner = CliRunner()
    # Supply 'y' to the confirmation prompt
    result = runner.invoke(cli, ["runner", "delete", "--platform", "fabric"], input="y\n")
    assert result.exit_code == 0, result.output
    assert "Deleted" in result.output


def test_runner_delete_json_output(monkeypatch):
    monkeypatch.setattr("kindling_cli.cli._create_platform_api", lambda p: (_FakeRunnerAPI(), p))
    runner = CliRunner()
    result = runner.invoke(cli, ["runner", "delete", "--platform", "synapse", "--yes", "--json"])
    assert result.exit_code == 0, result.output
    payload = json.loads(result.output)
    assert payload["deleted"] is True
    assert payload["platform"] == "synapse"


def test_runner_delete_requires_platform_or_env():
    runner = CliRunner()
    result = runner.invoke(
        cli, ["runner", "delete", "--yes"], env=dict(_BLANK_PLATFORM_DETECT_VARS)
    )
    assert result.exit_code != 0
    assert "Unable to determine platform" in result.output


def test_runner_invoke_runs_job(monkeypatch, tmp_path):
    monkeypatch.setattr("kindling_cli.cli._create_platform_api", lambda p: (_FakeRunnerAPI(), p))
    params_file = tmp_path / "params.yaml"
    params_file.write_text("runner_job_id: runner-001\napp_name: my-app\n", encoding="utf-8")
    runner = CliRunner()
    result = runner.invoke(
        cli,
        ["runner", "invoke", "--params", str(params_file), "--platform", "synapse", "--json"],
    )
    assert result.exit_code == 0, result.output
    payload = json.loads(result.output)
    assert payload["run_id"] == "run-999"
    assert payload["runner_job_id"] == "runner-001"


def test_runner_invoke_looks_up_runner_id_when_missing_from_params(monkeypatch, tmp_path):
    monkeypatch.setattr("kindling_cli.cli._create_platform_api", lambda p: (_FakeRunnerAPI(), p))
    params_file = tmp_path / "params.yaml"
    params_file.write_text("app_name: my-app\n", encoding="utf-8")
    runner = CliRunner()
    result = runner.invoke(
        cli,
        ["runner", "invoke", "--params", str(params_file), "--platform", "fabric", "--json"],
    )
    assert result.exit_code == 0, result.output
    payload = json.loads(result.output)
    assert payload["run_id"] == "run-999"
    # runner_job_id resolved from status
    assert payload["runner_job_id"] == "runner-001"


def test_runner_invoke_rejects_non_positive_poll_interval(monkeypatch, tmp_path):
    monkeypatch.setattr("kindling_cli.cli._create_platform_api", lambda p: (_FakeRunnerAPI(), p))
    params_file = tmp_path / "params.yaml"
    params_file.write_text("runner_job_id: runner-001\n", encoding="utf-8")
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "runner",
            "invoke",
            "--params",
            str(params_file),
            "--platform",
            "synapse",
            "--wait",
            "--poll-interval",
            "0",
        ],
    )
    assert result.exit_code != 0
    assert "poll-interval" in result.output.lower()


def test_runner_invoke_requires_platform_or_env(tmp_path):
    params_file = tmp_path / "params.yaml"
    params_file.write_text("runner_job_id: runner-001\n", encoding="utf-8")
    runner = CliRunner()
    result = runner.invoke(
        cli,
        ["runner", "invoke", "--params", str(params_file)],
        env=dict(_BLANK_PLATFORM_DETECT_VARS),
    )
    assert result.exit_code != 0
    assert "Unable to determine platform" in result.output


# ---------------------------------------------------------------------------
# Standalone app run — exit code / success semantics
# ---------------------------------------------------------------------------


def _make_execution_result(all_succeeded: bool, failed_pipe_ids=None):
    """Build a minimal ExecutionResult stand-in for CLI tests."""
    from unittest.mock import MagicMock

    result = MagicMock()
    result.all_succeeded = all_succeeded
    result.failed_count = 0 if all_succeeded else len(failed_pipe_ids or [])
    result.failed_pipes = list(failed_pipe_ids or [])
    return result


def _patch_standalone_run(monkeypatch, tmp_path, execution_result):
    """
    Patch subprocess.run for _run_standalone_app so the test doesn't
    need a real Spark/kindling install.

    Returns the app_dir path the CLI should be invoked with.
    execution_result.all_succeeded controls the subprocess returncode.
    """
    import subprocess

    app_dir = tmp_path / "myapp"
    app_dir.mkdir()
    (app_dir / "app.py").write_text("# stub\n")

    returncode = 0 if execution_result.all_succeeded else 1

    def fake_run(cmd, env=None, **kwargs):
        return subprocess.CompletedProcess(cmd, returncode=returncode)

    monkeypatch.setattr(subprocess, "run", fake_run)

    return app_dir


def test_standalone_run_exits_0_on_success(monkeypatch, tmp_path):
    """Exit code 0 when all pipes succeed."""
    app_dir = _patch_standalone_run(monkeypatch, tmp_path, _make_execution_result(True))
    result = CliRunner().invoke(cli, ["app", "run", str(app_dir)])
    assert result.exit_code == 0


def test_standalone_run_exits_1_when_pipe_fails(monkeypatch, tmp_path):
    """Exit code 1 when any pipe fails (fail_on_error default=True)."""
    app_dir = _patch_standalone_run(
        monkeypatch, tmp_path, _make_execution_result(False, ["pipe_a"])
    )
    result = CliRunner().invoke(cli, ["app", "run", str(app_dir)])
    assert result.exit_code == 1


def test_standalone_run_exits_0_with_no_fail_on_error_even_when_pipe_fails(monkeypatch, tmp_path):
    """--no-fail-on-error: exit 0 even when a pipe fails."""
    app_dir = _patch_standalone_run(
        monkeypatch, tmp_path, _make_execution_result(False, ["pipe_a"])
    )
    result = CliRunner().invoke(cli, ["app", "run", "--no-fail-on-error", str(app_dir)])
    assert result.exit_code == 0


def test_app_add_only_scaffolds_executor():
    result = CliRunner().invoke(cli, ["app", "add", "--help"])

    assert result.exit_code == 0
    assert "executor" in result.output
    assert " entity" not in result.output
    assert " pipe" not in result.output
    assert "ingestion" not in result.output

    executor_help = CliRunner().invoke(cli, ["app", "add", "executor", "--help"])
    assert executor_help.exit_code == 0
    assert "--pattern [batch|structured-streaming|file-ingestion]" in executor_help.output


def test_package_add_owns_entity_pipe_ingestion_scaffolds():
    result = CliRunner().invoke(cli, ["package", "add", "--help"])

    assert result.exit_code == 0
    assert "entity" in result.output
    assert "pipe" in result.output
    assert "ingestion" in result.output


def test_app_add_executor_creates_entrypoint_and_app_yaml(tmp_path):
    app_dir = tmp_path / "apps" / "sales_ops"
    app_dir.mkdir(parents=True)
    (app_dir / "app.py").write_text(
        "\n".join(
            [
                "def register_all():",
                "    import sales.entities.records",
                "    import sales.pipes.bronze_to_silver",
                "",
            ]
        ),
        encoding="utf-8",
    )

    result = CliRunner().invoke(cli, ["app", "add", "executor", "--app", str(app_dir)])

    assert result.exit_code == 0, result.output
    executor = app_dir / "main.py"
    assert executor.exists()
    content = executor.read_text(encoding="utf-8")
    assert (
        "REGISTRATION_MODULES = ['sales.entities.records', " "'sales.pipes.bronze_to_silver']"
    ) in content
    assert "get_kindling_service(DataPipesExecution)" in content
    assert "run_datapipes(pipe_ids, use_dag=USE_DAG)" in content
    assert "_add_local_package_paths()" not in content

    app_config = yaml.safe_load((app_dir / "app.yaml").read_text(encoding="utf-8"))
    assert app_config["name"] == "sales_ops"
    assert app_config["entry_point"] == "main.py"
    app_settings = yaml.safe_load(
        (app_dir / "config" / "settings.yaml").read_text(encoding="utf-8")
    )
    assert app_settings["kindling"]["extensions"] == ["sales"]


def test_app_add_executor_auto_discovers_app_directory_from_repo_root(tmp_path, monkeypatch):
    app_dir = tmp_path / "apps" / "sales_ops"
    app_dir.mkdir(parents=True)
    package_dir = tmp_path / "packages" / "sales"
    package_dir.mkdir(parents=True)
    (package_dir / "pyproject.toml").write_text(
        "\n".join(
            [
                "[tool.poetry]",
                'name = "sales-domain"',
                'version = "1.2.3"',
                "",
            ]
        ),
        encoding="utf-8",
    )
    (app_dir / "app.py").write_text(
        "\n".join(
            [
                "def register_all():",
                "    import sales.entities.records",
                "    import sales.pipes.bronze_to_silver",
                "",
            ]
        ),
        encoding="utf-8",
    )

    monkeypatch.chdir(tmp_path)
    result = CliRunner().invoke(cli, ["app", "add", "executor"])

    assert result.exit_code == 0, result.output
    assert (app_dir / "main.py").exists()
    assert (app_dir / "app.yaml").exists()
    assert not (tmp_path / "main.py").exists()
    assert not (tmp_path / "app.yaml").exists()
    app_settings = yaml.safe_load(
        (app_dir / "config" / "settings.yaml").read_text(encoding="utf-8")
    )
    assert app_settings["kindling"]["extensions"] == ["sales-domain==1.2.3"]


def test_app_add_executor_supports_structured_streaming_pattern(tmp_path):
    app_dir = tmp_path / "apps" / "streaming_app"
    app_dir.mkdir(parents=True)
    (app_dir / "app.py").write_text(
        "def initialize(env=None):\n    return None\n", encoding="utf-8"
    )

    result = CliRunner().invoke(
        cli,
        [
            "app",
            "add",
            "executor",
            "--app",
            str(app_dir),
            "--pattern",
            "structured-streaming",
            "--module",
            "sales.entities.records",
            "--module",
            "sales.pipes.bronze_to_silver",
            "--pipe",
            "bronze_to_silver",
            "--checkpoint-path",
            "abfss://checkpoints/app",
        ],
    )

    assert result.exit_code == 0, result.output
    content = (app_dir / "main.py").read_text(encoding="utf-8")
    assert "from kindling.execution_orchestrator import ExecutionOrchestrator" in content
    assert "PIPE_IDS = ['bronze_to_silver']" in content
    assert 'streaming_options["base_checkpoint_path"] = CHECKPOINT_PATH' in content
    assert "orchestrator.execute_streaming(" in content


def test_app_add_executor_supports_file_ingestion_pattern(tmp_path):
    app_dir = tmp_path / "apps" / "ingestion_app"
    app_dir.mkdir(parents=True)
    (app_dir / "app.py").write_text(
        "def initialize(env=None):\n    return None\n", encoding="utf-8"
    )

    result = CliRunner().invoke(
        cli,
        [
            "app",
            "add",
            "executor",
            "--app",
            str(app_dir),
            "--pattern",
            "file-ingestion",
            "--module",
            "sales.ingestion.orders",
            "--source-path",
            "abfss://landing/orders",
        ],
    )

    assert result.exit_code == 0, result.output
    content = (app_dir / "main.py").read_text(encoding="utf-8")
    assert "from kindling.file_ingestion import FileIngestionProcessor" in content
    assert "SOURCE_PATH = 'abfss://landing/orders'" in content
    assert 'os.environ.get("KINDLING_INGESTION_PATH")' in content
    assert "processor.process_path(source_path)" in content


def test_package_add_entity_uses_package_option(tmp_path):
    package_dir = tmp_path / "packages" / "sales" / "src" / "sales"
    package_dir.mkdir(parents=True)

    result = CliRunner().invoke(
        cli,
        ["package", "add", "entity", "bronze.orders", "--package", str(package_dir)],
    )

    assert result.exit_code == 0, result.output
    assert (package_dir / "entities.py").exists()
    assert (package_dir / "tests" / "entities" / "bronze" / "orders.csv").exists()

    old_result = CliRunner().invoke(
        cli,
        ["app", "add", "entity", "bronze.orders", "--app", str(package_dir)],
    )
    assert old_result.exit_code != 0
