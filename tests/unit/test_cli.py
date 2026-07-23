import base64
import json
import os
import sys
import types
import zipfile
from pathlib import Path

import click
import pytest
import yaml
from click.testing import CliRunner
from kindling_cli.cli import (
    _find_wheels,
    _generate_bootstrap_notebook,
    _generate_sample_notebook,
    _install_global_wheels,
    _load_app_module,
    _parse_abfss_uri,
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
        settings_path = Path("settings.yaml")
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


def test_env_update_refreshes_local_kindling_index(monkeypatch, tmp_path):
    package_dir = tmp_path / "kindling-packages"

    monkeypatch.setattr("kindling_cli.cli._resolve_github_version", lambda version, repo: "1.2.3")

    def fake_download(version, temp_dir, repo):
        assert version == "1.2.3"
        (temp_dir / "spark_kindling-1.2.3-py3-none-any.whl").write_bytes(b"runtime")
        (temp_dir / "spark_kindling_cli-1.2.3-py3-none-any.whl").write_bytes(b"cli")
        (temp_dir / "spark_kindling_sdk-1.2.3-py3-none-any.whl").write_bytes(b"sdk")

    monkeypatch.setattr("kindling_cli.cli._download_github_release_assets", fake_download)

    result = CliRunner().invoke(
        cli,
        [
            "env",
            "update",
            "--version",
            "1.2.3",
            "--package-dir",
            str(package_dir),
            "--no-global",
            "--no-project",
        ],
    )

    assert result.exit_code == 0, result.output
    assert (package_dir / "wheels" / "spark_kindling-1.2.3-py3-none-any.whl").exists()
    root_index = (package_dir / "simple" / "index.html").read_text(encoding="utf-8")
    assert "spark-kindling" in root_index
    package_index = (package_dir / "simple" / "spark-kindling-cli" / "index.html").read_text(
        encoding="utf-8"
    )
    assert "spark_kindling_cli-1.2.3-py3-none-any.whl" in package_index


def test_env_update_can_update_current_poetry_project(monkeypatch, tmp_path):
    package_dir = tmp_path / "kindling-packages"
    project_dir = tmp_path / "project"
    project_dir.mkdir()
    (project_dir / "pyproject.toml").write_text("[tool.poetry]\nname = 'demo'\n", encoding="utf-8")
    calls = {}

    monkeypatch.setattr("kindling_cli.cli._resolve_github_version", lambda version, repo: "1.2.3")
    monkeypatch.setattr("kindling_cli.cli._install_global_wheels", lambda wheels, use_sudo: None)

    def fake_download(version, temp_dir, repo):
        (temp_dir / "spark_kindling-1.2.3-py3-none-any.whl").write_bytes(b"runtime")

    def fake_project_update(path, sync):
        calls["path"] = path
        calls["sync"] = sync

    monkeypatch.setattr("kindling_cli.cli._download_github_release_assets", fake_download)
    monkeypatch.setattr("kindling_cli.cli._update_kindling_project", fake_project_update)

    result = CliRunner().invoke(
        cli,
        [
            "env",
            "update",
            "--package-dir",
            str(package_dir),
            "--project",
            str(project_dir),
            "--no-sync",
        ],
    )

    assert result.exit_code == 0, result.output
    assert calls == {"path": project_dir.resolve(), "sync": False}


def test_env_update_global_install_preserves_standalone_extra(monkeypatch, tmp_path):
    wheel = tmp_path / "spark_kindling-1.2.3-py3-none-any.whl"
    wheel.write_bytes(b"runtime")
    cli_wheel = tmp_path / "spark_kindling_cli-1.2.3-py3-none-any.whl"
    cli_wheel.write_bytes(b"cli")
    commands = []

    def fake_run(command, check):
        commands.append(command)

    monkeypatch.setattr("kindling_cli.cli.subprocess.run", fake_run)

    _install_global_wheels([wheel, cli_wheel], use_sudo=False)

    assert f"{wheel}[standalone]" in commands[0]
    assert str(cli_wheel) in commands[0]


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
            path = Path("settings.fabric.yaml")
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
            path = Path("settings.prod.yaml")
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
            path = Path("data-apps/ingest-app/settings.databricks.yaml")
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


def test_fabric_ipynb_definition_contains_encoded_notebook_content():
    from kindling_sdk.notebooks import FabricNotebookClient

    notebook_data = {"nbformat": 4, "cells": [{"cell_type": "code", "source": ["print('hi')\n"]}]}

    definition = FabricNotebookClient._ipynb_definition(notebook_data)

    assert definition["format"] == "ipynb"
    parts = definition["parts"]
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
    assert '"extensions": ["kindling-ext-otel-azure>=0.3.0"]' in source


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
        (app_dir / "lake-reqs.txt").write_text("domain-records==1.2.3\n", encoding="utf-8")
        (app_dir / "nested" / "settings.yaml").write_text("name: demo\n", encoding="utf-8")

        result = runner.invoke(cli, ["app", "package", "demo_app", "--local-folder", str(app_dir)])

        assert result.exit_code == 0, result.output
        package_path = Path("dist/demo_app.kda")
        assert package_path.exists()
        with zipfile.ZipFile(package_path, "r") as archive:
            assert sorted(archive.namelist()) == [
                "app.py",
                "lake-reqs.txt",
                "nested/settings.yaml",
            ]


def test_app_package_includes_only_selected_settings_overlays():
    runner = CliRunner()
    with runner.isolated_filesystem():
        app_dir = Path("demo_app")
        app_dir.mkdir()
        (app_dir / "app.py").write_text("print('hello')\n", encoding="utf-8")
        (app_dir / "app.yaml").write_text("name: demo\n", encoding="utf-8")
        (app_dir / "app.fabric.yaml").write_text("legacy: true\n", encoding="utf-8")
        (app_dir / "settings.yaml").write_text("name: demo\n", encoding="utf-8")
        (app_dir / "settings.fabric.yaml").write_text("platform: fabric\n", encoding="utf-8")
        (app_dir / "settings.synapse.yaml").write_text("platform: synapse\n", encoding="utf-8")
        (app_dir / "settings.prod.yaml").write_text("env: prod\n", encoding="utf-8")
        (app_dir / "settings.dev.yaml").write_text("env: dev\n", encoding="utf-8")
        (app_dir / "settings.local.yaml").write_text("local: true\n", encoding="utf-8")

        result = runner.invoke(
            cli,
            [
                "app",
                "package",
                "demo_app",
                "--local-folder",
                str(app_dir),
                "--platform",
                "fabric",
                "--env",
                "prod",
            ],
        )

        assert result.exit_code == 0, result.output
        with zipfile.ZipFile(Path("dist/demo_app.kda"), "r") as archive:
            assert sorted(archive.namelist()) == [
                "app.py",
                "app.yaml",
                "settings.fabric.yaml",
                "settings.prod.yaml",
                "settings.yaml",
            ]


def test_app_package_convention_lookup():
    runner = CliRunner()
    with runner.isolated_filesystem():
        app_dir = Path("apps/demo_app")
        app_dir.mkdir(parents=True)
        (app_dir / "app.py").write_text("print('hello')\n", encoding="utf-8")

        result = runner.invoke(cli, ["app", "package", "demo-app"], catch_exceptions=False)

        assert result.exit_code == 0, result.output
        assert Path("dist/demo_app.kda").exists()


def test_app_package_convention_lookup_from_subdirectory():
    """Convention lookup walks up from cwd so commands work inside subdirectories."""
    import os

    runner = CliRunner()
    with runner.isolated_filesystem():
        app_dir = Path("apps/demo_app").resolve()
        app_dir.mkdir(parents=True)
        (app_dir / "app.py").write_text("print('hello')\n", encoding="utf-8")

        orig = os.getcwd()
        os.chdir(app_dir)
        try:
            result = runner.invoke(cli, ["app", "package", "demo-app"], catch_exceptions=False)
            assert result.exit_code == 0, result.output
        finally:
            os.chdir(orig)


def test_app_package_json_output_is_machine_readable():
    runner = CliRunner()
    with runner.isolated_filesystem():
        app_dir = Path("demo_app")
        app_dir.mkdir()
        (app_dir / "app.py").write_text("print('hello')\n", encoding="utf-8")

        result = runner.invoke(
            cli, ["app", "package", "demo_app", "--local-folder", str(app_dir), "--json"]
        )

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
            [
                "app",
                "deploy",
                "demo_app",
                "--kda-package",
                str(package_path),
                "--platform",
                "fabric",
            ],
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
                "demo_app",
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


def test_app_deploy_requires_app_name():
    runner = CliRunner()
    result = runner.invoke(cli, ["app", "deploy", "--platform", "fabric"])
    assert result.exit_code != 0
    assert "Missing argument 'APP_NAME'" in result.output


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
            [
                "app",
                "deploy",
                "demo_app",
                "--local-folder",
                str(app_dir),
                "--platform",
                "fabric",
            ],
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
        (app_dir / "lake-reqs.txt").write_text("domain-records==1.2.3\n", encoding="utf-8")
        (app_dir / "pipelines" / "job.yml").write_text("job_name: demo\n", encoding="utf-8")

        result = runner.invoke(
            cli,
            ["app", "deploy", "demo_app", "--local-folder", str(app_dir), "--platform", "fabric"],
        )

        assert result.exit_code == 0, result.output
        assert fake_api.calls[0][0] == "demo_app"
        assert sorted(fake_api.calls[0][1]) == [
            "app.py",
            "lake-reqs.txt",
            "pipelines/job.yml",
        ]
        assert "Deployed app `demo_app`" in result.output


def test_app_deploy_rejects_missing_configured_entry_point(monkeypatch):
    monkeypatch.setattr(
        "kindling_cli.cli._create_platform_api",
        lambda platform: (object(), platform),
    )

    runner = CliRunner()
    with runner.isolated_filesystem():
        app_dir = Path("apps/sample_engine")
        app_dir.mkdir(parents=True)
        (app_dir / "app.py").write_text("print('hello')\n", encoding="utf-8")
        (app_dir / "app.yaml").write_text(
            "name: sample_engine\nentry_point: main.py\n",
            encoding="utf-8",
        )

        result = runner.invoke(
            cli,
            [
                "app",
                "deploy",
                "sample_engine",
                "--local-folder",
                str(app_dir),
                "--platform",
                "fabric",
            ],
        )

        assert result.exit_code != 0
        assert "entry_point `main.py`" in result.output
        assert "main.py" in result.output


def test_app_deploy_kda_package_flag(monkeypatch):
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
        kda = Path("demo_app.kda")
        with zipfile.ZipFile(kda, "w") as archive:
            archive.writestr("app.py", "print('hello')\n")
            archive.writestr("lake-reqs.txt", "domain-records==1.2.3\n")

        result = runner.invoke(
            cli,
            ["app", "deploy", "demo_app", "--kda-package", str(kda), "--platform", "fabric"],
        )

        assert result.exit_code == 0, result.output
        assert fake_api.calls[0][1]["lake-reqs.txt"] == "domain-records==1.2.3\n"
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
            [
                "app",
                "deploy",
                "demo_app",
                "--local-folder",
                str(app_dir),
                "--platform",
                "fabric",
                "--json",
            ],
        )

        assert result.exit_code == 0, result.output
        payload = json.loads(result.output)
        assert payload["app_name"] == "demo_app"
        assert payload["platform"] == "fabric"
        assert payload["storage_path"].startswith("abfss://")


def test_package_deploy_builds_wheel_and_uploads_to_artifacts_packages(
    monkeypatch,
):
    calls = {}

    def fake_run(cmd, cwd=None, capture_output=False, text=False):
        calls["build_cmd"] = cmd
        calls["build_cwd"] = cwd
        dist = Path(cwd) / "dist"
        dist.mkdir(exist_ok=True)
        (dist / "domain_records-1.2.3-py3-none-any.whl").write_bytes(b"wheel")
        return types.SimpleNamespace(returncode=0, stdout="", stderr="")

    def fake_deploy_wheels(blob_service_client, container, packages_path, wheels):
        calls["blob_service_client"] = blob_service_client
        calls["container"] = container
        calls["packages_path"] = packages_path
        calls["wheels"] = wheels
        return len(wheels)

    monkeypatch.setattr("kindling_cli.cli.subprocess.run", fake_run)
    monkeypatch.setattr(
        "kindling_cli.cli._get_blob_service_client",
        lambda account: "blob-client",
    )
    monkeypatch.setattr("kindling_cli.cli._deploy_wheels", fake_deploy_wheels)

    runner = CliRunner()
    with runner.isolated_filesystem():
        package_dir = Path("packages/domain_records")
        package_dir.mkdir(parents=True)
        (package_dir / "pyproject.toml").write_text(
            """
[tool.poetry]
name = "domain-records"
version = "1.2.3"
""",
            encoding="utf-8",
        )

        result = runner.invoke(
            cli,
            [
                "package",
                "deploy",
                "domain-records",
                "--local-folder",
                str(package_dir),
                "--storage-account",
                "acct",
                "--container",
                "artifacts",
                "--base-path",
                "dev",
            ],
        )

        assert result.exit_code == 0, result.output
        assert calls["build_cmd"] == [
            "poetry",
            "build",
            "--format",
            "wheel",
            "--output",
            str(package_dir.resolve() / "dist"),
        ]
        assert calls["build_cwd"] == package_dir.resolve()
        assert calls["container"] == "artifacts"
        assert calls["packages_path"] == "dev/packages"
        assert [wheel.name for wheel in calls["wheels"]] == [
            "domain_records-1.2.3-py3-none-any.whl"
        ]
        assert "Deployed `domain_records-1.2.3-py3-none-any.whl`" in result.output


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


def test_app_run_remote_submits_without_deploying(monkeypatch):
    class FakeAPI:
        def __init__(self):
            self.deployed = []
            self.submitted = []

        def deploy_app(self, app_name, app_files):
            self.deployed.append((app_name, app_files))
            return "abfss://artifacts@acct.dfs.core.windows.net/dev/data-apps/orders"

        def submit_app_run(self, app_name, environment=None, parameters=None):
            self.submitted.append((app_name, environment, parameters))
            return "run-1"

    fake_api = FakeAPI()
    monkeypatch.setattr(
        "kindling_cli.cli._create_platform_api",
        lambda platform: (fake_api, platform),
    )

    runner = CliRunner()
    result = runner.invoke(
        cli,
        ["app", "run", "orders", "--platform", "fabric", "--no-wait"],
    )

    assert result.exit_code == 0, result.output
    assert "run-1" in result.output
    assert len(fake_api.deployed) == 0, "deploy_app must NOT be called for remote runs"
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


def test_app_cleanup_rejects_local_folder_flag():
    runner = CliRunner()
    with runner.isolated_filesystem():
        app_dir = Path("orders")
        app_dir.mkdir()
        result = runner.invoke(
            cli,
            ["app", "cleanup", "orders", "--local-folder", str(app_dir), "--platform", "fabric"],
        )
        assert result.exit_code != 0
        assert "No such option" in result.output or "no such option" in result.output.lower()


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
            ],
        )

    assert result.exit_code != 0
    assert "Use --skip-config or --allow-missing-config" in result.output


# ---------------------------------------------------------------------------
# workspace init
# ---------------------------------------------------------------------------


class TestWorkspaceInit:
    # --- Config deployment path ---

    def test_deploys_config_when_settings_yaml_exists(self, monkeypatch):
        monkeypatch.setattr("kindling_cli.cli._get_blob_service_client", lambda account: object())
        deployed = []
        monkeypatch.setattr(
            "kindling_cli.cli._deploy_config",
            lambda client, container, base, config, overwrite=False, **kwargs: deployed.append(
                config
            ),
        )
        runner = CliRunner()
        with runner.isolated_filesystem():
            Path("settings.yaml").write_text("kindling:\n  name: test\n")
            result = runner.invoke(
                cli, ["workspace", "init", "--platform", "fabric", "--storage-account", "acct"]
            )
        assert result.exit_code == 0, result.output
        assert len(deployed) == 1

    def test_errors_with_config_init_hint_when_settings_yaml_missing(self, monkeypatch):
        monkeypatch.setattr("kindling_cli.cli._get_blob_service_client", lambda account: object())
        runner = CliRunner()
        with runner.isolated_filesystem():
            result = runner.invoke(
                cli, ["workspace", "init", "--platform", "fabric", "--storage-account", "acct"]
            )
        assert result.exit_code != 0
        assert "kindling config init" in result.output

    def test_allow_missing_config_is_not_an_option(self):
        result = CliRunner().invoke(
            cli,
            [
                "workspace",
                "init",
                "--allow-missing-config",
                "--platform",
                "fabric",
                "--storage-account",
                "acct",
            ],
        )
        assert result.exit_code != 0
        assert "no such option" in result.output.lower()

    def test_overwrite_passes_to_deploy_config(self, monkeypatch):
        monkeypatch.setattr("kindling_cli.cli._get_blob_service_client", lambda account: object())
        calls = []
        monkeypatch.setattr(
            "kindling_cli.cli._deploy_config",
            lambda client, container, base, config, overwrite=False, **kwargs: calls.append(
                overwrite
            ),
        )
        runner = CliRunner()
        with runner.isolated_filesystem():
            Path("settings.yaml").write_text("kindling:\n  name: test\n")
            runner.invoke(
                cli,
                [
                    "workspace",
                    "init",
                    "--platform",
                    "fabric",
                    "--storage-account",
                    "acct",
                    "--overwrite",
                ],
            )
        assert calls == [True]

    def test_platform_auto_detected_from_env(self, monkeypatch):
        monkeypatch.setenv("FABRIC_WORKSPACE_ID", "ws-123")
        monkeypatch.setattr("kindling_cli.cli._get_blob_service_client", lambda account: object())
        monkeypatch.setattr("kindling_cli.cli._deploy_config", lambda *a, **kw: None)
        runner = CliRunner()
        with runner.isolated_filesystem():
            Path("settings.yaml").write_text("kindling:\n  name: test\n")
            result = runner.invoke(cli, ["workspace", "init", "--storage-account", "acct"])
        assert result.exit_code == 0, result.output
        assert "fabric" in result.output

    # --- --notebook-bootstrap path ---

    def test_notebook_bootstrap_errors_without_workspace_or_env(self, monkeypatch):
        monkeypatch.setattr("kindling_cli.cli._get_blob_service_client", lambda account: object())
        monkeypatch.setattr("kindling_cli.cli._deploy_config", lambda *a, **kw: None)
        for var in ("DATABRICKS_HOST", "SYNAPSE_WORKSPACE_NAME", "FABRIC_WORKSPACE_ID"):
            monkeypatch.delenv(var, raising=False)
        runner = CliRunner()
        with runner.isolated_filesystem():
            Path("settings.yaml").write_text("kindling:\n  name: test\n")
            result = runner.invoke(
                cli,
                [
                    "workspace",
                    "init",
                    "--platform",
                    "fabric",
                    "--storage-account",
                    "acct",
                    "--notebook-bootstrap",
                ],
            )
        assert result.exit_code != 0
        assert "DATABRICKS_HOST" in result.output or "SYNAPSE_WORKSPACE_NAME" in result.output

    def test_notebook_bootstrap_auto_detects_workspace_from_env(self, monkeypatch):
        monkeypatch.setattr("kindling_cli.cli._get_blob_service_client", lambda account: object())
        monkeypatch.setattr("kindling_cli.cli._deploy_config", lambda *a, **kw: None)
        monkeypatch.setenv("DATABRICKS_HOST", "https://my-db.azuredatabricks.net")
        imported = []
        monkeypatch.setattr(
            "kindling_cli.cli._import_notebook_to_workspace",
            lambda platform, ws, name, nb: imported.append((ws, name)),
        )
        runner = CliRunner()
        with runner.isolated_filesystem():
            Path("settings.yaml").write_text("kindling:\n  name: test\n")
            result = runner.invoke(
                cli,
                [
                    "workspace",
                    "init",
                    "--platform",
                    "databricks",
                    "--storage-account",
                    "acct",
                    "--notebook-bootstrap",
                ],
            )
        assert result.exit_code == 0, result.output
        assert any("https://my-db.azuredatabricks.net" == ws for ws, _ in imported)

    def test_notebook_bootstrap_imports_both_notebooks(self, monkeypatch):
        monkeypatch.setattr("kindling_cli.cli._get_blob_service_client", lambda account: object())
        monkeypatch.setattr("kindling_cli.cli._deploy_config", lambda *a, **kw: None)
        imported = []
        monkeypatch.setattr(
            "kindling_cli.cli._import_notebook_to_workspace",
            lambda platform, ws, name, nb: imported.append(name),
        )
        runner = CliRunner()
        with runner.isolated_filesystem():
            Path("settings.yaml").write_text("kindling:\n  name: test\n")
            result = runner.invoke(
                cli,
                [
                    "workspace",
                    "init",
                    "--platform",
                    "fabric",
                    "--storage-account",
                    "acct",
                    "--notebook-bootstrap",
                    "--workspace",
                    "ws-id",
                ],
            )
        assert result.exit_code == 0, result.output
        assert "environment_bootstrap" in imported
        assert "kindling_hello_world" in imported

    def test_no_notebook_import_without_flag(self, monkeypatch):
        monkeypatch.setattr("kindling_cli.cli._get_blob_service_client", lambda account: object())
        monkeypatch.setattr("kindling_cli.cli._deploy_config", lambda *a, **kw: None)
        imported = []
        monkeypatch.setattr(
            "kindling_cli.cli._import_notebook_to_workspace",
            lambda platform, ws, name, nb: imported.append(name),
        )
        runner = CliRunner()
        with runner.isolated_filesystem():
            Path("settings.yaml").write_text("kindling:\n  name: test\n")
            result = runner.invoke(
                cli, ["workspace", "init", "--platform", "fabric", "--storage-account", "acct"]
            )
        assert result.exit_code == 0, result.output
        assert imported == []

    # --- General ---

    def test_missing_storage_account_errors(self, monkeypatch):
        monkeypatch.delenv("AZURE_STORAGE_ACCOUNT", raising=False)
        runner = CliRunner()
        with runner.isolated_filesystem():
            result = runner.invoke(cli, ["workspace", "init", "--platform", "fabric"])
        assert result.exit_code != 0
        assert "AZURE_STORAGE_ACCOUNT" in result.output

    def test_missing_platform_errors(self, monkeypatch):
        for var in ("FABRIC_WORKSPACE_ID", "SYNAPSE_WORKSPACE_NAME", "DATABRICKS_HOST"):
            monkeypatch.delenv(var, raising=False)
        runner = CliRunner()
        with runner.isolated_filesystem():
            result = runner.invoke(cli, ["workspace", "init", "--storage-account", "acct"])
        assert result.exit_code != 0
        assert "platform" in result.output.lower()

    def test_output_messages(self, monkeypatch):
        monkeypatch.setattr("kindling_cli.cli._get_blob_service_client", lambda account: object())
        monkeypatch.setattr("kindling_cli.cli._deploy_config", lambda *a, **kw: None)
        runner = CliRunner()
        with runner.isolated_filesystem():
            Path("settings.yaml").write_text("kindling:\n  name: test\n")
            result = runner.invoke(
                cli, ["workspace", "init", "--platform", "fabric", "--storage-account", "acct"]
            )
        assert result.exit_code == 0, result.output
        assert "Initializing workspace" in result.output
        assert "Config →" in result.output
        assert "Init complete." in result.output


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
        api.submit_app_run.return_value = "run-42"
        api.get_app_run_status.return_value = {"state": "SUCCEEDED"}
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
        result = runner.invoke(cli, ["app", "run", "myapp", "--local-folder", str(app_dir)])

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
        result = runner.invoke(
            cli, ["app", "run", "myapp", "--local-folder", str(app_dir), "--json"]
        )

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
        config_dir = tmp_path / "myconfig"
        config_dir.mkdir()
        (config_dir / "settings.yaml").write_text("kindling: {}\n")

        captured_cmd = {}

        def fake_run(cmd, env=None, **kwargs):
            captured_cmd["cmd"] = cmd
            captured_cmd["env"] = env or {}
            return subprocess.CompletedProcess(cmd, returncode=0)

        monkeypatch.setattr(subprocess, "run", fake_run)

        result = CliRunner().invoke(
            cli,
            [
                "app",
                "run",
                "myapp",
                "--local-folder",
                str(app_dir),
                "--env",
                "dev",
                "--config",
                str(config_dir),
                "--quiet",
            ],
        )

        assert result.exit_code == 0, result.output
        cmd = captured_cmd["cmd"]
        env = captured_cmd["env"]
        assert "-m" in cmd and "kindling_cli._runner" in cmd
        assert "--env" in cmd and "dev" in cmd
        assert str(config_dir / "settings.yaml") in cmd
        assert env["KINDLING_ENV"] == "dev"
        assert env["KINDLING_SPARK_ENABLE_DELTA"] == "true"
        assert env["KINDLING_LOG_LEVEL"] == "WARNING"
        assert "KINDLING_CONFIG_DIR" not in env

    def test_standalone_config_args_include_selected_platform_then_env(self, tmp_path, monkeypatch):
        import subprocess

        app_dir = tmp_path / "myapp"
        app_dir.mkdir()
        (app_dir / "app.py").write_text("# stub\n", encoding="utf-8")
        (app_dir / "settings.yaml").write_text("base: true\n", encoding="utf-8")
        (app_dir / "settings.fabric.yaml").write_text("platform: fabric\n", encoding="utf-8")
        (app_dir / "settings.dev.yaml").write_text("env: dev\n", encoding="utf-8")
        (app_dir / "settings.standalone.yaml").write_text(
            "platform: standalone\n", encoding="utf-8"
        )

        captured_cmd = []

        def fake_run(cmd, env=None, **kwargs):
            captured_cmd.extend(cmd)
            return subprocess.CompletedProcess(cmd, returncode=0)

        monkeypatch.setenv("KINDLING_PLATFORM", "fabric")
        monkeypatch.setattr(subprocess, "run", fake_run)

        result = CliRunner().invoke(
            cli,
            [
                "app",
                "run",
                "myapp",
                "--local-folder",
                str(app_dir),
                "--env",
                "dev",
            ],
        )

        assert result.exit_code == 0, result.output
        config_args = [
            captured_cmd[i + 1] for i, arg in enumerate(captured_cmd) if arg == "--config"
        ]
        assert config_args == [
            str(app_dir / "settings.yaml"),
            str(app_dir / "settings.fabric.yaml"),
            str(app_dir / "settings.dev.yaml"),
        ]

    def test_standalone_uses_kindling_runner_module(self, tmp_path, monkeypatch):
        import subprocess

        app_dir = tmp_path / "myapp"
        app_dir.mkdir()
        (app_dir / "app.py").write_text("# stub\n", encoding="utf-8")
        (app_dir / "settings.yaml").write_text("kindling: {}\n", encoding="utf-8")

        captured_cmd = []

        def fake_run(cmd, env=None, **kwargs):
            captured_cmd.extend(cmd)
            return subprocess.CompletedProcess(cmd, returncode=0)

        monkeypatch.setattr(subprocess, "run", fake_run)

        result = CliRunner().invoke(cli, ["app", "run", "myapp", "--local-folder", str(app_dir)])

        assert result.exit_code == 0, result.output
        assert "kindling_cli._runner" in captured_cmd
        assert str(app_dir / "settings.yaml") in captured_cmd
        assert "KINDLING_CONFIG_DIR" not in dict(zip(captured_cmd, captured_cmd))

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

        result = CliRunner().invoke(cli, ["app", "run", "myapp", "--local-folder", str(app_dir)])

        assert result.exit_code == 0, result.output
        assert captured_env["KINDLING_SPARK_ENABLE_DELTA"] == "false"

    def test_standalone_local_package_prepends_pythonpath(self, tmp_path, monkeypatch):
        import subprocess

        app_dir = tmp_path / "myapp"
        app_dir.mkdir()
        (app_dir / "app.py").write_text("# stub\n", encoding="utf-8")
        package_root = tmp_path / "packages" / "domain"
        package_src = package_root / "src"
        package_src.mkdir(parents=True)
        (package_src / "orders_domain").mkdir()
        (package_src / "orders_domain" / "__init__.py").write_text("", encoding="utf-8")
        direct_src = tmp_path / "shared_src"
        direct_src.mkdir()
        (direct_src / "shared_domain").mkdir()
        (direct_src / "shared_domain" / "__init__.py").write_text("", encoding="utf-8")
        captured_env = {}

        def fake_run(cmd, env=None, **kwargs):
            captured_env.update(env or {})
            return subprocess.CompletedProcess(cmd, returncode=0)

        monkeypatch.setenv("PYTHONPATH", "/existing/path")
        monkeypatch.setattr(subprocess, "run", fake_run)

        result = CliRunner().invoke(
            cli,
            [
                "app",
                "run",
                "myapp",
                "--local-folder",
                str(app_dir),
                "--local-package",
                str(package_root),
                "--local-package",
                str(direct_src),
            ],
        )

        assert result.exit_code == 0, result.output
        assert captured_env["PYTHONPATH"].split(os.pathsep) == [
            str(package_src.resolve()),
            str(direct_src.resolve()),
            "/existing/path",
        ]
        assert json.loads(captured_env["KINDLING_LOCAL_PACKAGE_MODULES"]) == [
            "orders_domain",
            "shared_domain",
        ]

    def test_standalone_local_package_accepts_direct_package_dir(self, tmp_path, monkeypatch):
        import subprocess

        app_dir = tmp_path / "myapp"
        app_dir.mkdir()
        (app_dir / "app.py").write_text("# stub\n", encoding="utf-8")
        package_dir = tmp_path / "packages" / "direct_domain"
        package_dir.mkdir(parents=True)
        (package_dir / "__init__.py").write_text("", encoding="utf-8")
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
                "myapp",
                "--local-folder",
                str(app_dir),
                "--local-package",
                str(package_dir),
            ],
        )

        assert result.exit_code == 0, result.output
        assert captured_env["PYTHONPATH"].split(os.pathsep)[0] == str(package_dir.parent)
        assert json.loads(captured_env["KINDLING_LOCAL_PACKAGE_MODULES"]) == ["direct_domain"]

    def test_remote_rejects_local_package_override(self, tmp_path):
        app_dir = tmp_path / "myapp"
        app_dir.mkdir()
        (app_dir / "app.py").write_text("# app", encoding="utf-8")
        package_root = tmp_path / "packages" / "domain"
        package_root.mkdir(parents=True)

        result = CliRunner().invoke(
            cli,
            [
                "app",
                "run",
                str(app_dir),
                "--platform",
                "synapse",
                "--local-package",
                str(package_root),
            ],
        )

        assert result.exit_code != 0
        assert "--local-package is only valid for standalone app runs" in result.output

    def test_standalone_rejects_remote_only_options(self, tmp_path):
        app_dir = tmp_path / "myapp"
        app_dir.mkdir()
        (app_dir / "app.py").write_text("def initialize(env=None):\n    return None\n")

        runner = CliRunner()
        result = runner.invoke(
            cli,
            ["app", "run", "myapp", "--local-folder", str(app_dir), "--no-wait"],
        )

        assert result.exit_code != 0
        assert "remote app runs" in result.output

    def test_submit_no_wait_returns_run_id(self, tmp_path, monkeypatch):
        import kindling_cli.cli as cli_mod

        mock_api = self._make_mock_api()
        monkeypatch.setattr(cli_mod, "_create_platform_api", lambda p: (mock_api, p))

        runner = CliRunner()
        result = runner.invoke(
            cli,
            ["app", "run", "myapp", "--platform", "synapse", "--no-wait"],
        )

        assert result.exit_code == 0, result.output
        assert "run-42" in result.output
        mock_api.deploy_app.assert_not_called()
        mock_api.submit_app_run.assert_called_once()

    def test_remote_json_keeps_stdout_machine_readable(self, tmp_path, monkeypatch):
        import kindling_cli.cli as cli_mod

        mock_api = self._make_mock_api()
        monkeypatch.setattr(cli_mod, "_create_platform_api", lambda p: (mock_api, p))

        result = CliRunner().invoke(
            cli,
            ["app", "run", "myapp", "--platform", "fabric", "--no-wait", "--json"],
        )

        assert result.exit_code == 0, result.output
        payload = json.loads(result.stdout)
        assert payload["run_id"] == "run-42"
        assert payload["platform"] == "fabric"
        assert "Submitting" not in result.stdout, "progress text must not appear in --json stdout"
        assert "Submitting" in result.stderr

    def test_standalone_load_lake_passes_flag_to_runner(self, tmp_path, monkeypatch):
        import subprocess

        app_dir = tmp_path / "myapp"
        app_dir.mkdir()
        (app_dir / "app.py").write_text("# stub\n", encoding="utf-8")

        captured_cmd = []

        def fake_run(cmd, env=None, **kwargs):
            captured_cmd.extend(cmd)
            return subprocess.CompletedProcess(cmd, returncode=0)

        monkeypatch.setattr(subprocess, "run", fake_run)

        result = CliRunner().invoke(
            cli, ["app", "run", "myapp", "--local-folder", str(app_dir), "--load-lake"]
        )

        assert result.exit_code == 0, result.output
        assert "--load-lake" in captured_cmd

    def test_standalone_no_load_lake_by_default(self, tmp_path, monkeypatch):
        import subprocess

        app_dir = tmp_path / "myapp"
        app_dir.mkdir()
        (app_dir / "app.py").write_text("# stub\n", encoding="utf-8")

        captured_cmd = []

        def fake_run(cmd, env=None, **kwargs):
            captured_cmd.extend(cmd)
            return subprocess.CompletedProcess(cmd, returncode=0)

        monkeypatch.setattr(subprocess, "run", fake_run)

        result = CliRunner().invoke(cli, ["app", "run", "myapp", "--local-folder", str(app_dir)])

        assert result.exit_code == 0, result.output
        assert "--load-lake" not in captured_cmd

    def test_load_lake_rejected_for_remote_runs(self, tmp_path, monkeypatch):
        import kindling_cli.cli as cli_mod

        mock_api = self._make_mock_api()
        monkeypatch.setattr(cli_mod, "_create_platform_api", lambda p: (mock_api, p))

        app_dir = tmp_path / "myapp"
        app_dir.mkdir()
        (app_dir / "app.py").write_text("# app")

        result = CliRunner().invoke(
            cli,
            ["app", "run", str(app_dir), "--platform", "fabric", "--load-lake"],
        )

        assert result.exit_code != 0
        assert "--load-lake is only valid for standalone app runs" in result.output

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
        assert "Config file not found at settings.yaml" in result.output
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
        Path("settings.yaml").write_text("name: test-app\n", encoding="utf-8")
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

    def register_app_job(self, app_name, config_overrides=None):
        return {"job_id": f"job-{app_name}", "job_name": app_name, "platform": "synapse"}

    def find_job_by_name(self, name):
        return f"job-{name}"

    def delete_job(self, job_id):
        return True

    def run_job(self, job_id, parameters=None):
        return "run-999"


def test_runner_help_lists_all_subcommands():
    runner = CliRunner()
    result = runner.invoke(cli, ["runner", "--help"])
    assert result.exit_code == 0, result.output
    for sub in ("register", "status", "delete", "invoke"):
        assert sub in result.output


def test_runner_register_single_app(monkeypatch):
    monkeypatch.setattr("kindling_cli.cli._create_platform_api", lambda p: (_FakeRunnerAPI(), p))
    runner = CliRunner()
    result = runner.invoke(cli, ["runner", "register", "--app", "my-app", "--platform", "synapse"])
    assert result.exit_code == 0, result.output
    assert "my-app" in result.output


def test_runner_register_with_config_overrides(monkeypatch):
    monkeypatch.setattr("kindling_cli.cli._create_platform_api", lambda p: (_FakeRunnerAPI(), p))
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "runner",
            "register",
            "--app",
            "my-app",
            "--platform",
            "synapse",
            "--config",
            "env=prod",
        ],
    )
    assert result.exit_code == 0, result.output


def test_runner_register_json_output(monkeypatch):
    monkeypatch.setattr("kindling_cli.cli._create_platform_api", lambda p: (_FakeRunnerAPI(), p))
    runner = CliRunner()
    result = runner.invoke(
        cli, ["runner", "register", "--app", "my-app", "--platform", "synapse", "--json"]
    )
    assert result.exit_code == 0, result.output
    payload = json.loads(result.output)
    assert payload["platform"] == "synapse"
    assert payload["job_id"] == "job-my-app"


def test_runner_register_fails_when_platform_vars_missing(monkeypatch):
    monkeypatch.setenv("DATABRICKS_HOST", "https://adb-123.azuredatabricks.net")
    monkeypatch.delenv("DATABRICKS_TOKEN", raising=False)
    runner = CliRunner()
    result = runner.invoke(
        cli, ["runner", "register", "--app", "my-app", "--platform", "databricks"]
    )
    assert result.exit_code != 0
    assert "Missing required environment variables" in result.output


def test_runner_status_summary(monkeypatch):
    monkeypatch.setattr("kindling_cli.cli._create_platform_api", lambda p: (_FakeRunnerAPI(), p))
    monkeypatch.setattr(
        "kindling_cli.cli._discover_local_app_names",
        lambda: [("my-app", None)],
    )
    runner = CliRunner()
    result = runner.invoke(cli, ["runner", "status", "--platform", "synapse"])
    assert result.exit_code == 0, result.output
    assert "my-app" in result.output
    assert "registered" in result.output


def test_runner_status_single_app(monkeypatch):
    monkeypatch.setattr("kindling_cli.cli._create_platform_api", lambda p: (_FakeRunnerAPI(), p))
    runner = CliRunner()
    result = runner.invoke(cli, ["runner", "status", "--app", "my-app", "--platform", "synapse"])
    assert result.exit_code == 0, result.output
    assert "my-app" in result.output


def test_runner_status_json(monkeypatch):
    monkeypatch.setattr("kindling_cli.cli._create_platform_api", lambda p: (_FakeRunnerAPI(), p))
    runner = CliRunner()
    result = runner.invoke(
        cli, ["runner", "status", "--app", "my-app", "--platform", "fabric", "--json"]
    )
    assert result.exit_code == 0, result.output
    payload = json.loads(result.output)
    assert payload["platform"] == "fabric"
    assert payload["apps"][0]["app_name"] == "my-app"
    assert payload["apps"][0]["registered"] is True


def test_runner_status_requires_platform_or_env():
    runner = CliRunner()
    result = runner.invoke(cli, ["runner", "status"], env=dict(_BLANK_PLATFORM_DETECT_VARS))
    assert result.exit_code != 0
    assert "Unable to determine platform" in result.output


def test_runner_delete_prompts_when_yes_not_supplied(monkeypatch):
    monkeypatch.setattr("kindling_cli.cli._create_platform_api", lambda p: (_FakeRunnerAPI(), p))
    runner = CliRunner()
    result = runner.invoke(
        cli, ["runner", "delete", "--app", "my-app", "--platform", "fabric"], input="n\n"
    )
    assert result.exit_code == 0, result.output
    assert "Aborted" in result.output


def test_runner_delete_skips_prompt_with_yes_flag(monkeypatch):
    monkeypatch.setattr("kindling_cli.cli._create_platform_api", lambda p: (_FakeRunnerAPI(), p))
    runner = CliRunner()
    result = runner.invoke(
        cli, ["runner", "delete", "--app", "my-app", "--platform", "fabric", "--yes"]
    )
    assert result.exit_code == 0, result.output
    assert "Deleted" in result.output


def test_runner_delete_confirms_and_proceeds(monkeypatch):
    monkeypatch.setattr("kindling_cli.cli._create_platform_api", lambda p: (_FakeRunnerAPI(), p))
    runner = CliRunner()
    result = runner.invoke(
        cli, ["runner", "delete", "--app", "my-app", "--platform", "fabric"], input="y\n"
    )
    assert result.exit_code == 0, result.output
    assert "Deleted" in result.output


def test_runner_delete_json_output(monkeypatch):
    monkeypatch.setattr("kindling_cli.cli._create_platform_api", lambda p: (_FakeRunnerAPI(), p))
    runner = CliRunner()
    result = runner.invoke(
        cli, ["runner", "delete", "--app", "my-app", "--platform", "synapse", "--yes", "--json"]
    )
    assert result.exit_code == 0, result.output
    payload = json.loads(result.output)
    assert payload["deleted"] is True
    assert payload["platform"] == "synapse"


def test_runner_delete_requires_platform_or_env():
    runner = CliRunner()
    result = runner.invoke(
        cli, ["runner", "delete", "--app", "my-app", "--yes"], env=dict(_BLANK_PLATFORM_DETECT_VARS)
    )
    assert result.exit_code != 0
    assert "Unable to determine platform" in result.output


def test_runner_delete_app_not_found(monkeypatch):
    class FakeAPINoJob:
        def find_job_by_name(self, name):
            return None

    monkeypatch.setattr("kindling_cli.cli._create_platform_api", lambda p: (FakeAPINoJob(), p))
    runner = CliRunner()
    result = runner.invoke(
        cli, ["runner", "delete", "--app", "missing-app", "--platform", "synapse", "--yes"]
    )
    assert result.exit_code != 0
    assert "missing-app" in result.output


def test_runner_register_requires_platform_or_env():
    runner = CliRunner()
    result = runner.invoke(
        cli, ["runner", "register", "--app", "my-app"], env=dict(_BLANK_PLATFORM_DETECT_VARS)
    )
    assert result.exit_code != 0
    assert "Unable to determine platform" in result.output


def test_runner_invoke_missing_job_id_and_app_name(monkeypatch, tmp_path):
    monkeypatch.setattr("kindling_cli.cli._create_platform_api", lambda p: (_FakeRunnerAPI(), p))
    params_file = tmp_path / "params.yaml"
    params_file.write_text("some_other_key: value\n", encoding="utf-8")
    runner = CliRunner()
    result = runner.invoke(
        cli, ["runner", "invoke", "--params", str(params_file), "--platform", "synapse"]
    )
    assert result.exit_code != 0
    assert "job_id" in result.output or "app_name" in result.output


def test_runner_invoke_runs_job(monkeypatch, tmp_path):
    monkeypatch.setattr("kindling_cli.cli._create_platform_api", lambda p: (_FakeRunnerAPI(), p))
    params_file = tmp_path / "params.yaml"
    params_file.write_text("job_id: job-my-app\n", encoding="utf-8")
    runner = CliRunner()
    result = runner.invoke(
        cli,
        ["runner", "invoke", "--params", str(params_file), "--platform", "synapse", "--json"],
    )
    assert result.exit_code == 0, result.output
    payload = json.loads(result.output)
    assert payload["run_id"] == "run-999"
    assert payload["job_id"] == "job-my-app"


def test_runner_invoke_looks_up_job_id_via_app_name(monkeypatch, tmp_path):
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
    assert payload["job_id"] == "job-my-app"


def test_runner_invoke_rejects_non_positive_poll_interval(monkeypatch, tmp_path):
    monkeypatch.setattr("kindling_cli.cli._create_platform_api", lambda p: (_FakeRunnerAPI(), p))
    params_file = tmp_path / "params.yaml"
    params_file.write_text("job_id: job-my-app\n", encoding="utf-8")
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
    params_file.write_text("job_id: job-my-app\n", encoding="utf-8")
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
    result = CliRunner().invoke(cli, ["app", "run", "myapp", "--local-folder", str(app_dir)])
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
    result = CliRunner().invoke(
        cli,
        ["app", "run", "--no-fail-on-error", "myapp", "--local-folder", str(app_dir)],
    )
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
    executor = app_dir / "app.py"
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
    assert app_config["entry_point"] == "app.py"
    app_settings = yaml.safe_load((app_dir / "settings.yaml").read_text(encoding="utf-8"))
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
    assert (app_dir / "app.py").exists()
    assert (app_dir / "app.yaml").exists()
    assert not (tmp_path / "app.py").exists()
    assert not (tmp_path / "app.yaml").exists()
    app_settings = yaml.safe_load((app_dir / "settings.yaml").read_text(encoding="utf-8"))
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
    content = (app_dir / "app.py").read_text(encoding="utf-8")
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
    content = (app_dir / "app.py").read_text(encoding="utf-8")
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


# ---------------------------------------------------------------------------
# runtime publish — _parse_abfss_uri unit tests
# ---------------------------------------------------------------------------


class TestParseAbfssUri:
    def test_valid_uri_returns_container_account_path(self):
        container, account, path = _parse_abfss_uri(
            "abfss://artifacts@mystorageacct.dfs.core.windows.net/kindling"
        )
        assert container == "artifacts"
        assert account == "mystorageacct"
        assert path == "kindling"

    def test_valid_uri_no_path(self):
        container, account, path = _parse_abfss_uri(
            "abfss://artifacts@mystorageacct.dfs.core.windows.net"
        )
        assert container == "artifacts"
        assert account == "mystorageacct"
        assert path == ""

    def test_valid_uri_nested_path(self):
        container, account, path = _parse_abfss_uri(
            "abfss://artifacts@acct.dfs.core.windows.net/a/b/c"
        )
        assert path == "a/b/c"

    def test_strips_leading_slashes_from_path(self):
        _, _, path = _parse_abfss_uri("abfss://artifacts@acct.dfs.core.windows.net/kindling")
        assert not path.startswith("/")

    def test_missing_abfss_scheme_raises(self):
        with pytest.raises(click.ClickException, match="Invalid destination URI"):
            _parse_abfss_uri("https://acct.blob.core.windows.net/container")

    def test_missing_at_separator_raises(self):
        with pytest.raises(click.ClickException, match="Missing '@' separator"):
            _parse_abfss_uri("abfss://artifacts.dfs.core.windows.net/path")

    def test_empty_container_raises(self):
        with pytest.raises(click.ClickException, match="Container name is empty"):
            _parse_abfss_uri("abfss://@acct.dfs.core.windows.net/path")

    def test_host_without_dot_raises(self):
        with pytest.raises(click.ClickException, match="does not contain a domain suffix"):
            _parse_abfss_uri("abfss://container@accountnodot/path")


# ---------------------------------------------------------------------------
# runtime publish — CLI command tests
# ---------------------------------------------------------------------------


def _make_fake_blob_service_client(blobs_by_prefix=None):
    """Build a minimal fake BlobServiceClient for publish tests."""
    from unittest.mock import MagicMock

    blobs_by_prefix = blobs_by_prefix or {}
    uploaded = {}

    def fake_get_container_client(container):
        cc = MagicMock()

        def list_blobs(name_starts_with=""):
            result = []
            for prefix, names in blobs_by_prefix.items():
                if name_starts_with.rstrip("/") == prefix.rstrip("/") or name_starts_with == "":
                    for name in names:
                        b = MagicMock()
                        b.name = name
                        result.append(b)
            return result

        cc.list_blobs.side_effect = list_blobs
        return cc

    def fake_get_blob_client(container, blob):
        bc = MagicMock()

        def download():
            dl = MagicMock()
            dl.readall.return_value = b"wheel-content"
            return dl

        bc.download_blob.side_effect = download
        bc.get_blob_properties.side_effect = Exception("not found")

        def upload(data, overwrite=False):
            uploaded[blob] = data

        bc.upload_blob.side_effect = upload
        return bc

    client = MagicMock()
    client.get_container_client.side_effect = fake_get_container_client
    client.get_blob_client.side_effect = fake_get_blob_client
    client._uploaded = uploaded
    return client


class TestRuntimeDeploy:
    def test_deploy_help_shows_source_and_dest(self):
        result = CliRunner().invoke(cli, ["runtime", "deploy", "--help"])
        assert result.exit_code == 0, result.output
        assert "--source" in result.output
        assert "--dest" in result.output

    def test_deploy_requires_source(self):
        result = CliRunner().invoke(
            cli, ["runtime", "deploy", "--dest", "abfss://a@acct.dfs.core.windows.net/k"]
        )
        assert result.exit_code != 0

    def test_deploy_requires_dest(self):
        result = CliRunner().invoke(cli, ["runtime", "deploy", "--source", "github:latest"])
        assert result.exit_code != 0

    def test_deploy_invalid_dest_uri_fails(self, tmp_path, monkeypatch):
        monkeypatch.setattr(
            "kindling_cli.cli._get_blob_service_client",
            lambda account: _make_fake_blob_service_client(),
        )
        result = CliRunner().invoke(
            cli,
            [
                "runtime",
                "deploy",
                "--source",
                "local:" + str(tmp_path),
                "--dest",
                "not-an-abfss-uri",
            ],
        )
        assert result.exit_code != 0
        assert "Invalid destination URI" in result.output

    def test_deploy_unrecognized_source_fails(self, monkeypatch):
        monkeypatch.setattr(
            "kindling_cli.cli._get_blob_service_client",
            lambda account: _make_fake_blob_service_client(),
        )
        result = CliRunner().invoke(
            cli,
            [
                "runtime",
                "deploy",
                "--source",
                "ftp://someserver/path",
                "--dest",
                "abfss://artifacts@acct.dfs.core.windows.net/k",
            ],
        )
        assert result.exit_code != 0
        assert "Unrecognized source specifier" in result.output

    def test_deploy_local_source_uploads_wheels(self, tmp_path, monkeypatch):
        wheel = tmp_path / "spark_kindling-0.9.25-py3-none-any.whl"
        wheel.write_bytes(b"fake-wheel")

        fake_client = _make_fake_blob_service_client()
        monkeypatch.setattr(
            "kindling_cli.cli._get_blob_service_client",
            lambda account: fake_client,
        )

        result = CliRunner().invoke(
            cli,
            [
                "runtime",
                "deploy",
                "--source",
                f"local:{tmp_path}",
                "--dest",
                "abfss://artifacts@myacct.dfs.core.windows.net/kindling",
            ],
        )

        assert result.exit_code == 0, result.output
        assert "Deploy complete." in result.output
        assert "spark_kindling-0.9.25-py3-none-any.whl" in result.output
        uploaded_blobs = list(fake_client._uploaded.keys())
        assert any("packages/" in b for b in uploaded_blobs)

    def test_deploy_local_source_missing_dir_fails(self, monkeypatch):
        monkeypatch.setattr(
            "kindling_cli.cli._get_blob_service_client",
            lambda account: _make_fake_blob_service_client(),
        )
        result = CliRunner().invoke(
            cli,
            [
                "runtime",
                "deploy",
                "--source",
                "local:/no/such/dir/at/all",
                "--dest",
                "abfss://artifacts@acct.dfs.core.windows.net/k",
            ],
        )
        assert result.exit_code != 0
        assert "not found" in result.output

    def test_deploy_local_source_no_wheels_fails(self, tmp_path, monkeypatch):
        monkeypatch.setattr(
            "kindling_cli.cli._get_blob_service_client",
            lambda account: _make_fake_blob_service_client(),
        )
        (tmp_path / "README.txt").write_text("no wheels here")
        result = CliRunner().invoke(
            cli,
            [
                "runtime",
                "deploy",
                "--source",
                f"local:{tmp_path}",
                "--dest",
                "abfss://artifacts@acct.dfs.core.windows.net/k",
            ],
        )
        assert result.exit_code != 0
        assert "No wheel files" in result.output

    def test_deploy_adls_source_copies_packages(self, monkeypatch):
        src_prefix = "staging/packages"
        blob_name = "staging/packages/spark_kindling-0.9.0-py3-none-any.whl"
        fake_src = _make_fake_blob_service_client(blobs_by_prefix={src_prefix: [blob_name]})
        fake_dest = _make_fake_blob_service_client()

        call_order = []

        def fake_get_client(account):
            call_order.append(account)
            if account == "staging":
                return fake_src
            return fake_dest

        monkeypatch.setattr("kindling_cli.cli._get_blob_service_client", fake_get_client)

        result = CliRunner().invoke(
            cli,
            [
                "runtime",
                "deploy",
                "--source",
                "abfss://artifacts@staging.dfs.core.windows.net/staging",
                "--dest",
                "abfss://artifacts@prod.dfs.core.windows.net/kindling",
            ],
        )

        assert result.exit_code == 0, result.output
        assert "Deploy complete." in result.output

    def test_deploy_github_source_uses_public_kindling_release_without_gh(self, monkeypatch):
        import subprocess

        requested_api_paths = []
        downloaded_urls = []

        monkeypatch.setattr(
            "kindling_cli.cli._get_blob_service_client",
            lambda account: _make_fake_blob_service_client(),
        )

        def fake_github_api_json(path):
            requested_api_paths.append(path)
            if path == "/repos/sep/spark-kindling-framework/releases/latest":
                return {"tag_name": "v0.10.33"}
            if path == "/repos/sep/spark-kindling-framework/releases/tags/v0.10.33":
                return {
                    "assets": [
                        {
                            "name": "spark_kindling-0.10.33-py3-none-any.whl",
                            "browser_download_url": "https://example.test/runtime.whl",
                        },
                        {
                            "name": "kindling_bootstrap.py",
                            "browser_download_url": "https://example.test/bootstrap.py",
                        },
                    ]
                }
            raise AssertionError(f"Unexpected GitHub API path: {path}")

        def fake_download_url(url, dest):
            downloaded_urls.append(url)
            dest.write_bytes(b"fake asset")

        def fake_run(cmd, **kwargs):
            raise AssertionError(f"Unexpected subprocess call: {cmd}")

        monkeypatch.setattr("kindling_cli.cli._github_api_json", fake_github_api_json)
        monkeypatch.setattr("kindling_cli.cli._download_url", fake_download_url)
        monkeypatch.setattr(subprocess, "run", fake_run)

        result = CliRunner().invoke(
            cli,
            [
                "runtime",
                "deploy",
                "--source",
                "github:latest",
                "--dest",
                "abfss://artifacts@acct.dfs.core.windows.net/k",
            ],
        )

        assert result.exit_code == 0, result.output
        assert requested_api_paths == [
            "/repos/sep/spark-kindling-framework/releases/latest",
            "/repos/sep/spark-kindling-framework/releases/tags/v0.10.33",
        ]
        assert downloaded_urls == [
            "https://example.test/runtime.whl",
            "https://example.test/bootstrap.py",
        ]
        assert "Publishing from GitHub release v0.10.33" in result.output
        assert "Deploy complete." in result.output

    def test_deploy_skip_bootstrap_omits_scripts(self, tmp_path, monkeypatch):
        wheel = tmp_path / "spark_kindling-0.9.25-py3-none-any.whl"
        wheel.write_bytes(b"fake-wheel")
        (tmp_path / "runtime" / "scripts").mkdir(parents=True)
        (tmp_path / "runtime" / "scripts" / "kindling_bootstrap.py").write_text("# bootstrap")

        fake_client = _make_fake_blob_service_client()
        monkeypatch.setattr(
            "kindling_cli.cli._get_blob_service_client",
            lambda account: fake_client,
        )

        result = CliRunner().invoke(
            cli,
            [
                "runtime",
                "deploy",
                "--source",
                f"local:{tmp_path}",
                "--dest",
                "abfss://artifacts@myacct.dfs.core.windows.net/kindling",
                "--skip-bootstrap",
            ],
        )

        assert result.exit_code == 0, result.output
        uploaded_blobs = list(fake_client._uploaded.keys())
        assert not any("scripts/" in b for b in uploaded_blobs)

    def test_runtime_group_in_help(self):
        result = CliRunner().invoke(cli, ["--help"])
        assert result.exit_code == 0
        assert "runtime" in result.output


# ---------------------------------------------------------------------------
# migrate command group
# ---------------------------------------------------------------------------


def _make_fake_migrate_modules(
    monkeypatch, *, has_changes=False, has_destructive=False, has_errors=False
):
    """Wire up fake kindling.migration and kindling.injection for migrate tests."""
    import types

    fake_plan = types.SimpleNamespace(
        has_changes=has_changes,
        has_destructive_changes=has_destructive,
        errors=[types.SimpleNamespace(error="inspect failed")] if has_errors else [],
        print_summary=lambda: None,
    )

    fake_svc = types.SimpleNamespace(
        plan=lambda: fake_plan,
        apply=lambda plan, allow_destructive=False, backup=None: None,
        rollback=lambda entity: None,
        cleanup=lambda entity: None,
    )

    fake_entity = types.SimpleNamespace(entityid="silver.dim_customer")

    fake_registry = types.SimpleNamespace(
        get_entity_definition=lambda eid: fake_entity if eid == "silver.dim_customer" else None
    )

    class FakeInjector:
        _map = {}

        @classmethod
        def get(cls, svc_type):
            return cls._map.get(svc_type)

    # Use distinct sentinel classes so the two _map entries don't collide.
    class _FakeMigrationService:
        pass

    class _FakeDataEntityRegistry:
        pass

    fake_migration = types.ModuleType("kindling.migration")
    fake_migration.MigrationService = _FakeMigrationService
    fake_migration.BackupStrategy = types.SimpleNamespace(NONE="none", SNAPSHOT="snapshot")
    FakeInjector._map[_FakeMigrationService] = fake_svc

    fake_entities = types.ModuleType("kindling.data_entities")
    fake_entities.DataEntityRegistry = _FakeDataEntityRegistry
    FakeInjector._map[_FakeDataEntityRegistry] = fake_registry

    fake_injection = types.ModuleType("kindling.injection")
    fake_injection.GlobalInjector = FakeInjector

    monkeypatch.setitem(sys.modules, "kindling.migration", fake_migration)
    monkeypatch.setitem(sys.modules, "kindling.data_entities", fake_entities)
    monkeypatch.setitem(sys.modules, "kindling.injection", fake_injection)
    monkeypatch.setattr("kindling_cli.cli._load_app_module", lambda *a, **kw: None)
    monkeypatch.setattr("kindling_cli.cli._discover_app_py", lambda p: Path(p or "app.py"))

    return fake_svc, fake_plan, fake_entity


def test_migrate_plan_reports_up_to_date(tmp_path, monkeypatch):
    _make_fake_migrate_modules(monkeypatch, has_changes=False)
    result = CliRunner().invoke(cli, ["migrate", "plan", "--app", "app.py", "--env", "local"])
    assert result.exit_code == 0, result.output
    assert "up to date" in result.output


def test_migrate_plan_shows_pending_changes(tmp_path, monkeypatch):
    _make_fake_migrate_modules(monkeypatch, has_changes=True)
    result = CliRunner().invoke(cli, ["migrate", "plan", "--app", "app.py", "--env", "local"])
    assert result.exit_code == 0, result.output
    assert "Pending migrations" in result.output


def test_migrate_plan_warns_on_destructive(tmp_path, monkeypatch):
    _make_fake_migrate_modules(monkeypatch, has_changes=True, has_destructive=True)
    result = CliRunner().invoke(cli, ["migrate", "plan", "--app", "app.py"])
    assert result.exit_code == 0, result.output
    assert "--destructive" in result.output


def test_migrate_apply_up_to_date(tmp_path, monkeypatch):
    _make_fake_migrate_modules(monkeypatch, has_changes=False)
    result = CliRunner().invoke(cli, ["migrate", "apply", "--app", "app.py"])
    assert result.exit_code == 0, result.output
    assert "up to date" in result.output


def test_migrate_apply_with_changes(tmp_path, monkeypatch):
    _make_fake_migrate_modules(monkeypatch, has_changes=True)
    result = CliRunner().invoke(cli, ["migrate", "apply", "--app", "app.py"])
    assert result.exit_code == 0, result.output
    assert "Migration complete" in result.output


def test_migrate_plan_fails_on_errors_even_without_changes(tmp_path, monkeypatch):
    """An errors-only plan has no changes; it must not report 'up to date'."""
    _make_fake_migrate_modules(monkeypatch, has_changes=False, has_errors=True)
    result = CliRunner().invoke(cli, ["migrate", "plan", "--app", "app.py"])
    assert result.exit_code != 0
    assert "could not be inspected" in result.output
    assert "up to date" not in result.output


def test_migrate_apply_refuses_plan_with_errors(tmp_path, monkeypatch):
    fake_svc, _, _ = _make_fake_migrate_modules(monkeypatch, has_changes=True, has_errors=True)
    applied = []
    fake_svc.apply = lambda *a, **kw: applied.append(True)

    result = CliRunner().invoke(cli, ["migrate", "apply", "--app", "app.py"])

    assert result.exit_code != 0
    assert "Refusing to apply" in result.output
    assert not applied, "apply must not run when the plan has inspection errors"


def test_migrate_rollback_unknown_entity(tmp_path, monkeypatch):
    _make_fake_migrate_modules(monkeypatch)
    result = CliRunner().invoke(cli, ["migrate", "rollback", "bronze.unknown", "--app", "app.py"])
    assert result.exit_code != 0
    assert "not found" in result.output


def test_migrate_rollback_known_entity(tmp_path, monkeypatch):
    _make_fake_migrate_modules(monkeypatch)
    result = CliRunner().invoke(
        cli, ["migrate", "rollback", "silver.dim_customer", "--app", "app.py"]
    )
    assert result.exit_code == 0, result.output
    assert "Rolled back" in result.output


def test_migrate_cleanup_known_entity(tmp_path, monkeypatch):
    _make_fake_migrate_modules(monkeypatch)
    result = CliRunner().invoke(
        cli, ["migrate", "cleanup", "silver.dim_customer", "--app", "app.py"]
    )
    assert result.exit_code == 0, result.output
    assert "Cleanup complete" in result.output


def test_migrate_plan_accepts_env_and_config_flags(tmp_path, monkeypatch):
    _make_fake_migrate_modules(monkeypatch, has_changes=False)
    result = CliRunner().invoke(
        cli,
        ["migrate", "plan", "--app", "app.py", "--env", "prod", "--config", str(tmp_path)],
    )
    assert result.exit_code == 0, result.output


def test_deploy_config_uploads_only_selected_overlays(tmp_path):
    from unittest.mock import MagicMock

    from kindling_cli.cli import _deploy_config

    config_dir = tmp_path / "config"
    config_dir.mkdir()
    for filename in [
        "settings.yaml",
        "settings.fabric.yaml",
        "settings.synapse.yaml",
        "settings.prod.yaml",
        "settings.dev.yaml",
        "settings.local.yaml",
    ]:
        (config_dir / filename).write_text(f"name: {filename}\n", encoding="utf-8")

    uploaded = {}

    def get_blob_client(container, blob):
        blob_client = MagicMock()

        def upload_blob(data, overwrite=True):
            uploaded[blob] = data

        blob_client.upload_blob.side_effect = upload_blob
        return blob_client

    blob_service_client = MagicMock()
    blob_service_client.get_blob_client.side_effect = get_blob_client

    _deploy_config(
        blob_service_client,
        "artifacts",
        "base",
        config_dir / "settings.yaml",
        platform="fabric",
        environment="prod",
    )

    assert sorted(uploaded) == [
        "base/config/settings.fabric.yaml",
        "base/config/settings.prod.yaml",
        "base/config/settings.yaml",
    ]


# env ensure + _detect_cloud
class TestEnvEnsure:
    """Tests for env ensure command and cloud auto-detection."""

    def test_detect_cloud_returns_azure_when_az_on_path(self, monkeypatch):
        import kindling_cli.cli as cli_module
        from kindling_cli.cli import _detect_cloud

        monkeypatch.setattr(
            cli_module.shutil, "which", lambda cmd: "/usr/bin/az" if cmd == "az" else None
        )
        assert _detect_cloud() == "azure"

    def test_detect_cloud_returns_none_when_no_cli_found(self, monkeypatch):
        import kindling_cli.cli as cli_module
        from kindling_cli.cli import _detect_cloud

        monkeypatch.setattr(cli_module.shutil, "which", lambda _: None)
        assert _detect_cloud() is None

    def _fake_jar_specs(self):
        return [("fake.jar", "http://example.com/fake.jar", None)]

    def test_env_ensure_explicit_cloud_azure_prints_help_text(self, monkeypatch, tmp_path):
        import urllib.request

        import kindling_cli.cli as cli_module

        monkeypatch.setattr(cli_module.shutil, "which", lambda _: None)  # no az on PATH
        monkeypatch.setattr(cli_module, "_HADOOP_JAR_DIR", tmp_path / "hadoop-jars")
        monkeypatch.setattr(cli_module, "_azure_jar_specs", self._fake_jar_specs)

        def fake_urlretrieve(url, dest):
            Path(dest).write_bytes(b"fake-jar")

        monkeypatch.setattr(urllib.request, "urlretrieve", fake_urlretrieve)

        result = CliRunner().invoke(cli, ["env", "ensure", "--cloud", "azure"])

        assert result.exit_code == 0
        assert "All JARs present" in result.output

    def test_env_ensure_no_cloud_detected_exits_zero_with_message(self, monkeypatch):
        import kindling_cli.cli as cli_module

        monkeypatch.setattr(cli_module.shutil, "which", lambda _: None)
        result = CliRunner().invoke(cli, ["env", "ensure"])

        assert result.exit_code == 0
        assert "No cloud provider detected" in result.output

    def test_env_ensure_auto_detects_azure_when_az_on_path(self, monkeypatch, tmp_path):
        import urllib.request

        import kindling_cli.cli as cli_module

        monkeypatch.setattr(
            cli_module.shutil, "which", lambda cmd: "/usr/bin/az" if cmd == "az" else None
        )
        monkeypatch.setattr(cli_module, "_HADOOP_JAR_DIR", tmp_path / "hadoop-jars")
        monkeypatch.setattr(cli_module, "_azure_jar_specs", self._fake_jar_specs)

        def fake_urlretrieve(url, dest):
            Path(dest).write_bytes(b"fake-jar")

        monkeypatch.setattr(urllib.request, "urlretrieve", fake_urlretrieve)

        result = CliRunner().invoke(cli, ["env", "ensure"])

        assert result.exit_code == 0
        assert "Detected cloud: azure" in result.output
        assert "All JARs present" in result.output

    def test_env_ensure_help_mentions_cloud_flag(self):
        result = CliRunner().invoke(cli, ["env", "ensure", "--help"])
        assert result.exit_code == 0
        assert "--cloud" in result.output


# ---------------------------------------------------------------------------
# notebook command group (round-tripping)
# ---------------------------------------------------------------------------


class _StubNotebookClient:
    def __init__(self, names=None, cells=None):
        self.names = names or []
        self.cells = cells if cells is not None else []
        self.imported = []

    def list_notebooks(self):
        return self.names

    def get_notebook_cells(self, name):
        return self.cells

    def import_notebook(self, name, notebook_data):
        self.imported.append((name, notebook_data))

    def delete_notebook(self, name):
        pass


class TestNotebookCommands:
    def _clear_platform_env(self, monkeypatch):
        for var in ("DATABRICKS_HOST", "SYNAPSE_WORKSPACE_NAME", "FABRIC_WORKSPACE_ID"):
            monkeypatch.delenv(var, raising=False)

    def _install_stub(self, monkeypatch, stub):
        captured = []
        monkeypatch.setattr(
            "kindling_cli.cli._notebook_client",
            lambda platform, workspace, folder: captured.append((platform, workspace, folder))
            or stub,
        )
        return captured

    def test_list_prints_workspace_notebooks(self, monkeypatch):
        self._clear_platform_env(monkeypatch)
        monkeypatch.setenv("DATABRICKS_HOST", "https://db.example.net")
        captured = self._install_stub(
            monkeypatch, _StubNotebookClient(names=["etl_orders", "etl_customers"])
        )
        result = CliRunner().invoke(cli, ["notebook", "list"])
        assert result.exit_code == 0, result.output
        assert "etl_orders" in result.output
        assert "etl_customers" in result.output
        assert captured == [("databricks", "https://db.example.net", "/Shared/kindling")]

    def test_commands_error_without_platform(self, monkeypatch):
        self._clear_platform_env(monkeypatch)
        result = CliRunner().invoke(cli, ["notebook", "list"])
        assert result.exit_code != 0
        assert "platform" in result.output.lower()

    def test_workspace_env_var_must_match_platform(self, monkeypatch):
        """--platform fabric must not silently use DATABRICKS_HOST as workspace."""
        self._clear_platform_env(monkeypatch)
        monkeypatch.setenv("DATABRICKS_HOST", "https://db.example.net")
        result = CliRunner().invoke(cli, ["notebook", "list", "--platform", "fabric"])
        assert result.exit_code != 0
        assert "FABRIC_WORKSPACE_ID" in result.output

    def test_pull_writes_source_file(self, monkeypatch):
        self._clear_platform_env(monkeypatch)
        monkeypatch.setenv("DATABRICKS_HOST", "https://db.example.net")
        cells = [
            {"cell_type": "markdown", "metadata": {}, "source": ["# Title"]},
            {"cell_type": "code", "metadata": {}, "outputs": [], "source": ["x = 1"]},
        ]
        self._install_stub(monkeypatch, _StubNotebookClient(cells=cells))
        runner = CliRunner()
        with runner.isolated_filesystem():
            result = runner.invoke(cli, ["notebook", "pull", "etl_orders", "--out", "nb"])
            assert result.exit_code == 0, result.output
            content = Path("nb/etl_orders.py").read_text(encoding="utf-8")
        assert content.startswith("# Databricks notebook source")
        assert "# MAGIC %md" in content
        assert "x = 1" in content

    def test_pull_refuses_existing_file_without_overwrite(self, monkeypatch):
        self._clear_platform_env(monkeypatch)
        monkeypatch.setenv("DATABRICKS_HOST", "https://db.example.net")
        self._install_stub(monkeypatch, _StubNotebookClient(cells=[]))
        runner = CliRunner()
        with runner.isolated_filesystem():
            Path("nb").mkdir()
            Path("nb/etl_orders.py").write_text("# local edits", encoding="utf-8")
            result = runner.invoke(cli, ["notebook", "pull", "etl_orders", "--out", "nb"])
            assert result.exit_code != 0
            assert "--overwrite" in result.output
            assert Path("nb/etl_orders.py").read_text(encoding="utf-8") == "# local edits"

    def test_pull_all_uses_workspace_listing(self, monkeypatch):
        self._clear_platform_env(monkeypatch)
        monkeypatch.setenv("DATABRICKS_HOST", "https://db.example.net")
        stub = _StubNotebookClient(
            names=["a", "b"],
            cells=[{"cell_type": "code", "metadata": {}, "outputs": [], "source": ["# x"]}],
        )
        self._install_stub(monkeypatch, stub)
        runner = CliRunner()
        with runner.isolated_filesystem():
            result = runner.invoke(cli, ["notebook", "pull", "--all", "--out", "nb"])
            assert result.exit_code == 0, result.output
            assert Path("nb/a.py").exists() and Path("nb/b.py").exists()
        assert "Pulled 2 notebook(s)" in result.output

    def test_push_imports_parsed_notebook(self, monkeypatch):
        self._clear_platform_env(monkeypatch)
        monkeypatch.setenv("DATABRICKS_HOST", "https://db.example.net")
        stub = _StubNotebookClient()
        captured = self._install_stub(monkeypatch, stub)
        runner = CliRunner()
        with runner.isolated_filesystem():
            Path("etl_orders.py").write_text(
                "# Databricks notebook source\n"
                "# MAGIC %md\n# MAGIC # Title\n\n"
                "# COMMAND ----------\n\nx = 1\n",
                encoding="utf-8",
            )
            result = runner.invoke(
                cli, ["notebook", "push", "etl_orders.py", "--folder", "/Shared/apps"]
            )
        assert result.exit_code == 0, result.output
        (imported,) = stub.imported
        name, nb = imported
        assert name == "etl_orders"
        assert [c["cell_type"] for c in nb["cells"]] == ["markdown", "code"]
        assert captured[-1] == ("databricks", "https://db.example.net", "/Shared/apps")

    def test_push_name_requires_single_file(self, monkeypatch):
        self._clear_platform_env(monkeypatch)
        monkeypatch.setenv("DATABRICKS_HOST", "https://db.example.net")
        runner = CliRunner()
        with runner.isolated_filesystem():
            Path("a.py").write_text("x = 1", encoding="utf-8")
            Path("b.py").write_text("y = 2", encoding="utf-8")
            result = runner.invoke(cli, ["notebook", "push", "a.py", "b.py", "--name", "combined"])
        assert result.exit_code != 0
        assert "exactly one file" in result.output

    def test_sdk_errors_become_cli_errors(self, monkeypatch):
        from kindling_sdk.notebooks import NotebookOperationError

        self._clear_platform_env(monkeypatch)
        monkeypatch.setenv("DATABRICKS_HOST", "https://db.example.net")

        class _FailingClient(_StubNotebookClient):
            def list_notebooks(self):
                raise NotebookOperationError("workspace list failed (403)")

        self._install_stub(monkeypatch, _FailingClient())
        result = CliRunner().invoke(cli, ["notebook", "list"])
        assert result.exit_code != 0
        assert "workspace list failed (403)" in result.output

    def test_pull_then_push_roundtrip(self, monkeypatch):
        """Pulled source pushed back parses to the same cells."""
        self._clear_platform_env(monkeypatch)
        monkeypatch.setenv("DATABRICKS_HOST", "https://db.example.net")
        cells = [
            {"cell_type": "markdown", "metadata": {}, "source": ["# Title\n", "\n", "Body"]},
            {"cell_type": "code", "metadata": {}, "outputs": [], "source": ["x = 1\n", "y = 2"]},
        ]
        stub = _StubNotebookClient(cells=cells)
        self._install_stub(monkeypatch, stub)
        runner = CliRunner()
        with runner.isolated_filesystem():
            result = runner.invoke(cli, ["notebook", "pull", "etl", "--out", "nb"])
            assert result.exit_code == 0, result.output
            result = runner.invoke(cli, ["notebook", "push", "nb/etl.py"])
            assert result.exit_code == 0, result.output

        ((_, nb),) = stub.imported
        roundtripped = [(c["cell_type"], "".join(c["source"])) for c in nb["cells"]]
        original = [(c["cell_type"], "".join(c["source"])) for c in cells]
        assert roundtripped == original
