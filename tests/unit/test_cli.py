import base64
import json
import zipfile
from pathlib import Path

import yaml
from click.testing import CliRunner
from kindling_cli.cli import (
    _build_fabric_notebook_payload,
    _find_wheels,
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

        result = runner.invoke(cli, ["app", "package", str(app_dir)])

        assert result.exit_code == 0, result.output
        package_path = Path("dist/demo_app.kda")
        assert package_path.exists()
        with zipfile.ZipFile(package_path, "r") as archive:
            assert sorted(archive.namelist()) == ["app.py", "nested/settings.yaml"]


def test_app_package_json_output_is_machine_readable():
    runner = CliRunner()
    with runner.isolated_filesystem():
        app_dir = Path("demo_app")
        app_dir.mkdir()
        (app_dir / "app.py").write_text("print('hello')\n", encoding="utf-8")

        result = runner.invoke(cli, ["app", "package", str(app_dir), "--json"])

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

        result = runner.invoke(cli, ["app", "deploy", str(package_path), "--platform", "fabric"])

        assert result.exit_code != 0
        assert "unsafe relative path traversal" in result.output


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

        result = runner.invoke(cli, ["app", "deploy", str(app_dir), "--platform", "fabric"])

        assert result.exit_code == 0, result.output
        assert fake_api.calls[0][0] == "demo_app"
        assert sorted(fake_api.calls[0][1]) == ["app.py", "pipelines/job.yml"]
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
            cli, ["app", "deploy", str(app_dir), "--platform", "fabric", "--json"]
        )

        assert result.exit_code == 0, result.output
        payload = json.loads(result.output)
        assert payload["app_name"] == "demo_app"
        assert payload["platform"] == "fabric"
        assert payload["storage_path"].startswith("abfss://")


def test_job_create_loads_yaml_and_prints_structured_result(monkeypatch):
    class FakeAPI:
        def create_job(self, job_name, job_config):
            assert job_name == "nightly-demo"
            assert job_config["schedule"] == "0 0 * * *"
            return {"job_id": "job-123", "job_name": job_name}

    monkeypatch.setattr(
        "kindling_cli.cli._create_platform_api",
        lambda platform: (FakeAPI(), platform),
    )

    runner = CliRunner()
    with runner.isolated_filesystem():
        Path("job.yaml").write_text(
            "job_name: nightly-demo\nschedule: '0 0 * * *'\n",
            encoding="utf-8",
        )

        result = runner.invoke(cli, ["job", "create", "job.yaml", "--platform", "synapse"])

        assert result.exit_code == 0, result.output
        payload = json.loads(result.output)
        assert payload["job_id"] == "job-123"
        assert payload["job_name"] == "nightly-demo"


def test_job_run_json_output_includes_run_id(monkeypatch):
    class FakeAPI:
        def run_job(self, job_id, parameters=None):
            assert job_id == "job-123"
            assert parameters is None
            return "run-456"

    monkeypatch.setattr(
        "kindling_cli.cli._create_platform_api",
        lambda platform: (FakeAPI(), platform),
    )

    runner = CliRunner()
    result = runner.invoke(cli, ["job", "run", "job-123", "--platform", "synapse", "--json"])

    assert result.exit_code == 0, result.output
    payload = json.loads(result.output)
    assert payload["job_id"] == "job-123"
    assert payload["run_id"] == "run-456"
    assert payload["waited"] is False


def test_job_run_wait_fails_on_failed_terminal_state(monkeypatch):
    class FakeAPI:
        def run_job(self, job_id, parameters=None):
            return "run-456"

        def get_job_status(self, run_id):
            return {"status": "FAILED"}

    monkeypatch.setattr(
        "kindling_cli.cli._create_platform_api",
        lambda platform: (FakeAPI(), platform),
    )

    runner = CliRunner()
    result = runner.invoke(
        cli,
        ["job", "run", "job-123", "--platform", "synapse", "--wait", "--json"],
    )

    assert result.exit_code != 0
    payload = json.loads(result.output)
    assert payload["run_id"] == "run-456"
    assert payload["state"] == "FAILED"
    assert payload["succeeded"] is False


def test_job_run_wait_succeeds_on_completed_terminal_state(monkeypatch):
    class FakeAPI:
        def run_job(self, job_id, parameters=None):
            return "run-456"

        def get_job_status(self, run_id):
            return {"status": "COMPLETED"}

    monkeypatch.setattr(
        "kindling_cli.cli._create_platform_api",
        lambda platform: (FakeAPI(), platform),
    )

    runner = CliRunner()
    result = runner.invoke(
        cli,
        ["job", "run", "job-123", "--platform", "synapse", "--wait", "--json"],
    )

    assert result.exit_code == 0, result.output
    payload = json.loads(result.output)
    assert payload["state"] == "COMPLETED"
    assert payload["succeeded"] is True


def test_job_run_wait_handles_databricks_terminated_success(monkeypatch):
    class FakeAPI:
        def run_job(self, job_id, parameters=None):
            return "run-456"

        def get_job_status(self, run_id):
            return {"status": "TERMINATED", "result_state": "SUCCESS"}

    monkeypatch.setattr(
        "kindling_cli.cli._create_platform_api",
        lambda platform: (FakeAPI(), platform),
    )

    runner = CliRunner()
    result = runner.invoke(
        cli,
        ["job", "run", "job-123", "--platform", "databricks", "--wait", "--json"],
    )

    assert result.exit_code == 0, result.output
    payload = json.loads(result.output)
    assert payload["state"] == "SUCCESS"
    assert payload["succeeded"] is True


def test_job_run_wait_handles_nested_lifecycle_result_state(monkeypatch):
    class FakeAPI:
        def run_job(self, job_id, parameters=None):
            return "run-456"

        def get_job_status(self, run_id):
            return {"state": {"life_cycle_state": "TERMINATED", "result_state": "FAILED"}}

    monkeypatch.setattr(
        "kindling_cli.cli._create_platform_api",
        lambda platform: (FakeAPI(), platform),
    )

    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "job",
            "run",
            "job-123",
            "--platform",
            "databricks",
            "--wait",
            "--json",
            "--no-fail-on-error",
        ],
    )

    assert result.exit_code == 0, result.output
    payload = json.loads(result.output)
    assert payload["state"] == "FAILED"
    assert payload["succeeded"] is False


def test_job_run_wait_handles_fabric_succeeded_status(monkeypatch):
    class FakeAPI:
        def run_job(self, job_id, parameters=None):
            return "run-456"

        def get_job_status(self, run_id):
            return {"status": "Succeeded"}

    monkeypatch.setattr(
        "kindling_cli.cli._create_platform_api",
        lambda platform: (FakeAPI(), platform),
    )

    runner = CliRunner()
    result = runner.invoke(
        cli,
        ["job", "run", "job-123", "--platform", "fabric", "--wait", "--json"],
    )

    assert result.exit_code == 0, result.output
    payload = json.loads(result.output)
    assert payload["state"] == "SUCCEEDED"
    assert payload["succeeded"] is True


def test_job_run_wait_rejects_non_positive_poll_interval(monkeypatch):
    class FakeAPI:
        def run_job(self, job_id, parameters=None):
            return "run-456"

    monkeypatch.setattr(
        "kindling_cli.cli._create_platform_api",
        lambda platform: (FakeAPI(), platform),
    )

    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "job",
            "run",
            "job-123",
            "--platform",
            "synapse",
            "--wait",
            "--poll-interval",
            "0",
        ],
    )

    assert result.exit_code != 0
    assert "--poll-interval must be greater than 0" in result.output


def test_job_run_wait_rejects_non_positive_timeout(monkeypatch):
    class FakeAPI:
        def run_job(self, job_id, parameters=None):
            return "run-456"

    monkeypatch.setattr(
        "kindling_cli.cli._create_platform_api",
        lambda platform: (FakeAPI(), platform),
    )

    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "job",
            "run",
            "job-123",
            "--platform",
            "synapse",
            "--wait",
            "--timeout",
            "0",
        ],
    )

    assert result.exit_code != 0
    assert "--timeout must be greater than 0" in result.output


def test_job_logs_stream_requires_job_id():
    runner = CliRunner()
    result = runner.invoke(cli, ["job", "logs", "run-123", "--stream", "--platform", "fabric"])

    assert result.exit_code != 0
    assert "--job-id is required when --stream is used." in result.output


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
