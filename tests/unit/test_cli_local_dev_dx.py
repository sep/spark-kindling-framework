# [tester] cover CLI local dev DX behavior - TASK-20260430-001
import os
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

from click.testing import CliRunner
from kindling_cli.cli import _discover_app_py, cli

from kindling.data_entities import DataEntityRegistry
from kindling.data_pipes import DataPipesExecution, DataPipesRegistry


def _write_app(path: Path, body: str | None = None) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        body or "def initialize(env=None, config_dir=None):\n" "    return None\n",
        encoding="utf-8",
    )
    return path


def test_discover_app_py_finds_app_in_cwd():
    runner = CliRunner()
    with runner.isolated_filesystem():
        expected = _write_app(Path("app.py")).resolve()

        assert _discover_app_py(None) == expected


def test_discover_app_py_finds_single_level_src_package_app():
    runner = CliRunner()
    with runner.isolated_filesystem():
        expected = _write_app(Path("src/demo_app/app.py")).resolve()

        assert _discover_app_py(None) == expected


def test_discover_app_py_finds_src_package_app_from_tests_dir():
    runner = CliRunner()
    with runner.isolated_filesystem():
        expected = _write_app(Path("src/demo_app/app.py")).resolve()
        tests_dir = Path("tests")
        tests_dir.mkdir()
        original_cwd = Path.cwd()
        try:
            os.chdir(tests_dir)
            assert _discover_app_py(None) == expected
        finally:
            os.chdir(original_cwd)


def test_discover_app_py_uses_explicit_override():
    runner = CliRunner()
    with runner.isolated_filesystem():
        override = _write_app(Path("custom/app.py")).resolve()
        _write_app(Path("app.py"))

        assert _discover_app_py(Path("custom/app.py")) == override


def test_discover_app_py_missing_override_raises_clear_error():
    runner = CliRunner()
    with runner.isolated_filesystem():
        result = runner.invoke(cli, ["run", "pipe.one", "--app", "missing/app.py"])

        assert result.exit_code != 0
        assert "app.py not found at:" in result.output


def test_discover_app_py_missing_auto_discovery_raises_clear_error():
    runner = CliRunner()
    with runner.isolated_filesystem():
        result = runner.invoke(cli, ["run", "pipe.one"])

        assert result.exit_code != 0
        assert "Could not find app.py" in result.output
        assert "--app path/to/app.py" in result.output


def test_run_pipe_happy_path_calls_registered_executor(monkeypatch):
    runner = CliRunner()
    pipe_registry = Mock()
    pipe_registry.get_pipe_definition.return_value = SimpleNamespace(pipeid="bronze_to_silver")
    executor = Mock()

    def fake_get(service_type):
        if service_type is DataPipesRegistry:
            return pipe_registry
        if service_type is DataPipesExecution:
            return executor
        raise AssertionError(f"unexpected service: {service_type!r}")

    monkeypatch.setattr("kindling.injection.GlobalInjector.get", fake_get)

    with runner.isolated_filesystem():
        app_path = _write_app(
            Path("app.py"),
            "CAPTURED = []\n"
            "def initialize(env=None, config_dir=None):\n"
            "    CAPTURED.append((env, config_dir))\n",
        )

        result = runner.invoke(
            cli,
            ["run", "bronze_to_silver", "--env", "dev", "--app", str(app_path)],
        )

    assert result.exit_code == 0
    assert "Running pipe: bronze_to_silver" in result.output
    assert "completed successfully" in result.output
    pipe_registry.get_pipe_definition.assert_called_once_with("bronze_to_silver")
    executor.run_datapipes.assert_called_once_with(["bronze_to_silver"])


def test_run_pipe_unknown_pipe_lists_available_pipes(monkeypatch):
    runner = CliRunner()
    pipe_registry = Mock()
    pipe_registry.get_pipe_definition.return_value = None
    pipe_registry.get_pipe_ids.return_value = ["bronze_to_silver", "silver_to_gold"]

    def fake_get(service_type):
        if service_type is DataPipesRegistry:
            return pipe_registry
        raise AssertionError(f"unexpected service: {service_type!r}")

    monkeypatch.setattr("kindling.injection.GlobalInjector.get", fake_get)

    with runner.isolated_filesystem():
        app_path = _write_app(Path("app.py"))
        result = runner.invoke(cli, ["run", "missing_pipe", "--app", str(app_path)])

    assert result.exit_code != 0
    assert "Pipe 'missing_pipe' not found" in result.output
    assert "bronze_to_silver, silver_to_gold" in result.output


def test_validate_good_registry_checks_pass(monkeypatch):
    runner = CliRunner()
    entity_registry = Mock()
    entity_registry.get_entity_ids.return_value = ["bronze.records", "silver.records"]
    entity_registry.get_entity_definition.side_effect = lambda entity_id: {
        "bronze.records": SimpleNamespace(
            entityid="bronze.records",
            tags={"provider_type": "memory"},
            merge_columns=[],
        ),
        "silver.records": SimpleNamespace(
            entityid="silver.records",
            tags={"provider_type": "delta"},
            merge_columns=["id"],
        ),
    }[entity_id]
    pipe_registry = Mock()
    pipe_registry.get_pipe_ids.return_value = ["bronze_to_silver"]
    pipe_registry.get_pipe_definition.return_value = SimpleNamespace(
        pipeid="bronze_to_silver",
        input_entity_ids=["bronze.records"],
        output_entity_id="silver.records",
    )

    def fake_get(service_type):
        if service_type is DataEntityRegistry:
            return entity_registry
        if service_type is DataPipesRegistry:
            return pipe_registry
        raise AssertionError(f"unexpected service: {service_type!r}")

    monkeypatch.setattr("kindling.injection.GlobalInjector.get", fake_get)

    with runner.isolated_filesystem():
        app_path = _write_app(Path("app.py"))
        result = runner.invoke(cli, ["validate", "--app", str(app_path)])

    assert result.exit_code == 0
    assert "[PASS] entities_registered" in result.output
    assert "[PASS] pipes_registered" in result.output
    assert "[PASS] pipe.bronze_to_silver.input_entities: OK" in result.output
    assert "[PASS] pipe.bronze_to_silver.output_entity: OK" in result.output
    assert "[PASS] entity.silver.records.merge_columns: OK" in result.output
    assert "Validation passed." in result.output


def test_validate_bad_registry_checks_fail_with_missing_references(monkeypatch):
    runner = CliRunner()
    entity_registry = Mock()
    entity_registry.get_entity_ids.return_value = ["silver.records"]
    entity_registry.get_entity_definition.return_value = SimpleNamespace(
        entityid="silver.records",
        tags={"provider_type": "delta"},
        merge_columns=[],
    )
    pipe_registry = Mock()
    pipe_registry.get_pipe_ids.return_value = ["bronze_to_silver"]
    pipe_registry.get_pipe_definition.return_value = SimpleNamespace(
        pipeid="bronze_to_silver",
        input_entity_ids=["bronze.records"],
        output_entity_id="gold.records",
    )

    def fake_get(service_type):
        if service_type is DataEntityRegistry:
            return entity_registry
        if service_type is DataPipesRegistry:
            return pipe_registry
        raise AssertionError(f"unexpected service: {service_type!r}")

    monkeypatch.setattr("kindling.injection.GlobalInjector.get", fake_get)

    with runner.isolated_filesystem():
        app_path = _write_app(Path("app.py"))
        result = runner.invoke(cli, ["validate", "--app", str(app_path)])

    assert result.exit_code != 0
    assert "[FAIL] pipe.bronze_to_silver.input_entities: missing: bronze.records" in result.output
    assert "[FAIL] pipe.bronze_to_silver.output_entity: missing: gold.records" in result.output
    assert (
        "[FAIL] entity.silver.records.merge_columns: delta entity missing merge_columns"
        in result.output
    )
    assert "Validation failed" in result.output


def test_env_check_auto_probes_config_settings_yaml():
    runner = CliRunner()
    with runner.isolated_filesystem():
        Path("config").mkdir()
        Path("config/settings.yaml").write_text("kindling: {}\n", encoding="utf-8")

        result = runner.invoke(cli, ["env", "check"])

    assert result.exit_code == 0
    assert "[PASS] config_file_exists: config/settings.yaml" in result.output
    assert "Environment check passed." in result.output


def test_env_check_falls_back_to_root_settings_yaml():
    runner = CliRunner()
    with runner.isolated_filesystem():
        Path("settings.yaml").write_text("kindling: {}\n", encoding="utf-8")

        result = runner.invoke(cli, ["env", "check"])

    assert result.exit_code == 0
    assert "[PASS] config_file_exists: settings.yaml" in result.output


def test_new_project_next_steps_use_single_cd():
    runner = CliRunner()
    with runner.isolated_filesystem():
        result = runner.invoke(cli, ["new", "demo-project", "--no-integration"])

    assert result.exit_code == 0
    assert "  cd demo_project/packages/demo_project" in result.output
    assert "  cd demo_project\n  cd packages/demo_project" not in result.output
