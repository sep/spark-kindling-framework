"""Unit tests for repo/package scaffold generation."""

from pathlib import Path

import pytest
from click.testing import CliRunner
from kindling_cli.cli import cli
from kindling_cli.scaffold import (
    PackageScaffoldConfig,
    RepoScaffoldConfig,
    generate_package,
    generate_project,
    generate_repo,
    validate_name,
)


class TestValidateName:
    def test_hyphenated_name_converts_to_snake(self):
        assert validate_name("my-project") == "my_project"

    def test_spaces_convert_to_snake(self):
        assert validate_name("my project") == "my_project"

    def test_already_snake_passes_through(self):
        assert validate_name("my_project") == "my_project"

    def test_mixed_separators_normalize(self):
        assert validate_name("my--project__name") == "my_project_name"

    def test_uppercase_lowercased(self):
        assert validate_name("MyProject") == "myproject"

    def test_leading_digit_raises(self):
        with pytest.raises(ValueError, match="valid Python identifier"):
            validate_name("1project")

    def test_empty_string_raises(self):
        with pytest.raises(ValueError):
            validate_name("")

    def test_symbols_only_raises(self):
        with pytest.raises(ValueError):
            validate_name("---")


REPO_FILES = [
    ".gitignore",
    ".github/workflows/ci.yml",
    ".devcontainer/Dockerfile",
    ".devcontainer/devcontainer.json",
    ".devcontainer/docker-compose.yml",
]

PACKAGE_FILES = [
    "pyproject.toml",
    ".env.example",
    "config/settings.yaml",
    "config/env.local.yaml",
    "tests/conftest.py",
    "tests/unit/test_transforms.py",
    "tests/component/test_registration.py",
]


def _all_options():
    for layers in ("medallion", "minimal"):
        for auth in ("oauth", "key", "cli"):
            for integration in (True, False):
                yield pytest.param(
                    layers,
                    auth,
                    integration,
                    id=f"{layers}-{auth}-{'int' if integration else 'noint'}",
                )


def _package_root(repo_root: Path, package_name: str) -> Path:
    return repo_root / "packages" / package_name


def test_generate_repo_creates_shared_files(tmp_path):
    cfg = RepoScaffoldConfig(name="data-platform", output_dir=tmp_path)
    generate_repo(cfg)

    root = tmp_path / "data_platform"
    assert (root / "packages").is_dir()
    for rel in REPO_FILES:
        assert (root / rel).exists(), f"Missing repo file {rel}"


@pytest.mark.parametrize("layers,auth,integration", _all_options())
def test_generate_package_creates_package_structure(tmp_path, layers, auth, integration):
    repo_root = tmp_path / "kindling_repo"
    repo_root.mkdir()

    cfg = PackageScaffoldConfig(
        name="sales_ops",
        repo_root=repo_root,
        layers=layers,
        auth=auth,
        integration=integration,
    )
    generate_package(cfg)

    root = _package_root(repo_root, "sales_ops")
    for rel in PACKAGE_FILES:
        assert (root / rel).exists(), f"Missing package file {rel}"

    for rel in [
        "src/sales_ops/entities",
        "src/sales_ops/pipes",
        "src/sales_ops/transforms",
    ]:
        assert (root / rel).is_dir(), f"Missing package dir {rel}"

    if integration:
        assert (root / "tests/integration/test_pipeline.py").exists()
    else:
        assert not (root / "tests/integration").exists()


def test_generate_project_creates_repo_and_initial_package(tmp_path):
    cfg = PackageScaffoldConfig(name="sales_ops", repo_root=tmp_path / "sales_ops")
    generate_project(cfg)

    repo_root = tmp_path / "sales_ops"
    pkg_root = _package_root(repo_root, "sales_ops")
    assert repo_root.is_dir()
    assert pkg_root.is_dir()
    assert (repo_root / ".devcontainer").is_dir()
    assert (pkg_root / "pyproject.toml").exists()


def test_cannot_create_repo_in_existing_directory(tmp_path):
    cfg = RepoScaffoldConfig(name="dupe", output_dir=tmp_path)
    generate_repo(cfg)
    with pytest.raises(FileExistsError):
        generate_repo(cfg)


def test_cannot_create_package_in_existing_directory(tmp_path):
    repo_root = tmp_path / "repo"
    repo_root.mkdir()
    cfg = PackageScaffoldConfig(name="dupe", repo_root=repo_root)
    generate_package(cfg)
    with pytest.raises(FileExistsError):
        generate_package(cfg)


def test_app_py_imports_correct_entity_module_medallion(tmp_path):
    repo_root = tmp_path / "repo"
    repo_root.mkdir()
    cfg = PackageScaffoldConfig(name="acme", repo_root=repo_root, layers="medallion")
    generate_package(cfg)

    app = (_package_root(repo_root, "acme") / "src" / "acme" / "app.py").read_text()
    assert "import acme.entities.records" in app
    assert "import acme.pipes.bronze_to_silver" in app


def test_app_py_imports_correct_pipe_module_minimal(tmp_path):
    repo_root = tmp_path / "repo"
    repo_root.mkdir()
    cfg = PackageScaffoldConfig(name="acme", repo_root=repo_root, layers="minimal")
    generate_package(cfg)

    app = (_package_root(repo_root, "acme") / "src" / "acme" / "app.py").read_text()
    assert "import acme.entities.records" in app
    assert "import acme.pipes.process" in app


def test_settings_yaml_has_no_default_wrapper(tmp_path):
    repo_root = tmp_path / "repo"
    repo_root.mkdir()
    cfg = PackageScaffoldConfig(name="proj", repo_root=repo_root)
    generate_package(cfg)

    settings = (_package_root(repo_root, "proj") / "config" / "settings.yaml").read_text()
    assert "default:" not in settings
    assert "kindling:" in settings


def test_env_local_yaml_top_level_entity_tags(tmp_path):
    repo_root = tmp_path / "repo"
    repo_root.mkdir()
    cfg = PackageScaffoldConfig(name="proj", repo_root=repo_root)
    generate_package(cfg)

    env_local = (_package_root(repo_root, "proj") / "config" / "env.local.yaml").read_text()
    assert env_local.startswith("entity_tags:") or "\nentity_tags:" in env_local
    assert "default:" not in env_local


def test_package_pyproject_uses_kebab_name(tmp_path):
    repo_root = tmp_path / "repo"
    repo_root.mkdir()
    cfg = PackageScaffoldConfig(name="my_project", repo_root=repo_root)
    generate_package(cfg)

    pyproject = (_package_root(repo_root, "my_project") / "pyproject.toml").read_text()
    assert 'name = "my-project"' in pyproject


def test_package_pyproject_uses_spark_kindling_dependency_and_poe_tasks(tmp_path):
    repo_root = tmp_path / "repo"
    repo_root.mkdir()
    cfg = PackageScaffoldConfig(name="proj", repo_root=repo_root, integration=True)
    generate_package(cfg)

    pyproject = (_package_root(repo_root, "proj") / "pyproject.toml").read_text()
    assert 'spark-kindling = {version = ">=0.9.2", extras = ["standalone"]}' in pyproject
    assert 'poethepoet = ">=0.24.0"' in pyproject
    assert '# spark-kindling-cli = ">=0.9.3"' in pyproject
    assert 'test = { sequence = ["test-unit", "test-component"] }' in pyproject
    assert 'test-unit = "pytest tests/unit -v"' in pyproject
    assert 'test-component = "pytest tests/component -v"' in pyproject
    assert 'test-integration = "pytest tests/integration -v"' in pyproject
    assert 'build = "poetry build"' in pyproject


def test_env_example_medallion_has_bronze_and_silver_paths(tmp_path):
    repo_root = tmp_path / "repo"
    repo_root.mkdir()
    cfg = PackageScaffoldConfig(name="proj", repo_root=repo_root, layers="medallion")
    generate_package(cfg)

    env_ex = (_package_root(repo_root, "proj") / ".env.example").read_text()
    assert "ABFSS_BRONZE_PATH" in env_ex
    assert "ABFSS_SILVER_PATH" in env_ex


def test_env_example_minimal_has_raw_path(tmp_path):
    repo_root = tmp_path / "repo"
    repo_root.mkdir()
    cfg = PackageScaffoldConfig(name="proj", repo_root=repo_root, layers="minimal")
    generate_package(cfg)

    env_ex = (_package_root(repo_root, "proj") / ".env.example").read_text()
    assert "ABFSS_RAW_PATH" in env_ex
    assert "ABFSS_BRONZE_PATH" not in env_ex


def test_repo_ci_runs_each_package(tmp_path):
    cfg = RepoScaffoldConfig(name="proj", output_dir=tmp_path)
    generate_repo(cfg)

    workflow = (tmp_path / "proj" / ".github" / "workflows" / "ci.yml").read_text()
    assert "for pkg in packages/*" in workflow
    assert "poetry run poe test" in workflow
    assert "poetry run poe build" in workflow


def test_repo_devcontainer_uses_repo_workspace_and_package_pythonpath_for_new(tmp_path):
    cfg = RepoScaffoldConfig(name="my-proj", output_dir=tmp_path, primary_package_name="my-proj")
    generate_repo(cfg)

    dcj = (tmp_path / "my_proj" / ".devcontainer" / "devcontainer.json").read_text()
    compose = (tmp_path / "my_proj" / ".devcontainer" / "docker-compose.yml").read_text()
    assert '"my-proj"' in dcj
    assert "/workspaces/my_proj" in dcj
    assert "/workspaces/my_proj/packages/my_proj/.venv/bin/python" in dcj
    assert "cd /workspaces/my_proj/packages/my_proj" in dcj
    assert "PYTHONPATH=/workspaces/my_proj/packages/my_proj/src" in compose


def test_repo_devcontainer_defaults_to_system_python_without_primary_package(tmp_path):
    cfg = RepoScaffoldConfig(name="repo-only", output_dir=tmp_path)
    generate_repo(cfg)

    dcj = (tmp_path / "repo_only" / ".devcontainer" / "devcontainer.json").read_text()
    compose = (tmp_path / "repo_only" / ".devcontainer" / "docker-compose.yml").read_text()
    assert "/usr/local/bin/python" in dcj
    assert "Repo initialized" in dcj
    assert "PYTHONPATH=" not in compose


class TestScaffoldCommands:
    def test_repo_init_creates_repo_directory(self, tmp_path):
        runner = CliRunner()
        result = runner.invoke(
            cli, ["repo", "init", "data-platform", "--output-dir", str(tmp_path)]
        )

        assert result.exit_code == 0, result.output
        assert (tmp_path / "data_platform").is_dir()
        assert (tmp_path / "data_platform" / "packages").is_dir()

    def test_package_init_creates_package_under_repo(self, tmp_path):
        repo_root = tmp_path / "repo"
        repo_root.mkdir()

        runner = CliRunner()
        result = runner.invoke(
            cli,
            ["package", "init", "sales-ops", "--repo-root", str(repo_root)],
        )

        assert result.exit_code == 0, result.output
        assert (repo_root / "packages" / "sales_ops").is_dir()

    def test_new_creates_repo_and_initial_package(self, tmp_path):
        runner = CliRunner()
        result = runner.invoke(cli, ["new", "test-proj", "--output-dir", str(tmp_path)])

        assert result.exit_code == 0, result.output
        assert (tmp_path / "test_proj").is_dir()
        assert (tmp_path / "test_proj" / "packages" / "test_proj").is_dir()

    def test_new_no_integration_skips_integration_dir(self, tmp_path):
        runner = CliRunner()
        result = runner.invoke(
            cli, ["new", "my-app", "--no-integration", "--output-dir", str(tmp_path)]
        )

        assert result.exit_code == 0, result.output
        assert not (tmp_path / "my_app" / "packages" / "my_app" / "tests" / "integration").exists()

    def test_new_existing_directory_fails(self, tmp_path):
        (tmp_path / "my_app").mkdir()
        runner = CliRunner()
        result = runner.invoke(cli, ["new", "my-app", "--output-dir", str(tmp_path)])

        assert result.exit_code != 0
        assert "already exists" in result.output.lower()

    def test_package_init_existing_directory_fails(self, tmp_path):
        repo_root = tmp_path / "repo"
        (repo_root / "packages" / "my_app").mkdir(parents=True)

        runner = CliRunner()
        result = runner.invoke(
            cli,
            ["package", "init", "my-app", "--repo-root", str(repo_root)],
        )

        assert result.exit_code != 0
        assert "already exists" in result.output.lower()

    def test_template_dir_overrides_builtin(self, tmp_path):
        tmpl_dir = tmp_path / "custom_templates"
        tmpl_dir.mkdir()
        (tmpl_dir / "app.py.j2").write_text("# CUSTOM_MARKER\n")

        runner = CliRunner()
        result = runner.invoke(
            cli,
            ["new", "my-proj", "--template-dir", str(tmpl_dir), "--output-dir", str(tmp_path)],
        )

        assert result.exit_code == 0, result.output
        app_py = (
            tmp_path / "my_proj" / "packages" / "my_proj" / "src" / "my_proj" / "app.py"
        ).read_text()
        assert "CUSTOM_MARKER" in app_py

    def test_template_dir_falls_back_to_builtin(self, tmp_path):
        tmpl_dir = tmp_path / "custom_templates"
        tmpl_dir.mkdir()
        (tmpl_dir / "app.py.j2").write_text("# override\n")

        runner = CliRunner()
        result = runner.invoke(
            cli,
            ["new", "my-proj", "--template-dir", str(tmpl_dir), "--output-dir", str(tmp_path)],
        )

        assert result.exit_code == 0, result.output
        gitignore = (tmp_path / "my_proj" / ".gitignore").read_text()
        assert ".env" in gitignore
