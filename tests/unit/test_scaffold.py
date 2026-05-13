"""Unit tests for repo/package scaffold generation."""

from pathlib import Path

import pytest
from click.testing import CliRunner
from kindling_cli.cli import cli
from kindling_cli.scaffold import (
    AppScaffoldConfig,
    PackageScaffoldConfig,
    RepoScaffoldConfig,
    generate_app,
    generate_package,
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
    ".devcontainer/devcontainer.json",
    "scripts/setup-local-dev.sh",
]

PACKAGE_FILES = [
    "pyproject.toml",
    ".env.example",
    "QUICKSTART.md",
    "config/settings.yaml",
    "config/env.local.yaml",
    "tests/conftest.py",
    "tests/unit/test_transforms.py",
    "tests/component/test_registration.py",
]

APP_FILES = [
    "app.py",
    ".env.example",
    "QUICKSTART.md",
    "config/settings.yaml",
    "config/env.local.yaml",
    "tests/entities/bronze/records.csv",  # medallion default
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
    repo_root = tmp_path / "data_platform"
    cfg = RepoScaffoldConfig(name="data-platform", output_dir=repo_root)
    generate_repo(cfg)

    assert repo_root.is_dir()
    assert (repo_root / "packages").is_dir()
    assert (repo_root / "apps").is_dir()
    for rel in REPO_FILES:
        assert (repo_root / rel).exists(), f"Missing repo file {rel}"


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
        assert (root / "tests/integration/test_pipeline_azure.py").exists()
        assert (root / "tests/integration/test_pipeline_local.py").exists()
    else:
        assert not (root / "tests/integration").exists()


def test_generate_app_creates_independent_app_structure(tmp_path):
    repo_root = tmp_path / "kindling_repo"
    repo_root.mkdir()

    cfg = AppScaffoldConfig(name="sales_ops", repo_root=repo_root, package_name="sales_ops")
    generate_app(cfg)

    root = repo_root / "apps" / "sales_ops"
    for rel in APP_FILES:
        assert (root / rel).exists(), f"Missing app file {rel}"
    app_py = (root / "app.py").read_text()
    assert '"packages" / "sales_ops" / "src"' in app_py


def test_cannot_create_repo_over_existing_generated_file(tmp_path):
    cfg = RepoScaffoldConfig(name="dupe", output_dir=tmp_path)
    generate_repo(cfg)
    with pytest.raises(FileExistsError):
        generate_repo(cfg)


def test_repo_preserves_existing_devcontainer(tmp_path):
    repo_root = tmp_path / "repo"
    devcontainer = repo_root / ".devcontainer" / "devcontainer.json"
    devcontainer.parent.mkdir(parents=True)
    devcontainer.write_text('{"name": "existing"}', encoding="utf-8")

    cfg = RepoScaffoldConfig(name="repo", output_dir=repo_root)
    generate_repo(cfg)

    assert devcontainer.read_text() == '{"name": "existing"}'
    assert (repo_root / ".gitignore").exists()


def test_repo_can_overwrite_existing_devcontainer(tmp_path):
    repo_root = tmp_path / "repo"
    devcontainer = repo_root / ".devcontainer" / "devcontainer.json"
    devcontainer.parent.mkdir(parents=True)
    devcontainer.write_text('{"name": "old"}', encoding="utf-8")

    cfg = RepoScaffoldConfig(name="repo", output_dir=repo_root, overwrite_devcontainer=True)
    generate_repo(cfg)

    assert '"Kindling Domain Development"' in devcontainer.read_text()
    assert (repo_root / ".gitignore").exists()


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
    cfg = AppScaffoldConfig(
        name="acme-app", package_name="acme", repo_root=repo_root, layers="medallion"
    )
    generate_app(cfg)

    app = (repo_root / "apps" / "acme_app" / "app.py").read_text()
    assert "import acme.entities.records" in app
    assert "import acme.pipes.bronze_to_silver" in app


def test_app_py_imports_correct_pipe_module_minimal(tmp_path):
    repo_root = tmp_path / "repo"
    repo_root.mkdir()
    cfg = AppScaffoldConfig(
        name="acme-app", package_name="acme", repo_root=repo_root, layers="minimal"
    )
    generate_app(cfg)

    app = (repo_root / "apps" / "acme_app" / "app.py").read_text()
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
    assert "spark-kindling = {path = " in pyproject
    assert 'extras = ["standalone"]' in pyproject
    assert 'poethepoet = ">=0.24.0"' in pyproject
    assert "spark-kindling-cli = {path = " in pyproject
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
    assert "AZURE_CLOUD" in env_ex
    assert "AZURE_STORAGE_DFS_ENDPOINT_SUFFIX" in env_ex
    assert "AZURE_STORAGE_BLOB_ENDPOINT_SUFFIX" in env_ex
    assert "AZURE_STORAGE_TOKEN_SCOPE" in env_ex
    assert "AZURE_AUTHORITY_HOST" in env_ex


def test_env_example_minimal_has_raw_path(tmp_path):
    repo_root = tmp_path / "repo"
    repo_root.mkdir()
    cfg = PackageScaffoldConfig(name="proj", repo_root=repo_root, layers="minimal")
    generate_package(cfg)

    env_ex = (_package_root(repo_root, "proj") / ".env.example").read_text()
    assert "ABFSS_RAW_PATH" in env_ex
    assert "ABFSS_BRONZE_PATH" not in env_ex


def test_generated_conftest_uses_azure_endpoint_env_overrides(tmp_path):
    repo_root = tmp_path / "repo"
    repo_root.mkdir()
    cfg = PackageScaffoldConfig(name="proj", repo_root=repo_root, auth="oauth")
    generate_package(cfg)

    conftest = (_package_root(repo_root, "proj") / "tests" / "conftest.py").read_text()
    assert "AZURE_STORAGE_DFS_ENDPOINT_SUFFIX" in conftest
    assert "AZURE_STORAGE_TOKEN_SCOPE" in conftest
    assert "AZURE_AUTHORITY_HOST" in conftest
    assert 'f"{account}.dfs.core.windows.net"' not in conftest
    assert "login.microsoftonline.com/{tenant}" not in conftest


def test_repo_ci_runs_each_package(tmp_path):
    cfg = RepoScaffoldConfig(name="proj", output_dir=tmp_path / "proj")
    generate_repo(cfg)

    workflow = (tmp_path / "proj" / ".github" / "workflows" / "ci.yml").read_text()
    assert "for pkg in packages/*" in workflow
    assert "poetry run poe test" in workflow
    assert "poetry run poe build" in workflow


def test_repo_devcontainer_uses_repo_workspace_and_package_pythonpath_for_new(tmp_path):
    cfg = RepoScaffoldConfig(
        name="my-proj", output_dir=tmp_path / "my-proj", primary_package_name="my-proj"
    )
    generate_repo(cfg)

    dcj = (tmp_path / "my-proj" / ".devcontainer" / "devcontainer.json").read_text()
    assert '"Kindling Domain Development"' in dcj
    assert '"image": "ghcr.io/sep/spark-kindling-framework/devcontainer:latest"' in dcj
    assert '"workspaceFolder": "/workspaces/my-proj"' in dcj
    assert '"PYTHONPATH": "/workspaces/my-proj"' in dcj


def test_repo_devcontainer_defaults_to_system_python_without_primary_package(tmp_path):
    cfg = RepoScaffoldConfig(name="repo-only", output_dir=tmp_path / "repo-only")
    generate_repo(cfg)

    dcj = (tmp_path / "repo-only" / ".devcontainer" / "devcontainer.json").read_text()
    assert "/usr/local/bin/python" in dcj
    assert "postCreateCommand" not in dcj


class TestScaffoldCommands:
    def test_repo_init_initializes_output_directory(self, tmp_path):
        runner = CliRunner()
        result = runner.invoke(
            cli, ["repo", "init", "data-platform", "--output-dir", str(tmp_path)]
        )

        assert result.exit_code == 0, result.output
        assert (tmp_path / "packages").is_dir()
        assert (tmp_path / ".devcontainer" / "devcontainer.json").exists()
        assert not (tmp_path / "data_platform").exists()

    def test_repo_init_warns_for_existing_devcontainer(self, tmp_path):
        devcontainer = tmp_path / ".devcontainer" / "devcontainer.json"
        devcontainer.parent.mkdir()
        devcontainer.write_text('{"name": "existing"}', encoding="utf-8")

        runner = CliRunner()
        result = runner.invoke(
            cli, ["repo", "init", "data-platform", "--output-dir", str(tmp_path)]
        )

        assert result.exit_code == 0, result.output
        assert "--overwrite-devcontainer" in result.output
        assert "already exists" in result.output
        assert devcontainer.read_text() == '{"name": "existing"}'
        assert (tmp_path / "packages").is_dir()

    def test_repo_init_overwrites_existing_devcontainer_with_flag(self, tmp_path):
        devcontainer = tmp_path / ".devcontainer" / "devcontainer.json"
        devcontainer.parent.mkdir()
        devcontainer.write_text('{"name": "old"}', encoding="utf-8")

        runner = CliRunner()
        result = runner.invoke(
            cli,
            [
                "repo",
                "init",
                "data-platform",
                "--output-dir",
                str(tmp_path),
                "--overwrite-devcontainer",
            ],
        )

        assert result.exit_code == 0, result.output
        assert '"Kindling Domain Development"' in devcontainer.read_text()
        assert (tmp_path / "packages").is_dir()

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
        assert not (repo_root / "packages" / "sales_ops" / "src" / "sales_ops" / "app.py").exists()

    def test_app_init_creates_app_under_repo(self, tmp_path):
        repo_root = tmp_path / "repo"
        repo_root.mkdir()

        runner = CliRunner()
        result = runner.invoke(
            cli,
            ["app", "init", "sales-ops", "--package", "sales-ops", "--repo-root", str(repo_root)],
        )

        assert result.exit_code == 0, result.output
        assert (repo_root / "apps" / "sales_ops" / "app.py").exists()

    def test_repo_package_app_init_create_explicit_structure(self, tmp_path):
        runner = CliRunner()
        repo_root = tmp_path / "test_proj"
        result_repo = runner.invoke(
            cli,
            ["repo", "init", "test-proj", "--output-dir", str(repo_root)],
        )
        result_package = runner.invoke(
            cli,
            ["package", "init", "test-proj", "--repo-root", str(repo_root)],
        )
        result_app = runner.invoke(
            cli,
            ["app", "init", "test-proj", "--package", "test-proj", "--repo-root", str(repo_root)],
        )

        assert result_repo.exit_code == 0, result_repo.output
        assert result_package.exit_code == 0, result_package.output
        assert result_app.exit_code == 0, result_app.output
        assert repo_root.is_dir()
        assert (repo_root / "packages" / "test_proj").is_dir()
        assert (repo_root / "apps" / "test_proj").is_dir()

    def test_app_init_prints_next_steps(self, tmp_path):
        repo_root = tmp_path / "repo"
        repo_root.mkdir()
        runner = CliRunner()
        result = runner.invoke(
            cli,
            ["app", "init", "my-proj", "--package", "my-proj", "--repo-root", str(repo_root)],
        )

        assert result.exit_code == 0, result.output
        assert "Next steps" in result.output
        assert "cd apps/my_proj" in result.output
        assert "kindling app run ." in result.output
        assert "kindling runner ensure --platform <platform>" in result.output
        assert "kindling app run . --platform <platform>" in result.output

    def test_app_init_minimal_layers_prints_run_step(self, tmp_path):
        repo_root = tmp_path / "repo"
        repo_root.mkdir()
        runner = CliRunner()
        result = runner.invoke(
            cli,
            [
                "app",
                "init",
                "my-proj",
                "--package",
                "my-proj",
                "--layers",
                "minimal",
                "--repo-root",
                str(repo_root),
            ],
        )

        assert result.exit_code == 0, result.output
        assert "kindling app run ." in result.output

    def test_package_init_prints_next_steps(self, tmp_path):
        repo_root = tmp_path / "repo"
        repo_root.mkdir()

        runner = CliRunner()
        result = runner.invoke(cli, ["package", "init", "my-pkg", "--repo-root", str(repo_root)])

        assert result.exit_code == 0, result.output
        assert "Next steps" in result.output
        assert "cd packages/my_pkg" in result.output
        assert "poetry run poe test" in result.output
        assert "kindling app init my_pkg --package my_pkg" in result.output

    def test_package_init_no_integration_skips_integration_dir(self, tmp_path):
        repo_root = tmp_path / "repo"
        repo_root.mkdir()
        runner = CliRunner()
        result = runner.invoke(
            cli,
            ["package", "init", "my-app", "--no-integration", "--repo-root", str(repo_root)],
        )

        assert result.exit_code == 0, result.output
        assert not (repo_root / "packages" / "my_app" / "tests" / "integration").exists()

    def test_app_init_existing_directory_fails(self, tmp_path):
        repo_root = tmp_path / "repo"
        (repo_root / "apps" / "my_app").mkdir(parents=True)
        runner = CliRunner()
        result = runner.invoke(
            cli,
            ["app", "init", "my-app", "--package", "my-app", "--repo-root", str(repo_root)],
        )

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
        repo_root = tmp_path / "repo"
        repo_root.mkdir()
        result = runner.invoke(
            cli,
            [
                "app",
                "init",
                "my-proj",
                "--package",
                "my-proj",
                "--template-dir",
                str(tmpl_dir),
                "--repo-root",
                str(repo_root),
            ],
        )

        assert result.exit_code == 0, result.output
        app_py = (repo_root / "apps" / "my_proj" / "app.py").read_text()
        assert "CUSTOM_MARKER" in app_py

    def test_template_dir_falls_back_to_builtin(self, tmp_path):
        tmpl_dir = tmp_path / "custom_templates"
        tmpl_dir.mkdir()
        (tmpl_dir / "app.py.j2").write_text("# override\n")

        runner = CliRunner()
        repo_root = tmp_path / "repo"
        result = runner.invoke(
            cli,
            [
                "repo",
                "init",
                "my-proj",
                "--template-dir",
                str(tmpl_dir),
                "--output-dir",
                str(repo_root),
            ],
        )

        assert result.exit_code == 0, result.output
        gitignore = (repo_root / ".gitignore").read_text()
        assert ".env" in gitignore


class TestAppFixtureCSVs:
    """Fixture CSV generation smoke tests."""

    def test_medallion_app_creates_bronze_fixture_csv(self, tmp_path):
        repo_root = tmp_path / "repo"
        repo_root.mkdir()
        cfg = AppScaffoldConfig(name="acme", repo_root=repo_root, layers="medallion")
        generate_app(cfg)

        csv = repo_root / "apps" / "acme" / "tests" / "entities" / "bronze" / "records.csv"
        assert csv.exists(), "bronze fixture CSV missing"
        header = csv.read_text().splitlines()[0]
        assert "id" in header
        assert "date" in header
        assert "value" in header

    def test_minimal_app_creates_raw_fixture_csv(self, tmp_path):
        repo_root = tmp_path / "repo"
        repo_root.mkdir()
        cfg = AppScaffoldConfig(name="acme", repo_root=repo_root, layers="minimal")
        generate_app(cfg)

        csv = repo_root / "apps" / "acme" / "tests" / "entities" / "raw" / "records.csv"
        assert csv.exists(), "raw fixture CSV missing"
        header = csv.read_text().splitlines()[0]
        assert "id" in header
        assert "value" in header

    def test_minimal_app_does_not_create_bronze_fixture(self, tmp_path):
        repo_root = tmp_path / "repo"
        repo_root.mkdir()
        cfg = AppScaffoldConfig(name="acme", repo_root=repo_root, layers="minimal")
        generate_app(cfg)

        bronze_csv = repo_root / "apps" / "acme" / "tests" / "entities" / "bronze" / "records.csv"
        assert not bronze_csv.exists()


class TestPipeSignatures:
    """Smoke tests for generated pipe parameter names."""

    def test_medallion_bronze_to_silver_uses_entity_kwarg(self, tmp_path):
        repo_root = tmp_path / "repo"
        repo_root.mkdir()
        cfg = PackageScaffoldConfig(name="acme", repo_root=repo_root, layers="medallion")
        generate_package(cfg)

        pipe = (
            _package_root(repo_root, "acme") / "src" / "acme" / "pipes" / "bronze_to_silver.py"
        ).read_text()
        assert "def bronze_to_silver(bronze_records)" in pipe
        assert "spark" not in pipe.split("def bronze_to_silver")[1].split("):")[0]

    def test_minimal_process_uses_entity_kwarg(self, tmp_path):
        repo_root = tmp_path / "repo"
        repo_root.mkdir()
        cfg = PackageScaffoldConfig(name="acme", repo_root=repo_root, layers="minimal")
        generate_package(cfg)

        pipe = (
            _package_root(repo_root, "acme") / "src" / "acme" / "pipes" / "process.py"
        ).read_text()
        assert "def process_records(raw_records)" in pipe
        assert "spark" not in pipe.split("def process_records")[1].split("):")[0]
