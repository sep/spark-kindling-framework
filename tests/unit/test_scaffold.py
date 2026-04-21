"""Unit tests for kindling_cli.scaffold (kindling new command)."""

import pytest
from click.testing import CliRunner
from kindling_cli.cli import cli
from kindling_cli.scaffold import ScaffoldConfig, generate_project, validate_name

# ---------------------------------------------------------------------------
# validate_name
# ---------------------------------------------------------------------------


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


# ---------------------------------------------------------------------------
# generate_project — invariant structure
# ---------------------------------------------------------------------------


INVARIANT_DIRS = [
    "config",
    "src",
    "tests/unit",
    "tests/component",
]

INVARIANT_FILES = [
    "pyproject.toml",
    ".env.example",
    ".gitignore",
    ".github/workflows/ci.yml",
    ".devcontainer/Dockerfile",
    ".devcontainer/devcontainer.json",
    ".devcontainer/docker-compose.yml",
    "config/settings.yaml",
    "config/env.local.yaml",
    "tests/conftest.py",
    "tests/unit/test_transforms.py",
    "tests/component/test_registration.py",
]


def _all_options():
    """Enumerate all meaningful combinations of scaffold options."""
    for layers in ("medallion", "minimal"):
        for auth in ("oauth", "key", "cli"):
            for integration in (True, False):
                yield pytest.param(
                    layers,
                    auth,
                    integration,
                    id=f"{layers}-{auth}-{'int' if integration else 'noint'}",
                )


@pytest.mark.parametrize("layers,auth,integration", _all_options())
def test_invariant_structure(tmp_path, layers, auth, integration):
    """All option combinations produce the same directory/file layout."""
    cfg = ScaffoldConfig(
        name="test_proj",
        layers=layers,
        auth=auth,
        integration=integration,
        output_dir=tmp_path,
    )
    generate_project(cfg)

    root = tmp_path / "test_proj"

    for rel in INVARIANT_FILES:
        assert (root / rel).exists(), f"Missing {rel} for layers={layers} auth={auth}"

    for rel in ["src/test_proj/entities", "src/test_proj/pipes", "src/test_proj/transforms"]:
        assert (root / rel).is_dir(), f"Missing dir {rel}"

    src_pkg = root / "src" / "test_proj"
    assert (src_pkg / "app.py").exists()
    assert (src_pkg / "entities" / "__init__.py").exists()
    assert (src_pkg / "pipes" / "__init__.py").exists()
    assert (src_pkg / "transforms" / "__init__.py").exists()

    if integration:
        assert (root / "tests/integration/test_pipeline.py").exists()
    else:
        assert not (root / "tests/integration").exists()


def test_cannot_create_project_in_existing_directory(tmp_path):
    cfg = ScaffoldConfig(name="dupe", output_dir=tmp_path)
    generate_project(cfg)

    with pytest.raises(FileExistsError):
        generate_project(cfg)


# ---------------------------------------------------------------------------
# Content assertions
# ---------------------------------------------------------------------------


def test_app_py_imports_correct_entity_module_medallion(tmp_path):
    cfg = ScaffoldConfig(name="acme", layers="medallion", output_dir=tmp_path)
    generate_project(cfg)

    app = (tmp_path / "acme" / "src" / "acme" / "app.py").read_text()
    assert "import acme.entities.records" in app
    assert "import acme.pipes.bronze_to_silver" in app


def test_app_py_imports_correct_pipe_module_minimal(tmp_path):
    cfg = ScaffoldConfig(name="acme", layers="minimal", output_dir=tmp_path)
    generate_project(cfg)

    app = (tmp_path / "acme" / "src" / "acme" / "app.py").read_text()
    assert "import acme.entities.records" in app
    assert "import acme.pipes.process" in app


def test_settings_yaml_has_no_default_wrapper(tmp_path):
    """Dynaconf get_entity_tags() requires top-level keys — no 'default:' wrap."""
    cfg = ScaffoldConfig(name="proj", output_dir=tmp_path)
    generate_project(cfg)

    settings = (tmp_path / "proj" / "config" / "settings.yaml").read_text()
    assert "default:" not in settings
    assert "kindling:" in settings


def test_env_local_yaml_top_level_entity_tags(tmp_path):
    """entity_tags must be at the top level, not nested under default:."""
    cfg = ScaffoldConfig(name="proj", output_dir=tmp_path)
    generate_project(cfg)

    env_local = (tmp_path / "proj" / "config" / "env.local.yaml").read_text()
    assert env_local.startswith("entity_tags:") or "\nentity_tags:" in env_local
    assert "default:" not in env_local


def test_pyproject_uses_kebab_name(tmp_path):
    cfg = ScaffoldConfig(name="my_project", output_dir=tmp_path)
    generate_project(cfg)

    pyproject = (tmp_path / "my_project" / "pyproject.toml").read_text()
    assert 'name = "my-project"' in pyproject


def test_pyproject_uses_spark_kindling_dependency_and_poe_tasks(tmp_path):
    cfg = ScaffoldConfig(name="proj", integration=True, output_dir=tmp_path)
    generate_project(cfg)

    pyproject = (tmp_path / "proj" / "pyproject.toml").read_text()
    assert 'spark-kindling = {version = ">=0.9.1", extras = ["standalone"]}' in pyproject
    assert 'poethepoet = ">=0.24.0"' in pyproject
    assert 'test = { sequence = ["test-unit", "test-component"] }' in pyproject
    assert 'test-integration = "pytest tests/integration -v"' in pyproject
    assert 'build = "poetry build"' in pyproject


def test_conftest_has_oauth_vars(tmp_path):
    cfg = ScaffoldConfig(name="proj", auth="oauth", output_dir=tmp_path)
    generate_project(cfg)

    conftest = (tmp_path / "proj" / "tests" / "conftest.py").read_text()
    assert "AZURE_CLIENT_SECRET" in conftest
    assert "AZURE_TENANT_ID" in conftest


def test_conftest_has_key_vars(tmp_path):
    cfg = ScaffoldConfig(name="proj", auth="key", output_dir=tmp_path)
    generate_project(cfg)

    conftest = (tmp_path / "proj" / "tests" / "conftest.py").read_text()
    assert "AZURE_STORAGE_KEY" in conftest
    assert "AZURE_TENANT_ID" not in conftest


def test_conftest_has_no_secret_for_cli_auth(tmp_path):
    cfg = ScaffoldConfig(name="proj", auth="cli", output_dir=tmp_path)
    generate_project(cfg)

    conftest = (tmp_path / "proj" / "tests" / "conftest.py").read_text()
    assert "az login" in conftest
    assert "AZURE_CLIENT_SECRET" not in conftest


def test_medallion_entity_tags_have_bronze_and_silver(tmp_path):
    cfg = ScaffoldConfig(name="proj", layers="medallion", output_dir=tmp_path)
    generate_project(cfg)

    env_local = (tmp_path / "proj" / "config" / "env.local.yaml").read_text()
    assert "bronze.records" in env_local
    assert "silver.records" in env_local


def test_minimal_entity_tags_have_raw(tmp_path):
    cfg = ScaffoldConfig(name="proj", layers="minimal", output_dir=tmp_path)
    generate_project(cfg)

    env_local = (tmp_path / "proj" / "config" / "env.local.yaml").read_text()
    assert "raw.records" in env_local
    assert "bronze.records" not in env_local


def test_env_example_oauth_has_all_sp_vars(tmp_path):
    cfg = ScaffoldConfig(name="proj", auth="oauth", output_dir=tmp_path)
    generate_project(cfg)

    env_ex = (tmp_path / "proj" / ".env.example").read_text()
    for var in (
        "AZURE_STORAGE_ACCOUNT",
        "AZURE_TENANT_ID",
        "AZURE_CLIENT_ID",
        "AZURE_CLIENT_SECRET",
    ):
        assert var in env_ex


def test_env_example_key_has_storage_key(tmp_path):
    cfg = ScaffoldConfig(name="proj", auth="key", output_dir=tmp_path)
    generate_project(cfg)

    env_ex = (tmp_path / "proj" / ".env.example").read_text()
    assert "AZURE_STORAGE_KEY" in env_ex
    assert "AZURE_TENANT_ID" not in env_ex


def test_env_example_cli_has_no_secret(tmp_path):
    cfg = ScaffoldConfig(name="proj", auth="cli", output_dir=tmp_path)
    generate_project(cfg)

    env_ex = (tmp_path / "proj" / ".env.example").read_text()
    assert "az login" in env_ex
    assert "AZURE_CLIENT_SECRET" not in env_ex


def test_env_example_medallion_has_bronze_and_silver_paths(tmp_path):
    cfg = ScaffoldConfig(name="proj", layers="medallion", output_dir=tmp_path)
    generate_project(cfg)

    env_ex = (tmp_path / "proj" / ".env.example").read_text()
    assert "ABFSS_BRONZE_PATH" in env_ex
    assert "ABFSS_SILVER_PATH" in env_ex


def test_env_example_minimal_has_raw_path(tmp_path):
    cfg = ScaffoldConfig(name="proj", layers="minimal", output_dir=tmp_path)
    generate_project(cfg)

    env_ex = (tmp_path / "proj" / ".env.example").read_text()
    assert "ABFSS_RAW_PATH" in env_ex
    assert "ABFSS_BRONZE_PATH" not in env_ex


def test_gitignore_excludes_dotenv(tmp_path):
    cfg = ScaffoldConfig(name="proj", output_dir=tmp_path)
    generate_project(cfg)

    gitignore = (tmp_path / "proj" / ".gitignore").read_text()
    assert ".env" in gitignore


def test_generated_ci_workflow_runs_poe_test_and_build(tmp_path):
    cfg = ScaffoldConfig(name="proj", output_dir=tmp_path)
    generate_project(cfg)

    workflow = (tmp_path / "proj" / ".github" / "workflows" / "ci.yml").read_text()
    assert "name: CI" in workflow
    assert "poetry run poe test" in workflow
    assert "poetry run poe build" in workflow


# ---------------------------------------------------------------------------
# Dev container
# ---------------------------------------------------------------------------


def test_devcontainer_dockerfile_downloads_hadoop_jars(tmp_path):
    cfg = ScaffoldConfig(name="proj", output_dir=tmp_path)
    generate_project(cfg)

    dockerfile = (tmp_path / "proj" / ".devcontainer" / "Dockerfile").read_text()
    assert "hadoop-azure-3.3.4.jar" in dockerfile
    assert "/opt/hadoop-jars" in dockerfile
    assert "/tmp/hadoop-jars" in dockerfile  # symlink target


def test_devcontainer_json_uses_project_name(tmp_path):
    cfg = ScaffoldConfig(name="my-proj", output_dir=tmp_path)
    generate_project(cfg)

    dcj = (tmp_path / "my_proj" / ".devcontainer" / "devcontainer.json").read_text()
    assert '"my-proj"' in dcj
    assert "/workspaces/my_proj" in dcj


def test_devcontainer_json_forwards_spark_ui_port(tmp_path):
    cfg = ScaffoldConfig(name="proj", output_dir=tmp_path)
    generate_project(cfg)

    dcj = (tmp_path / "proj" / ".devcontainer" / "devcontainer.json").read_text()
    assert "4040" in dcj
    assert "Spark UI" in dcj


def test_devcontainer_compose_sets_pythonpath(tmp_path):
    cfg = ScaffoldConfig(name="proj", output_dir=tmp_path)
    generate_project(cfg)

    compose = (tmp_path / "proj" / ".devcontainer" / "docker-compose.yml").read_text()
    assert "PYTHONPATH=/workspaces/proj/src" in compose


# ---------------------------------------------------------------------------
# CLI integration — kindling new
# ---------------------------------------------------------------------------


class TestKindlingNewCommand:
    def test_creates_project_directory(self, tmp_path):
        runner = CliRunner()
        result = runner.invoke(cli, ["new", "test-proj", "--output-dir", str(tmp_path)])

        assert result.exit_code == 0, result.output
        assert (tmp_path / "test_proj").is_dir()

    def test_all_default_files_created(self, tmp_path):
        runner = CliRunner()
        runner.invoke(cli, ["new", "my-app", "--output-dir", str(tmp_path)])

        root = tmp_path / "my_app"
        for rel in INVARIANT_FILES:
            assert (root / rel).exists(), f"Missing: {rel}"

    def test_no_integration_skips_integration_dir(self, tmp_path):
        runner = CliRunner()
        runner.invoke(cli, ["new", "my-app", "--no-integration", "--output-dir", str(tmp_path)])

        assert not (tmp_path / "my_app" / "tests" / "integration").exists()

    def test_existing_directory_fails(self, tmp_path):
        (tmp_path / "my_app").mkdir()
        runner = CliRunner()
        result = runner.invoke(cli, ["new", "my-app", "--output-dir", str(tmp_path)])

        assert result.exit_code != 0
        assert "already exists" in result.output.lower()

    def test_invalid_name_fails(self, tmp_path):
        runner = CliRunner()
        result = runner.invoke(cli, ["new", "1invalid", "--output-dir", str(tmp_path)])

        assert result.exit_code != 0

    def test_minimal_layers_option(self, tmp_path):
        runner = CliRunner()
        result = runner.invoke(
            cli, ["new", "min-proj", "--layers", "minimal", "--output-dir", str(tmp_path)]
        )

        assert result.exit_code == 0, result.output
        root = tmp_path / "min_proj"
        assert (root / "src" / "min_proj" / "entities").is_dir()
        assert (root / "src" / "min_proj" / "pipes").is_dir()
        assert (root / "src" / "min_proj" / "transforms").is_dir()

    def test_auth_key_option(self, tmp_path):
        runner = CliRunner()
        result = runner.invoke(
            cli, ["new", "key-proj", "--auth", "key", "--output-dir", str(tmp_path)]
        )

        assert result.exit_code == 0, result.output
        conftest = (tmp_path / "key_proj" / "tests" / "conftest.py").read_text()
        assert "AZURE_STORAGE_KEY" in conftest

    def test_template_dir_overrides_builtin(self, tmp_path):
        """A custom template in --template-dir takes precedence over the built-in."""
        tmpl_dir = tmp_path / "custom_templates"
        tmpl_dir.mkdir()
        # Write a minimal app.py.j2 override with a sentinel comment
        (tmpl_dir / "app.py.j2").write_text("# CUSTOM_MARKER\n")

        proj_dir = tmp_path / "projects"
        proj_dir.mkdir()
        runner = CliRunner()
        result = runner.invoke(
            cli,
            ["new", "my-proj", "--template-dir", str(tmpl_dir), "--output-dir", str(proj_dir)],
        )

        assert result.exit_code == 0, result.output
        app_py = (proj_dir / "my_proj" / "src" / "my_proj" / "app.py").read_text()
        assert "CUSTOM_MARKER" in app_py

    def test_template_dir_falls_back_to_builtin(self, tmp_path):
        """Templates NOT in --template-dir are served from the built-in directory."""
        tmpl_dir = tmp_path / "custom_templates"
        tmpl_dir.mkdir()
        # Only override app.py.j2; .gitignore should still come from built-ins
        (tmpl_dir / "app.py.j2").write_text("# override\n")

        proj_dir = tmp_path / "projects"
        proj_dir.mkdir()
        runner = CliRunner()
        runner.invoke(
            cli,
            ["new", "my-proj", "--template-dir", str(tmpl_dir), "--output-dir", str(proj_dir)],
        )

        gitignore = (proj_dir / "my_proj" / ".gitignore").read_text()
        assert ".env" in gitignore
