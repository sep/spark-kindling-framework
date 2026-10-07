"""Unit tests for repo/package scaffold generation."""

from pathlib import Path

import pytest
from click.testing import CliRunner
from kindling_cli.cli import (
    _find_kindling_dependencies,
    _iter_kindling_dependency_entries,
    _load_pyproject_toml,
    cli,
)


def _load_toml_text(text):
    try:
        import tomllib
    except ImportError:  # Python 3.10
        import tomli as tomllib
    return tomllib.loads(text)


from kindling_cli.scaffold import (
    _LOCAL_SPARK_REQUIREMENTS,
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
    "pyproject.toml",
    ".github/workflows/ci.yml",
    ".devcontainer/devcontainer.json",
    "scripts/setup-local-dev.sh",
]

PACKAGE_FILES = [
    "pyproject.toml",
    ".env.example",
    "QUICKSTART.md",
    "settings.yaml",
    "settings.local.yaml",
    "tests/conftest.py",
    "tests/unit/test_transforms.py",
    "tests/component/test_registration.py",
]

APP_FILES = [
    "app.py",
    ".env.example",
    "QUICKSTART.md",
    "settings.yaml",
    "settings.local.yaml",
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
    assert "Hello from sales-ops" in app_py


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


def test_repo_root_pyproject_is_a_uv_workspace_over_packages(tmp_path):
    """The devcontainer runs `kindling env bootstrap` at the repo root, which
    needs a root pyproject.toml to pin Kindling into and sync."""
    repo_root = tmp_path / "data_platform"
    generate_repo(RepoScaffoldConfig(name="data-platform", output_dir=repo_root))

    data = _load_pyproject_toml(repo_root / "pyproject.toml")
    assert data["project"]["name"] == "data-platform-workspace"
    assert data["project"]["dependencies"] == []
    assert data["tool"]["uv"]["package"] is False
    assert data["tool"]["uv"]["workspace"]["members"] == ["packages/*"]
    assert ".venv/" in (repo_root / ".gitignore").read_text()


def test_repo_root_name_does_not_collide_with_same_named_package(tmp_path):
    """uv rejects two workspace members with one name; the documented flow
    scaffolds `repo init X` then `package init X`."""
    repo_root = tmp_path / "my_pipeline"
    generate_repo(RepoScaffoldConfig(name="my-pipeline", output_dir=repo_root))
    generate_package(PackageScaffoldConfig(name="my-pipeline", repo_root=repo_root))

    root_name = _load_pyproject_toml(repo_root / "pyproject.toml")["project"]["name"]
    package_name = _load_pyproject_toml(_package_root(repo_root, "my_pipeline") / "pyproject.toml")[
        "project"
    ]["name"]
    assert root_name != package_name


def test_repo_preserves_existing_root_pyproject(tmp_path):
    repo_root = tmp_path / "repo"
    repo_root.mkdir()
    (repo_root / "pyproject.toml").write_text('[project]\nname = "existing"\n', encoding="utf-8")

    generate_repo(RepoScaffoldConfig(name="repo", output_dir=repo_root))

    assert (repo_root / "pyproject.toml").read_text() == '[project]\nname = "existing"\n'
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


def test_app_py_batch_scaffold_has_no_live_domain_imports(tmp_path):
    repo_root = tmp_path / "repo"
    repo_root.mkdir()
    cfg = AppScaffoldConfig(
        name="acme-app",
        package_name="acme",
        repo_root=repo_root,
        layers="medallion",
        pattern="batch",
    )
    generate_app(cfg)

    app = (repo_root / "apps" / "acme_app" / "app.py").read_text()
    # Entities/pipes are auto-registered from lake-reqs.txt's declared
    # package -- app.py has no domain imports at all, live or commented.
    assert "acme.entities" not in app
    live_imports = [l for l in app.splitlines() if l.startswith("import acme")]
    assert not live_imports, f"Unexpected live domain imports: {live_imports}"
    assert 'if __name__ == "__main__":' in app
    assert "from kindling.apps import run_batch_app" in app

    lake_reqs = (repo_root / "apps" / "acme_app" / "lake-reqs.txt").read_text()
    assert "acme" in lake_reqs


def test_app_py_batch_scaffold_minimal_has_no_live_domain_imports(tmp_path):
    repo_root = tmp_path / "repo"
    repo_root.mkdir()
    cfg = AppScaffoldConfig(
        name="acme-app", package_name="acme", repo_root=repo_root, layers="minimal", pattern="batch"
    )
    generate_app(cfg)

    app = (repo_root / "apps" / "acme_app" / "app.py").read_text()
    assert "acme.entities" not in app
    live_imports = [l for l in app.splitlines() if l.startswith("import acme")]
    assert not live_imports, f"Unexpected live domain imports: {live_imports}"


def test_settings_yaml_has_no_default_wrapper(tmp_path):
    repo_root = tmp_path / "repo"
    repo_root.mkdir()
    cfg = PackageScaffoldConfig(name="proj", repo_root=repo_root)
    generate_package(cfg)

    settings = (_package_root(repo_root, "proj") / "settings.yaml").read_text()
    assert "default:" not in settings
    assert "kindling:" in settings


def test_env_local_yaml_top_level_entity_tags(tmp_path):
    repo_root = tmp_path / "repo"
    repo_root.mkdir()
    cfg = PackageScaffoldConfig(name="proj", repo_root=repo_root)
    generate_package(cfg)

    env_local = (_package_root(repo_root, "proj") / "settings.local.yaml").read_text()
    assert env_local.startswith("entity_tags:") or "\nentity_tags:" in env_local
    assert "default:" not in env_local


def test_app_template_has_no_local_bootstrap_branching(tmp_path):
    repo_root = tmp_path / "repo"
    repo_root.mkdir()
    cfg = AppScaffoldConfig(name="proj", package_name="proj", repo_root=repo_root)
    generate_app(cfg)

    app = (repo_root / "apps" / "proj" / "app.py").read_text()
    assert "initialize_framework" not in app
    assert "KINDLING_CONFIG_DIR" not in app
    assert "sys.path" not in app
    assert "register_all" not in app
    assert "Hello from proj" in app


def test_package_pyproject_uses_kebab_name(tmp_path):
    repo_root = tmp_path / "repo"
    repo_root.mkdir()
    cfg = PackageScaffoldConfig(name="my_project", repo_root=repo_root)
    generate_package(cfg)

    pyproject = (_package_root(repo_root, "my_project") / "pyproject.toml").read_text()
    assert 'name = "my-project"' in pyproject


def test_package_scaffold_is_buildable_by_uv_build(tmp_path):
    """uv_build refuses an __init__.py above the module root (src/), and
    module-name must point at the generated package directory."""
    repo_root = tmp_path / "repo"
    repo_root.mkdir()
    generate_package(PackageScaffoldConfig(name="sales-ops", repo_root=repo_root))

    package_root = _package_root(repo_root, "sales_ops")
    assert not (package_root / "src" / "__init__.py").exists()
    assert (package_root / "src" / "sales_ops" / "__init__.py").exists()
    pyproject = (package_root / "pyproject.toml").read_text()
    assert 'module-name = "sales_ops"' in pyproject


def test_package_pyproject_uses_spark_kindling_dependency_and_poe_tasks(tmp_path):
    repo_root = tmp_path / "repo"
    repo_root.mkdir()
    cfg = PackageScaffoldConfig(name="proj", repo_root=repo_root, integration=True)
    generate_package(cfg)

    pyproject = (_package_root(repo_root, "proj") / "pyproject.toml").read_text()
    assert 'build-backend = "uv_build"' in pyproject
    assert "spark-kindling[" not in pyproject
    assert "spark-kindling = { url = " in pyproject
    assert "/spark_kindling-" in pyproject  # pinned to a release wheel URL
    assert '"poethepoet>=0.24.0",' in pyproject
    assert "[tool.poetry" not in pyproject
    assert "spark-kindling-cli = { url = " in pyproject
    assert "/spark_kindling_cli-" in pyproject
    assert "spark-kindling-sdk = { url = " in pyproject
    assert "/spark_kindling_sdk-" in pyproject
    assert "kindling-local" not in pyproject  # no local PEP 503 index source anymore
    assert 'test = { sequence = ["test-unit", "test-component"] }' in pyproject
    assert 'test-unit = "pytest tests/unit -v"' in pyproject
    assert 'test-component = "pytest tests/component -v"' in pyproject
    assert 'test-integration = "pytest tests/integration -v"' in pyproject
    assert 'build = "uv build"' in pyproject
    assert 'update-kindling = "kindling env update"' in pyproject


def test_package_runtime_dependency_is_plain_spark_kindling(tmp_path):
    """A package wheel is pip-installed onto Databricks/Fabric/Synapse, which
    ship their own Spark and Delta: its runtime dependency must be plain
    spark-kindling, with the local Spark stack only in the dev group."""
    repo_root = tmp_path / "repo"
    repo_root.mkdir()
    generate_package(PackageScaffoldConfig(name="proj", repo_root=repo_root))

    data = _load_pyproject_toml(_package_root(repo_root, "proj") / "pyproject.toml")
    assert data["project"]["dependencies"] == ["spark-kindling"]
    dev = data["dependency-groups"]["dev"]
    for requirement in _LOCAL_SPARK_REQUIREMENTS:
        assert requirement in dev


def test_package_declares_each_kindling_distribution_once(tmp_path):
    """The env commands key Kindling dependencies by distribution name, so
    spark-kindling must not also appear in a dependency group."""
    repo_root = tmp_path / "repo"
    repo_root.mkdir()
    generate_package(PackageScaffoldConfig(name="proj", repo_root=repo_root))

    pyproject_path = _package_root(repo_root, "proj") / "pyproject.toml"
    entries = [
        (name, group) for name, group, _ in _iter_kindling_dependency_entries(pyproject_path)
    ]
    assert sorted(entries, key=lambda e: e[0]) == [
        ("spark-kindling", None),
        ("spark-kindling-cli", "dev"),
        ("spark-kindling-sdk", "dev"),
    ]
    assert _find_kindling_dependencies(pyproject_path)["spark-kindling"] == (None, [])


def test_local_spark_requirements_match_standalone_extra():
    """The package dev group mirrors spark-kindling's `standalone` extra."""
    root_pyproject = Path(__file__).resolve().parents[2] / "pyproject.toml"
    standalone = _load_pyproject_toml(root_pyproject)["project"]["optional-dependencies"][
        "standalone"
    ]
    assert list(_LOCAL_SPARK_REQUIREMENTS) == standalone


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
    assert '(cd "$pkg" && uv run poe test && uv run poe build)' in workflow
    assert "uv sync" not in workflow


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


def test_repo_devcontainer_uses_root_workspace_venv(tmp_path):
    cfg = RepoScaffoldConfig(name="repo-only", output_dir=tmp_path / "repo-only")
    generate_repo(cfg)

    dcj = (tmp_path / "repo-only" / ".devcontainer" / "devcontainer.json").read_text()
    assert '"python.defaultInterpreterPath": "${containerWorkspaceFolder}/.venv/bin/python"' in dcj
    assert "postCreateCommand" in dcj


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
        assert (tmp_path / "pyproject.toml").exists()
        assert "kindling env bootstrap" in result.output
        assert "Kept the existing pyproject.toml" not in result.output

    def test_package_init_rejects_root_workspace_name(self, tmp_path):
        runner = CliRunner()
        assert (
            runner.invoke(cli, ["repo", "init", "sales", "--output-dir", str(tmp_path)]).exit_code
            == 0
        )

        result = runner.invoke(
            cli, ["package", "init", "sales-workspace", "--repo-root", str(tmp_path)]
        )

        assert result.exit_code != 0
        assert "repo root project's name" in result.output
        assert not (tmp_path / "packages" / "sales_workspace").exists()

    def test_package_init_adopts_root_kindling_pin(self, tmp_path):
        runner = CliRunner()
        assert (
            runner.invoke(cli, ["repo", "init", "shop", "--output-dir", str(tmp_path)]).exit_code
            == 0
        )
        root = tmp_path / "pyproject.toml"
        url = (
            "https://github.com/sep/spark-kindling-framework/releases/download/"
            "v0.9.1/spark_kindling-0.9.1-py3-none-any.whl"
        )
        root.write_text(
            root.read_text().replace(
                "dependencies = []", 'dependencies = ["spark-kindling[standalone]"]'
            )
            + f'\n[tool.uv.sources]\nspark-kindling = {{ url = "{url}" }}\n',
            encoding="utf-8",
        )

        result = runner.invoke(cli, ["package", "init", "orders", "--repo-root", str(tmp_path)])

        assert result.exit_code == 0, result.output
        package_pyproject = (tmp_path / "packages" / "orders" / "pyproject.toml").read_text()
        assert "/v0.9.1/spark_kindling-0.9.1-py3-none-any.whl" in package_pyproject
        assert "/v0.9.1/spark_kindling_cli-0.9.1-py3-none-any.whl" in package_pyproject

    def test_repo_init_reports_kept_root_pyproject(self, tmp_path):
        (tmp_path / "pyproject.toml").write_text('[project]\nname = "x"\n', encoding="utf-8")
        result = CliRunner().invoke(cli, ["repo", "init", "x", "--output-dir", str(tmp_path)])

        assert result.exit_code == 0, result.output
        assert "Kept the existing pyproject.toml" in result.output
        snippet = "\n".join(
            line.strip()
            for line in result.output.splitlines()
            if line.strip().startswith(("[tool", "members"))
        )
        assert _load_toml_text(snippet)["tool"]["uv"]["workspace"]["members"] == ["packages/*"]

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
        assert "kindling app run . --platform <platform>" in result.output
        assert "kindling runner register" in result.output

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
        assert "uv run poe test" in result.output
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
