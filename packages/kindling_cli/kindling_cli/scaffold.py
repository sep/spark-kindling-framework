"""Scaffold generators for Kindling repos and packages."""

from __future__ import annotations

import importlib.metadata
import keyword
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import List, Optional

import jinja2


def _kindling_version() -> str:
    try:
        return importlib.metadata.version("spark-kindling-cli")
    except importlib.metadata.PackageNotFoundError:
        return "latest"


_BUILTIN_TEMPLATES = Path(__file__).parent / "templates"


@dataclass
class RepoScaffoldConfig:
    name: str
    output_dir: Path = field(default_factory=Path.cwd)
    template_dir: Optional[Path] = None
    primary_package_name: Optional[str] = None
    overwrite_devcontainer: bool = False

    @property
    def snake_name(self) -> str:
        return _to_snake(self.name)

    @property
    def kebab_name(self) -> str:
        return self.snake_name.replace("_", "-")

    @property
    def primary_package_snake_name(self) -> Optional[str]:
        if self.primary_package_name is None:
            return None
        return _to_snake(self.primary_package_name)


@dataclass
class PackageScaffoldConfig:
    name: str
    repo_root: Path
    layers: str = "medallion"
    auth: str = "oauth"
    integration: bool = True
    template_dir: Optional[Path] = None

    @property
    def snake_name(self) -> str:
        return _to_snake(self.name)

    @property
    def kebab_name(self) -> str:
        return self.snake_name.replace("_", "-")

    @property
    def repo_snake_name(self) -> str:
        return _to_snake(self.repo_root.name)

    @property
    def repo_kebab_name(self) -> str:
        return self.repo_snake_name.replace("_", "-")


@dataclass
class AppScaffoldConfig:
    name: str
    repo_root: Path
    package_name: Optional[str] = None
    layers: str = "medallion"
    auth: str = "oauth"
    pattern: Optional[str] = None
    template_dir: Optional[Path] = None

    @property
    def snake_name(self) -> str:
        return _to_snake(self.name)

    @property
    def kebab_name(self) -> str:
        return self.snake_name.replace("_", "-")

    @property
    def package_snake_name(self) -> str:
        return _to_snake(self.package_name or self.name)

    @property
    def package_kebab_name(self) -> str:
        return self.package_snake_name.replace("_", "-")

    @property
    def repo_snake_name(self) -> str:
        return _to_snake(self.repo_root.name)

    @property
    def repo_kebab_name(self) -> str:
        return self.repo_snake_name.replace("_", "-")


# Backward-compatible alias for older imports/tests.
ScaffoldConfig = PackageScaffoldConfig


def _to_snake(name: str) -> str:
    s = name.strip().lower()
    s = re.sub(r"[^a-z0-9]+", "_", s)
    s = s.strip("_")
    if not s or s[0].isdigit():
        raise ValueError(f"Project name {name!r} cannot be converted to a valid Python identifier.")
    if keyword.iskeyword(s):
        raise ValueError(
            f"Project name {name!r} maps to the Python keyword '{s}', which cannot be used as a package name."
        )
    return s


def validate_name(name: str) -> str:
    return _to_snake(name)


def _make_env(template_dir: Optional[Path]) -> jinja2.Environment:
    loaders: list[jinja2.BaseLoader] = []
    if template_dir:
        loaders.append(jinja2.FileSystemLoader(str(template_dir)))
    loaders.append(jinja2.FileSystemLoader(str(_BUILTIN_TEMPLATES)))
    return jinja2.Environment(  # nosec B701 — generates code/YAML, not HTML; autoescape would corrupt templates
        loader=jinja2.ChoiceLoader(loaders),
        keep_trailing_newline=True,
        undefined=jinja2.StrictUndefined,
    )


def _repo_ctx(cfg: RepoScaffoldConfig) -> dict:
    return {
        "snake_name": cfg.snake_name,
        "kebab_name": cfg.kebab_name,
        "repo_snake_name": cfg.snake_name,
        "repo_kebab_name": cfg.kebab_name,
        "repo_workspace_name": cfg.output_dir.name or cfg.kebab_name,
        "primary_package_snake_name": cfg.primary_package_snake_name,
        "kindling_version": _kindling_version(),
    }


def _package_ctx(cfg: PackageScaffoldConfig) -> dict:
    return {
        "snake_name": cfg.snake_name,
        "kebab_name": cfg.kebab_name,
        "repo_snake_name": cfg.repo_snake_name,
        "repo_kebab_name": cfg.repo_kebab_name,
        "auth": cfg.auth,
        "layers": cfg.layers,
        "integration": cfg.integration,
        "primary_package_snake_name": cfg.snake_name,
        "kindling_version": _kindling_version(),
    }


def _app_ctx(cfg: AppScaffoldConfig) -> dict:
    return {
        "snake_name": cfg.snake_name,
        "kebab_name": cfg.kebab_name,
        "package_snake_name": cfg.package_snake_name,
        "package_kebab_name": cfg.package_kebab_name,
        "repo_snake_name": cfg.repo_snake_name,
        "repo_kebab_name": cfg.repo_kebab_name,
        "auth": cfg.auth,
        "layers": cfg.layers,
        "pattern": cfg.pattern,
        "kindling_version": _kindling_version(),
    }


def _render(env: jinja2.Environment, template_name: str, ctx: dict) -> str:
    return env.get_template(template_name).render(ctx)


def generate_repo(cfg: RepoScaffoldConfig) -> List[Path]:
    root = cfg.output_dir
    root.mkdir(parents=True, exist_ok=True)
    devcontainer_dir = root / ".devcontainer"
    skip_devcontainer = devcontainer_dir.exists() and not cfg.overwrite_devcontainer

    env = _make_env(cfg.template_dir)
    ctx = _repo_ctx(cfg)
    files: List[Path] = []

    def _write(rel: str, template_name: str, *, overwrite: bool = False) -> None:
        path = root / rel
        if path.exists() and not overwrite:
            raise FileExistsError(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(_render(env, template_name, ctx), encoding="utf-8")
        files.append(path)

    (root / "packages").mkdir(parents=True, exist_ok=True)
    (root / "apps").mkdir(parents=True, exist_ok=True)

    _write(".gitignore", ".gitignore.j2")
    _write(".github/workflows/ci.yml", ".github/workflows/ci.yml.j2")
    if not skip_devcontainer:
        _write(
            ".devcontainer/devcontainer.json",
            ".devcontainer/devcontainer.json.j2",
            overwrite=cfg.overwrite_devcontainer,
        )
    _write("scripts/setup-local-dev.sh", "scripts/setup-local-dev.sh.j2")

    return files


def generate_package(cfg: PackageScaffoldConfig) -> List[Path]:
    root = cfg.repo_root / "packages" / cfg.snake_name
    root.mkdir(parents=True, exist_ok=False)

    env = _make_env(cfg.template_dir)
    ctx = _package_ctx(cfg)
    pkg = cfg.snake_name
    layer = cfg.layers
    files: List[Path] = []

    def _write(rel: str, content: str) -> Path:
        p = root / rel
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(content, encoding="utf-8")
        files.append(p)
        return p

    _write("src/__init__.py", "")
    _write(f"src/{pkg}/__init__.py", "")

    _write(f"src/{pkg}/entities/__init__.py", "")
    _write(
        f"src/{pkg}/entities/records.py",
        _render(env, f"src/entities/records.{layer}.py.j2", ctx),
    )

    _write(f"src/{pkg}/pipes/__init__.py", "")
    pipe_file = "bronze_to_silver" if layer == "medallion" else "process"
    _write(
        f"src/{pkg}/pipes/{pipe_file}.py",
        _render(env, f"src/pipes/{pipe_file}.py.j2", ctx),
    )

    _write(f"src/{pkg}/transforms/__init__.py", "")
    _write(
        f"src/{pkg}/transforms/quality.py",
        _render(env, f"src/transforms/quality.{layer}.py.j2", ctx),
    )

    _write("settings.yaml", _render(env, "settings.yaml.j2", ctx))
    _write("settings.local.yaml", _render(env, "settings.local.yaml.j2", ctx))

    _write("tests/__init__.py", "")
    _write("tests/conftest.py", _render(env, "tests/conftest.py.j2", ctx))
    _write("tests/unit/__init__.py", "")
    _write("tests/unit/test_transforms.py", _render(env, "tests/unit/test_transforms.py.j2", ctx))
    _write("tests/component/__init__.py", "")
    _write(
        "tests/component/test_registration.py",
        _render(env, "tests/component/test_registration.py.j2", ctx),
    )

    if cfg.integration:
        _write("tests/integration/__init__.py", "")
        _write(
            "tests/integration/test_pipeline_azure.py",
            _render(env, "tests/integration/test_pipeline_azure.py.j2", ctx),
        )
        _write(
            "tests/integration/test_pipeline_local.py",
            _render(env, "tests/integration/test_pipeline_local.py.j2", ctx),
        )

    _write("pyproject.toml", _render(env, "pyproject.toml.j2", ctx))
    _write(".env.example", _render(env, ".env.example.j2", ctx))
    _write("QUICKSTART.md", _render(env, "package/PACKAGE_QUICKSTART.md.j2", ctx))

    return files


def generate_app(cfg: AppScaffoldConfig) -> List[Path]:
    root = cfg.repo_root / "apps" / cfg.snake_name
    root.mkdir(parents=True, exist_ok=False)

    env = _make_env(cfg.template_dir)
    ctx = _app_ctx(cfg)
    files: List[Path] = []

    def _write(rel: str, content: str) -> Path:
        p = root / rel
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(content, encoding="utf-8")
        files.append(p)
        return p

    app_template = f"app.{cfg.pattern}.py.j2" if cfg.pattern else "app.py.j2"
    _write("app.py", _render(env, app_template, ctx))
    _write("app.yaml", _render(env, "app.yaml.j2", ctx))
    _write("lake-reqs.txt", _render(env, "lake-reqs.txt.j2", ctx))
    _write("settings.yaml", _render(env, "settings.yaml.j2", ctx))
    _write("settings.local.yaml", _render(env, "settings.local.yaml.j2", ctx))
    _write(".env.example", _render(env, ".env.example.j2", ctx))
    _write("QUICKSTART.md", _render(env, "package/QUICKSTART.md.j2", ctx))

    # Fixture CSVs — seed data for local runs without cloud storage
    if cfg.layers == "medallion":
        _write("tests/entities/bronze/records.csv", "id,date,value\n1,2024-01-01,hello\n")
    else:
        _write("tests/entities/raw/records.csv", "id,value\n1,hello\n")

    return files
