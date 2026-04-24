"""Scaffold generators for Kindling repos and packages."""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import List, Optional

import jinja2

_BUILTIN_TEMPLATES = Path(__file__).parent / "templates"


@dataclass
class RepoScaffoldConfig:
    name: str
    output_dir: Path = field(default_factory=Path.cwd)
    template_dir: Optional[Path] = None
    primary_package_name: Optional[str] = None

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


# Backward-compatible alias for older imports/tests.
ScaffoldConfig = PackageScaffoldConfig


def _to_snake(name: str) -> str:
    s = name.strip().lower()
    s = re.sub(r"[^a-z0-9]+", "_", s)
    s = s.strip("_")
    if not s or s[0].isdigit():
        raise ValueError(f"Project name {name!r} cannot be converted to a valid Python identifier.")
    return s


def validate_name(name: str) -> str:
    return _to_snake(name)


def _make_env(template_dir: Optional[Path]) -> jinja2.Environment:
    loaders: list[jinja2.BaseLoader] = []
    if template_dir:
        loaders.append(jinja2.FileSystemLoader(str(template_dir)))
    loaders.append(jinja2.FileSystemLoader(str(_BUILTIN_TEMPLATES)))
    return jinja2.Environment(
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
        "primary_package_snake_name": cfg.primary_package_snake_name,
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
    }


def _render(env: jinja2.Environment, template_name: str, ctx: dict) -> str:
    return env.get_template(template_name).render(ctx)


def generate_repo(cfg: RepoScaffoldConfig) -> List[Path]:
    root = cfg.output_dir / cfg.snake_name
    root.mkdir(parents=True, exist_ok=False)

    env = _make_env(cfg.template_dir)
    ctx = _repo_ctx(cfg)
    files: List[Path] = []

    def _write(rel: str, template_name: str) -> None:
        path = root / rel
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(_render(env, template_name, ctx), encoding="utf-8")
        files.append(path)

    (root / "packages").mkdir(parents=True, exist_ok=True)

    _write(".gitignore", ".gitignore.j2")
    _write(".github/workflows/ci.yml", ".github/workflows/ci.yml.j2")
    _write(".devcontainer/Dockerfile", ".devcontainer/Dockerfile.j2")
    _write(".devcontainer/devcontainer.json", ".devcontainer/devcontainer.json.j2")
    _write(".devcontainer/docker-compose.yml", ".devcontainer/docker-compose.yml.j2")

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
    _write(f"src/{pkg}/app.py", _render(env, "app.py.j2", ctx))

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

    _write("config/settings.yaml", _render(env, "config/settings.yaml.j2", ctx))
    _write("config/env.local.yaml", _render(env, "config/env.local.yaml.j2", ctx))

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
            "tests/integration/test_pipeline.py",
            _render(env, "tests/integration/test_pipeline.py.j2", ctx),
        )

    _write("pyproject.toml", _render(env, "pyproject.toml.j2", ctx))
    _write(".env.example", _render(env, ".env.example.j2", ctx))

    return files


def generate_project(cfg: PackageScaffoldConfig) -> List[Path]:
    repo_cfg = RepoScaffoldConfig(
        name=cfg.name,
        output_dir=cfg.repo_root.parent,
        template_dir=cfg.template_dir,
        primary_package_name=cfg.name,
    )
    files = generate_repo(repo_cfg)
    package_cfg = PackageScaffoldConfig(
        name=cfg.name,
        repo_root=repo_cfg.output_dir / repo_cfg.snake_name,
        layers=cfg.layers,
        auth=cfg.auth,
        integration=cfg.integration,
        template_dir=cfg.template_dir,
    )
    files.extend(generate_package(package_cfg))
    return files
