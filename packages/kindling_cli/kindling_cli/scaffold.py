"""Scaffold generator for `kindling new <project>`.

Templates live in kindling_cli/templates/ as Jinja2 .j2 files.
Pass --template-dir to overlay custom templates on top of the built-ins.
"""

from __future__ import annotations

import keyword
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import List, Optional

import jinja2

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

_BUILTIN_TEMPLATES = Path(__file__).parent / "templates"


@dataclass
class ScaffoldConfig:
    name: str  # raw name supplied by user, e.g. "my-project" or "my_project"
    layers: str = "medallion"  # "medallion" | "minimal"
    auth: str = "oauth"  # "oauth" | "key" | "cli"
    integration: bool = True
    output_dir: Path = field(default_factory=Path.cwd)
    template_dir: Optional[Path] = None

    @property
    def snake_name(self) -> str:
        return _to_snake(self.name)

    @property
    def kebab_name(self) -> str:
        return self.snake_name.replace("_", "-")


def _to_snake(name: str) -> str:
    """Normalize a project name to a valid Python identifier (snake_case)."""
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
    """Return the snake_case form of *name* or raise ValueError."""
    return _to_snake(name)


# ---------------------------------------------------------------------------
# Jinja2 environment
# ---------------------------------------------------------------------------


def _make_env(cfg: ScaffoldConfig) -> jinja2.Environment:
    loaders: list[jinja2.BaseLoader] = []
    if cfg.template_dir:
        loaders.append(jinja2.FileSystemLoader(str(cfg.template_dir)))
    loaders.append(jinja2.FileSystemLoader(str(_BUILTIN_TEMPLATES)))
    return jinja2.Environment(
        loader=jinja2.ChoiceLoader(loaders),
        keep_trailing_newline=True,
        undefined=jinja2.StrictUndefined,
    )


def _ctx(cfg: ScaffoldConfig) -> dict:
    return {
        "snake_name": cfg.snake_name,
        "kebab_name": cfg.kebab_name,
        "auth": cfg.auth,
        "layers": cfg.layers,
        "integration": cfg.integration,
    }


def _render(env: jinja2.Environment, template_name: str, cfg: ScaffoldConfig) -> str:
    return env.get_template(template_name).render(_ctx(cfg))


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------


def generate_project(cfg: ScaffoldConfig) -> List[Path]:
    """Write scaffolded project to *cfg.output_dir / cfg.snake_name*.

    Returns the list of files created.
    """
    root = cfg.output_dir / cfg.snake_name
    root.mkdir(parents=True, exist_ok=False)

    env = _make_env(cfg)
    pkg = cfg.snake_name
    layer = cfg.layers
    files: List[Path] = []

    def _write(rel: str, content: str) -> Path:
        p = root / rel
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(content, encoding="utf-8")
        files.append(p)
        return p

    # -- Package source -------------------------------------------------------
    _write("src/__init__.py", "")
    _write(f"src/{pkg}/__init__.py", "")
    _write(f"src/{pkg}/app.py", _render(env, "app.py.j2", cfg))

    _write(f"src/{pkg}/entities/__init__.py", "")
    _write(
        f"src/{pkg}/entities/records.py",
        _render(env, f"src/entities/records.{layer}.py.j2", cfg),
    )

    _write(f"src/{pkg}/pipes/__init__.py", "")
    pipe_file = "bronze_to_silver" if layer == "medallion" else "process"
    _write(
        f"src/{pkg}/pipes/{pipe_file}.py",
        _render(env, f"src/pipes/{pipe_file}.py.j2", cfg),
    )

    _write(f"src/{pkg}/transforms/__init__.py", "")
    _write(
        f"src/{pkg}/transforms/quality.py",
        _render(env, f"src/transforms/quality.{layer}.py.j2", cfg),
    )

    # -- Config ---------------------------------------------------------------
    _write("config/settings.yaml", _render(env, "config/settings.yaml.j2", cfg))
    _write("config/env.local.yaml", _render(env, "config/env.local.yaml.j2", cfg))

    # -- Tests ----------------------------------------------------------------
    _write("tests/__init__.py", "")
    _write("tests/conftest.py", _render(env, "tests/conftest.py.j2", cfg))

    _write("tests/unit/__init__.py", "")
    _write("tests/unit/test_transforms.py", _render(env, "tests/unit/test_transforms.py.j2", cfg))

    _write("tests/component/__init__.py", "")
    _write(
        "tests/component/test_registration.py",
        _render(env, "tests/component/test_registration.py.j2", cfg),
    )

    if cfg.integration:
        _write("tests/integration/__init__.py", "")
        _write(
            "tests/integration/test_pipeline.py",
            _render(env, "tests/integration/test_pipeline.py.j2", cfg),
        )

    # -- Project metadata -----------------------------------------------------
    _write("pyproject.toml", _render(env, "pyproject.toml.j2", cfg))
    _write(".env.example", _render(env, ".env.example.j2", cfg))
    _write(".gitignore", _render(env, ".gitignore.j2", cfg))
    _write(".devcontainer/Dockerfile", _render(env, ".devcontainer/Dockerfile.j2", cfg))
    _write(
        ".devcontainer/devcontainer.json", _render(env, ".devcontainer/devcontainer.json.j2", cfg)
    )
    _write(
        ".devcontainer/docker-compose.yml", _render(env, ".devcontainer/docker-compose.yml.j2", cfg)
    )

    return files
