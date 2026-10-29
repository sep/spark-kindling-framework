#!/usr/bin/env python3
"""Check that each extension installs on the supported runtimes without
replacing their key packages.

The cluster bootstrap installs an extension's requirements with the
runtime's key packages pinned at their installed versions, and drops the
pins (replacing them) only when the requirements can't be met otherwise.
This resolves each extension's requirements, with those of the Kindling
distributions it requires taken from this repo (`uv pip compile`, against the
package index) with the key package versions each runtime ships pinned, and
fails when a runtime would need one of them replaced.

    poe check-extension-runtimes [--extension otel_azure] [--runtime dbr17.3]

The runtimes and the key packages each ships come from
kindling.supported_runtimes (packages/kindling/supported_runtimes.py), the
list of runtimes Kindling is system-tested on; update it there.
"""

from __future__ import annotations

import argparse
import importlib.util
import subprocess
import sys
import tempfile
from pathlib import Path

try:
    import tomllib
except ModuleNotFoundError:  # Python 3.10
    import tomli as tomllib  # type: ignore[no-redef]

REPO_ROOT = Path(__file__).resolve().parent.parent
EXTENSIONS_DIR = REPO_ROOT / "packages" / "extensions"


def _load_supported_runtimes():
    """kindling.supported_runtimes, loaded by file path so this check never
    imports Kindling (or Spark)."""
    path = REPO_ROOT / "packages" / "kindling" / "supported_runtimes.py"
    spec = importlib.util.spec_from_file_location("_kindling_supported_runtimes", path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module  # dataclasses resolve their module
    spec.loader.exec_module(module)
    return module.SUPPORTED_RUNTIMES


RUNTIMES = {
    runtime.name: (runtime.python, dict(runtime.packages)) for runtime in _load_supported_runtimes()
}


def _requirement_name(requirement: str) -> str:
    name = requirement.split(";")[0]
    for separator in ("[", "=", "<", ">", "!", "~", " "):
        name = name.split(separator)[0]
    return name.strip().lower().replace("_", "-")


def _is_kindling_requirement(requirement: str) -> bool:
    return _requirement_name(requirement).startswith("spark-kindling")


def _requirement_extras(requirement: str) -> list[str]:
    head = requirement.split(";")[0]
    if "[" not in head:
        return []
    return [extra.strip() for extra in head.split("[", 1)[1].split("]", 1)[0].split(",")]


def _workspace_projects() -> dict[str, dict]:
    """The ``[project]`` table of each Kindling distribution in this repo."""
    projects = {}
    for pyproject in [REPO_ROOT / "pyproject.toml", *REPO_ROOT.glob("packages/**/pyproject.toml")]:
        project = tomllib.loads(pyproject.read_text()).get("project", {})
        if _is_kindling_requirement(project.get("name", "")):
            projects[_requirement_name(project["name"])] = project
    return projects


def extension_requirements(extension_dir: Path) -> list[str]:
    """The requirements pip processes when the bootstrap installs the extension.

    Its Kindling requirements (``spark-kindling``, other extensions) are
    followed through this repo's copies, which are the versions the cluster
    has installed, so their own requirements are checked too.
    """
    projects = _workspace_projects()
    root = tomllib.loads((extension_dir / "pyproject.toml").read_text())["project"]
    requirements: list[str] = []
    seen: set[tuple[str, str]] = set()
    pending = [(root, "")]
    while pending:
        project, extra = pending.pop()
        key = (_requirement_name(project["name"]), extra)
        if key in seen:
            continue
        seen.add(key)
        listed = (
            project.get("optional-dependencies", {}).get(extra, [])
            if extra
            else project.get("dependencies", [])
        )
        for requirement in listed:
            if not _is_kindling_requirement(requirement):
                if requirement not in requirements:
                    requirements.append(requirement)
                continue
            dependency = projects.get(_requirement_name(requirement))
            if dependency is None:
                raise SystemExit(f"{requirement!r} is not a Kindling distribution in this repo")
            pending.append((dependency, ""))
            pending.extend((dependency, name) for name in _requirement_extras(requirement))
    return requirements


def resolve(
    requirements: list[str], python_version: str, pins: dict
) -> subprocess.CompletedProcess:
    """`uv pip compile` the requirements with the pins as constraints."""
    with tempfile.TemporaryDirectory(prefix="kindling_runtime_check_") as work_dir:
        requirements_file = Path(work_dir) / "requirements.in"
        constraints_file = Path(work_dir) / "constraints.txt"
        requirements_file.write_text("".join(f"{line}\n" for line in requirements))
        constraints_file.write_text("".join(f"{n}=={v}\n" for n, v in sorted(pins.items())))
        return subprocess.run(
            [
                "uv",
                "pip",
                "compile",
                "--quiet",
                "--no-header",
                "--python-version",
                python_version,
                "--python-platform",
                "linux",
                "-c",
                str(constraints_file),
                str(requirements_file),
            ],
            capture_output=True,
            text=True,
        )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--extension", action="append", default=[], help="e.g. otel_azure")
    parser.add_argument("--runtime", action="append", default=[], choices=sorted(RUNTIMES))
    args = parser.parse_args(argv)

    extension_dirs = sorted(EXTENSIONS_DIR.glob("kindling_ext_*"))
    if args.extension:
        wanted = {f"kindling_ext_{name.replace('-', '_')}" for name in args.extension}
        unknown = sorted(wanted - {path.name for path in extension_dirs})
        if unknown:
            parser.error(
                f"unknown extension(s): {', '.join(unknown)}; "
                f"known: {', '.join(path.name for path in extension_dirs)}"
            )
        extension_dirs = [path for path in extension_dirs if path.name in wanted]
    runtimes = args.runtime or sorted(RUNTIMES)

    failures = 0
    for extension_dir in extension_dirs:
        requirements = extension_requirements(extension_dir)
        if not requirements:
            continue
        for runtime in runtimes:
            python_version, pins = RUNTIMES[runtime]
            result = resolve(requirements, python_version, pins)
            if result.returncode == 0:
                resolved = [
                    line.strip()
                    for line in result.stdout.splitlines()
                    if line.strip() and not line.lstrip().startswith("#")
                ]
                direct = {_requirement_name(requirement) for requirement in requirements}
                print(f"ok    {extension_dir.name} on {runtime}: {len(resolved)} packages")
                for line in resolved:
                    name = _requirement_name(line)
                    if name in pins:
                        print(f"        keeps    {line}")
                    elif name in direct:
                        print(f"        installs {line}")
            else:
                failures += 1
                print(
                    f"FAIL  {extension_dir.name} on {runtime}: its requirements need a "
                    f"key package the runtime ships replaced\n{result.stderr.strip()}"
                )
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
