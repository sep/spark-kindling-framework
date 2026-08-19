#!/usr/bin/env python3
"""One-off script to migrate a Kindling domain project's pyproject.toml from
Poetry's schema to uv's (PEP 621 + [tool.uv.sources] + [dependency-groups]),
and switch its build backend to uv_build.

Not a kindling_cli command -- this is meant to run three times, once per
existing Poetry-based domain project, then be deleted. See
docs/proposals/poetry_to_uv_migration.md for the full rationale.

Usage:
    python scripts/migrate_domain_project_to_uv.py --project /path/to/project
    python scripts/migrate_domain_project_to_uv.py --project /path/to/project \\
        --workspace-root /path/to/monorepo/root
"""

import argparse
import re
import subprocess  # nosec B404 -- invokes `uv`, a fixed, non-shell command list
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

try:
    import tomllib
except ImportError:
    import tomli as tomllib  # type: ignore[no-redef]


def _load_pyproject_toml(pyproject_path: Path) -> Dict[str, Any]:
    return tomllib.loads(pyproject_path.read_text(encoding="utf-8"))


def _canonical_distribution_name(name: str) -> str:
    return re.sub(r"[-_.]+", "-", name).lower()


def _poetry_authors_to_pep621(authors: List[str]) -> List[Dict[str, str]]:
    """Convert Poetry's `authors = ["Name <email>", ...]` to PEP 621's
    `authors = [{name=..., email=...}, ...]`."""
    result: List[Dict[str, str]] = []
    for author in authors:
        match = re.match(r"^\s*(.*?)\s*<([^<>]+)>\s*$", author)
        if match:
            name, email = match.group(1), match.group(2)
            result.append({"name": name, "email": email} if name else {"email": email})
        elif author.strip():
            result.append({"name": author.strip()})
    return result


_CARET_SPECIFIER_RE = re.compile(r"^\^(\d+)(?:\.(\d+))?(?:\.(\d+))?$")


def _convert_caret_specifier(specifier: str) -> Optional[str]:
    """Convert a Poetry caret range (e.g. '^3.10') to a PEP 508 specifier
    (e.g. '>=3.10,<4.0'), per Poetry's own caret semantics: the leftmost
    nonzero component is the one allowed to bump."""
    match = _CARET_SPECIFIER_RE.match(specifier.strip())
    if not match:
        return None
    major = int(match.group(1))
    minor = int(match.group(2)) if match.group(2) is not None else None
    if major > 0:
        upper = f"{major + 1}.0.0"
    elif minor:
        upper = f"0.{minor + 1}.0"
    else:
        upper = "0.0.1"
    lower = specifier.strip()[1:]
    return f">={lower},<{upper}"


def _poetry_python_constraint_to_requires_python(constraint: str) -> str:
    converted = _convert_caret_specifier(constraint)
    return converted if converted is not None else constraint.strip()


def _poetry_dependency_to_pep508(name: str, entry: Any) -> Tuple[str, Optional[Dict[str, Any]]]:
    """Convert one non-Kindling Poetry dependency entry to a (PEP 508
    requirement string, uv [tool.uv.sources] override or None) pair.

    Best-effort: a specifier this can't confidently convert (e.g. an
    unusual constraint table) is dropped, leaving the bare package name --
    `uv add`/manual review can tighten it afterward, which is safer than
    guessing wrong.
    """
    if isinstance(entry, str):
        specifier = entry.strip()
        if specifier in ("", "*"):
            return name, None
        if specifier[0] in ">=<~!":
            return f"{name}{specifier}", None
        caret_converted = _convert_caret_specifier(specifier)
        if caret_converted is not None:
            return f"{name}{caret_converted}", None
        return name, None
    if isinstance(entry, dict):
        extras = entry.get("extras")
        extras_suffix = f"[{','.join(extras)}]" if isinstance(extras, list) and extras else ""
        if isinstance(entry.get("url"), str):
            return f"{name}{extras_suffix}", {"url": entry["url"]}
        if isinstance(entry.get("path"), str):
            return f"{name}{extras_suffix}", {"path": entry["path"]}
        version = entry.get("version")
        if isinstance(version, str) and version.strip():
            specifier = version.strip()
            if specifier[0] in ">=<~!":
                return f"{name}{extras_suffix}{specifier}", None
            caret_converted = _convert_caret_specifier(specifier)
            if caret_converted is not None:
                return f"{name}{extras_suffix}{caret_converted}", None
        return f"{name}{extras_suffix}", None
    return name, None


_TOML_TOP_LEVEL_HEADER_RE = re.compile(r"^(\[+)([^\[\]]+)\]+", re.MULTILINE)


def _extract_raw_toml_sections(text: str, table_prefixes: Tuple[str, ...]) -> str:
    """Return the exact original text (comments, formatting, and all) of
    every top-level TOML table whose dotted name matches one of
    table_prefixes, by slicing between successive top-level [...] headers
    rather than re-serializing parsed data. Used to carry sections this
    migration doesn't rewrite (poe tasks, pytest config, ...) through
    untouched.
    """
    headers = list(_TOML_TOP_LEVEL_HEADER_RE.finditer(text))
    kept_blocks = []
    for i, match in enumerate(headers):
        brackets, header_name = match.group(1), match.group(2).strip()
        if brackets != "[":  # skip [[array-of-tables]] headers -- unused by our templates
            continue
        if any(
            header_name == prefix or header_name.startswith(prefix + ".")
            for prefix in table_prefixes
        ):
            start = match.start()
            end = headers[i + 1].start() if i + 1 < len(headers) else len(text)
            kept_blocks.append(text[start:end].rstrip("\n"))
    return "\n\n".join(kept_blocks)


def render_uv_pyproject_toml(data: Dict[str, Any], original_text: str) -> str:
    """Render a uv/PEP 621-schema pyproject.toml equivalent to a Poetry-schema
    one, converting [tool.poetry.*] to [project]/[tool.uv.sources]/
    [dependency-groups] and switching the build backend to uv_build. Every
    other top-level tool table (poe tasks, pytest config, ...) is carried
    through as the original, unmodified text.
    """
    poetry = data.get("tool", {}).get("poetry", {})
    name = poetry.get("name", "")
    version = poetry.get("version", "0.1.0")
    description = poetry.get("description")
    readme = poetry.get("readme")
    authors = _poetry_authors_to_pep621(poetry.get("authors") or [])

    main_deps = poetry.get("dependencies", {})
    python_constraint = main_deps.get("python") if isinstance(main_deps, dict) else None

    dependencies: List[str] = []
    sources: Dict[str, Dict[str, Any]] = {}
    if isinstance(main_deps, dict):
        for dep_name, entry in main_deps.items():
            if dep_name == "python":
                continue
            requirement, source = _poetry_dependency_to_pep508(dep_name, entry)
            dependencies.append(requirement)
            if source is not None:
                sources[dep_name] = source

    groups: Dict[str, List[str]] = {}
    poetry_groups = poetry.get("group", {})
    if isinstance(poetry_groups, dict):
        for group_name, group_data in poetry_groups.items():
            group_deps = group_data.get("dependencies", {}) if isinstance(group_data, dict) else {}
            reqs: List[str] = []
            if isinstance(group_deps, dict):
                for dep_name, entry in group_deps.items():
                    requirement, source = _poetry_dependency_to_pep508(dep_name, entry)
                    reqs.append(requirement)
                    if source is not None:
                        sources[dep_name] = source
            groups[group_name] = reqs

    lines: List[str] = ["[project]", f'name = "{name}"', f'version = "{version}"']
    if description:
        lines.append(f'description = "{description}"')
    if readme:
        lines.append(f'readme = "{readme}"')
    if authors:
        authors_toml = ", ".join(
            "{ " + ", ".join(f'{k} = "{v}"' for k, v in author.items()) + " }" for author in authors
        )
        lines.append(f"authors = [{authors_toml}]")
    else:
        lines.append("authors = []")
    if python_constraint:
        requires_python = _poetry_python_constraint_to_requires_python(str(python_constraint))
        lines.append(f'requires-python = "{requires_python}"')
    if dependencies:
        deps_toml = ",\n    ".join(f'"{d}"' for d in dependencies)
        lines.append(f"dependencies = [\n    {deps_toml},\n]")
    else:
        lines.append("dependencies = []")

    lines += [
        "",
        "[build-system]",
        'requires = ["uv_build>=0.12.5,<0.13.0"]',
        'build-backend = "uv_build"',
    ]

    if sources:
        lines += ["", "[tool.uv.sources]"]
        for dep_name in sorted(sources):
            source_toml = ", ".join(f'{k} = "{v}"' for k, v in sources[dep_name].items())
            lines.append(f"{dep_name} = {{ {source_toml} }}")

    if groups:
        lines += ["", "[dependency-groups]"]
        for group_name in sorted(groups):
            reqs_toml = ", ".join(f'"{r}"' for r in groups[group_name])
            lines.append(f"{group_name} = [{reqs_toml}]")

    preserved = _extract_raw_toml_sections(
        original_text,
        tuple(
            f"tool.{tool_name}"
            for tool_name in data.get("tool", {})
            if tool_name not in ("poetry", "uv")
        ),
    )
    # Poe's `build` task is the one command line in a preserved-verbatim
    # section that's actually Poetry-specific -- everything else there
    # (test tasks, pytest config, ...) is dependency-manager-agnostic.
    preserved = preserved.replace('"poetry build"', '"uv build"')
    if preserved:
        lines += ["", preserved]

    return "\n".join(lines) + "\n"


def ensure_uv_workspace_member(workspace_root: Path, member_path: Path) -> None:
    """Ensure workspace_root has a uv workspace pyproject.toml declaring
    member_path as a member, creating a minimal non-buildable root project
    if none exists yet. Idempotent -- already-declared members are left
    alone.
    """
    workspace_root.mkdir(parents=True, exist_ok=True)
    root_pyproject_path = workspace_root / "pyproject.toml"
    try:
        relative_member = member_path.resolve().relative_to(workspace_root)
    except ValueError as exc:
        raise SystemExit(f"{member_path} is not inside workspace root {workspace_root}.") from exc
    member_str = relative_member.as_posix()

    if not root_pyproject_path.exists():
        root_name = _canonical_distribution_name(workspace_root.name) or "workspace-root"
        root_pyproject_path.write_text(
            "[project]\n"
            f'name = "{root_name}"\n'
            'version = "0.0.0"\n'
            "dependencies = []\n\n"
            "[tool.uv]\n"
            "package = false\n\n"
            "[tool.uv.workspace]\n"
            f'members = ["{member_str}"]\n',
            encoding="utf-8",
        )
        print(f"Created workspace root {root_pyproject_path} with member {member_str}.")
        return

    root_data = _load_pyproject_toml(root_pyproject_path)
    existing_members = (
        root_data.get("tool", {}).get("uv", {}).get("workspace", {}).get("members", [])
    )
    if not isinstance(existing_members, list):
        existing_members = []
    if member_str in existing_members:
        print(f"{root_pyproject_path} already declares {member_str} as a workspace member.")
        return

    text = root_pyproject_path.read_text(encoding="utf-8")
    has_workspace_table = "workspace" in root_data.get("tool", {}).get("uv", {})
    if has_workspace_table:
        updated_members = existing_members + [member_str]
        members_toml = ", ".join(f'"{m}"' for m in updated_members)
        if "members" in root_data.get("tool", {}).get("uv", {}).get("workspace", {}):
            new_text = re.sub(
                r"members\s*=\s*\[[^\]]*\]", f"members = [{members_toml}]", text, count=1
            )
        else:
            new_text = re.sub(
                r"(\[tool\.uv\.workspace\])", f"\\1\nmembers = [{members_toml}]", text, count=1
            )
    else:
        addition = f'\n[tool.uv.workspace]\nmembers = ["{member_str}"]\n'
        new_text = text.rstrip("\n") + "\n" + addition
    root_pyproject_path.write_text(new_text, encoding="utf-8")
    print(f"Added {member_str} as a workspace member in {root_pyproject_path}.")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--project", type=Path, default=Path("."), help="Poetry-schema project to migrate."
    )
    parser.add_argument(
        "--workspace-root",
        type=Path,
        default=None,
        help="Monorepo root to declare/extend as a uv workspace with this project as a member.",
    )
    parser.add_argument("--no-sync", action="store_true", help="Skip running `uv sync` afterward.")
    args = parser.parse_args()

    project_path = args.project.expanduser().resolve()
    pyproject_path = project_path / "pyproject.toml"
    if not pyproject_path.exists():
        raise SystemExit(f"No pyproject.toml found at {project_path}.")

    original_text = pyproject_path.read_text(encoding="utf-8")
    data = _load_pyproject_toml(pyproject_path)
    if "poetry" not in data.get("tool", {}):
        print(f"{pyproject_path} does not look like a Poetry project ([tool.poetry] not found).")
        sys.exit(1)

    migrated_text = render_uv_pyproject_toml(data, original_text)
    pyproject_path.write_text(migrated_text, encoding="utf-8")
    print(f"Migrated {pyproject_path} from Poetry to uv.")

    lock_path = project_path / "poetry.lock"
    if lock_path.exists():
        lock_path.unlink()
        print(f"Removed {lock_path} (superseded by uv.lock).")

    if args.workspace_root is not None:
        ensure_uv_workspace_member(args.workspace_root.expanduser().resolve(), project_path)

    sync_command = ["uv", "sync"]
    if args.no_sync:
        sync_command.append("--inexact")
    subprocess.run(sync_command, cwd=project_path, check=True)  # nosec B603

    print(f"\n{project_path} is on uv.")


if __name__ == "__main__":
    main()
