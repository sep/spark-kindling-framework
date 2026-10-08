#!/usr/bin/env python3
"""
Version bumping utility for Kindling framework.

Supports semantic versioning with alpha releases.
Optionally rebuilds and deploys to specified platform after bumping.
"""

import re
import subprocess
import sys
from pathlib import Path

VERSIONED_PYPROJECTS = [
    Path("pyproject.toml"),
    Path("packages/kindling_sdk/pyproject.toml"),
    Path("packages/kindling_cli/pyproject.toml"),
]


def get_current_version() -> str:
    """Read current version from pyproject.toml"""
    pyproject_path = Path(__file__).parent.parent / "pyproject.toml"
    content = pyproject_path.read_text()

    match = re.search(r'^version\s*=\s*"([^"]+)"', content, re.MULTILINE)
    if not match:
        raise ValueError("Could not find version in pyproject.toml")

    return match.group(1)


def parse_version(version: str) -> dict:
    """Parse semantic version string into components"""
    # Match: major.minor.patch or major.minor.patch(alpha|beta|rc)N
    pattern = r"^(\d+)\.(\d+)\.(\d+)(?:(a|alpha|b|beta|rc)(\d+)?)?$"
    match = re.match(pattern, version)

    if not match:
        raise ValueError(f"Invalid version format: {version}")

    major, minor, patch, pre_type, pre_num = match.groups()

    return {
        "major": int(major),
        "minor": int(minor),
        "patch": int(patch),
        "pre_type": pre_type,
        "pre_num": int(pre_num) if pre_num else None,
    }


def bump_version(current: str, bump_type: str) -> str:
    """
    Bump version according to strategy.

    Args:
        current: Current version string (e.g., "0.4.1a1")
        bump_type: One of: alpha, rc, release, patch, minor, major

    Returns:
        New version string
    """
    parts = parse_version(current)

    if bump_type == "alpha":
        # If already alpha, increment alpha number
        if parts["pre_type"] in ("a", "alpha"):
            pre_num = (parts["pre_num"] or 0) + 1
            return f"{parts['major']}.{parts['minor']}.{parts['patch']}a{pre_num}"
        else:
            # Start new alpha series (bump patch)
            return f"{parts['major']}.{parts['minor']}.{parts['patch'] + 1}a1"

    elif bump_type == "rc":
        # Next release candidate: rcN -> rc(N+1); from a final version, the
        # first candidate of the next minor (0.13.1 -> 0.14.0rc1).
        if parts["pre_type"] == "rc":
            return (
                f"{parts['major']}.{parts['minor']}.{parts['patch']}rc{(parts['pre_num'] or 0) + 1}"
            )
        if parts["pre_type"]:
            return f"{parts['major']}.{parts['minor']}.{parts['patch']}rc1"
        return f"{parts['major']}.{parts['minor'] + 1}.0rc1"

    elif bump_type == "release":
        # Finalize a prerelease: 0.14.0rc2 -> 0.14.0.
        if not parts["pre_type"]:
            raise ValueError(f"{current} is not a prerelease; nothing to finalize")
        return f"{parts['major']}.{parts['minor']}.{parts['patch']}"

    elif bump_type == "patch":
        # Remove pre-release if exists, otherwise bump patch
        if parts["pre_type"]:
            return f"{parts['major']}.{parts['minor']}.{parts['patch']}"
        else:
            return f"{parts['major']}.{parts['minor']}.{parts['patch'] + 1}"

    elif bump_type == "minor":
        # Bump minor, reset patch
        return f"{parts['major']}.{parts['minor'] + 1}.0"

    elif bump_type == "major":
        # Bump major, reset minor and patch
        return f"{parts['major'] + 1}.0.0"

    else:
        raise ValueError(f"Invalid bump type: {bump_type}")


def update_pyprojects(new_version: str) -> list[Path]:
    """Update version in root and design-time package pyproject files."""
    repo_root = Path(__file__).parent.parent
    updated_paths: list[Path] = []

    for relative_path in VERSIONED_PYPROJECTS:
        pyproject_path = repo_root / relative_path
        if not pyproject_path.exists():
            continue

        content = pyproject_path.read_text()
        new_content = re.sub(
            r'^version\s*=\s*"[^"]+"',
            f'version = "{new_version}"',
            content,
            count=1,
            flags=re.MULTILINE,
        )
        pyproject_path.write_text(new_content)
        updated_paths.append(pyproject_path)

    return updated_paths


EXTENSIONS_DIR = Path("packages/extensions")
_KINDLING_REQUIREMENT_RE = re.compile(
    r"^spark-kindling(-ext-[a-z0-9-]+)?(\[[^\]]*\])?\s*(?:[<>=!~;]|$)"
)


def starts_minor_line(version: str) -> bool:
    """True for X.Y.0 and its prereleases (X.Y.0rcN): the versions every
    extension takes, since an extension's major.minor states which Kindling
    major.minor it works with. Patch releases leave extensions alone; an
    extension's own patch number counts its own fixes and additions."""
    return parse_version(version)["patch"] == 0


def kindling_range(version: str) -> str:
    """The Kindling range an extension released at `version` declares:
    from that version up to, not including, the next minor."""
    parts = parse_version(version)
    return f">={version},<{parts['major']}.{parts['minor'] + 1}"


def _array_span(content: str, key: str) -> tuple[int, int]:
    """(start, end) of the text between the brackets of `key = [...]` at the
    start of a line, matching brackets outside quoted strings and comments."""
    match = re.search(rf"(?m)^{re.escape(key)}\s*=\s*\[", content)
    if not match:
        raise ValueError(f"no {key} array")
    depth, i, quote = 1, match.end(), None
    while i < len(content):
        c = content[i]
        if quote:
            if c == "\\" and quote == '"':
                i += 1
            elif c == quote:
                quote = None
        elif c in "\"'":
            quote = c
        elif c == "#":
            newline = content.find("\n", i)
            i = len(content) if newline == -1 else newline
            continue
        elif c == "[":
            depth += 1
        elif c == "]":
            depth -= 1
            if depth == 0:
                return match.end(), i
        i += 1
    raise ValueError(f"unterminated {key} array")


def _set_kindling_dependencies(content: str, version: str) -> str:
    """Rewrite every spark-kindling / spark-kindling-ext-* requirement in
    [project].dependencies to the minor line's range (adding spark-kindling
    itself when the extension doesn't declare it yet). Other entries and
    comment lines are kept; the array is written one entry per line."""
    start, end = _array_span(content, "dependencies")
    ranged = kindling_range(version)
    kindling: list[str] = []
    kept: list[str] = []
    for line in content[start:end].splitlines():
        stripped = line.strip()
        if stripped.startswith("#"):
            kept.append(f"    {stripped}")
            continue
        for entry in re.findall(r"\"([^\"]+)\"|'([^']+)'", line):
            requirement = entry[0] or entry[1]
            name_match = _KINDLING_REQUIREMENT_RE.match(requirement)
            if name_match:
                name = "spark-kindling" + (name_match.group(1) or "") + (name_match.group(2) or "")
                kindling.append(f'    "{name}{ranged}",')
            else:
                kept.append(f'    "{requirement}",')
    if not any(re.match(r'^\s*"spark-kindling(\[|>)', line) for line in kindling):
        kindling.insert(0, f'    "spark-kindling{ranged}",')
    body = "\n" + "\n".join(kindling + kept) + "\n"
    return content[:start] + body + content[end:]


def align_extensions(new_version: str, repo_root: Path) -> list[Path]:
    """At the start of a minor line, give every extension Kindling's version
    and the matching Kindling range. Returns the files changed."""
    if not starts_minor_line(new_version):
        return []
    changed: list[Path] = []
    for pyproject_path in sorted(
        (repo_root / EXTENSIONS_DIR).glob("kindling_ext_*/pyproject.toml")
    ):
        content = pyproject_path.read_text()
        new_content = re.sub(
            r'^version\s*=\s*"[^"]+"',
            f'version = "{new_version}"',
            content,
            count=1,
            flags=re.MULTILINE,
        )
        new_content = _set_kindling_dependencies(new_content, new_version)
        if new_content != content:
            pyproject_path.write_text(new_content)
            changed.append(pyproject_path)
        package_init = pyproject_path.parent / pyproject_path.parent.name / "__init__.py"
        if package_init.is_file():
            init_text = package_init.read_text()
            new_init = re.sub(
                r'^__version__\s*=\s*"[^"]+"',
                f'__version__ = "{new_version}"',
                init_text,
                count=1,
                flags=re.MULTILINE,
            )
            if new_init != init_text:
                package_init.write_text(new_init)
                changed.append(package_init)
    return changed


def run_command(cmd: list) -> int:
    """Run command and return exit code"""
    print(f"Running: {' '.join(cmd)}")
    result = subprocess.run(cmd)
    return result.returncode


def main(
    bump_type: str = "alpha", platform: str = "", build: bool = False, deploy: bool = False
) -> int:
    """
    Main version bump workflow.

    Args:
        bump_type: Version bump strategy (alpha, patch, minor, major)
        platform: Optional platform to build/deploy (fabric, synapse, databricks)
        build: Whether to rebuild after version bump
        deploy: Whether to deploy after build (implies build=True)

    Returns:
        Exit code (0 = success)

    Examples:
        poe version --type alpha                    # Bump alpha version only
        poe version --type patch                    # Bump patch version only
        poe version --type alpha --build            # Bump + build all
        poe version --type alpha --platform fabric  # Bump + build + deploy Fabric
        poe version --type minor --platform synapse # Bump minor + build + deploy Synapse
    """
    try:
        # Get and bump version
        current = get_current_version()
        new_version = bump_version(current, bump_type)

        print(f"📌 Version bump: {current} → {new_version}")

        # Update versioned pyproject files
        updated_paths = update_pyprojects(new_version)
        for updated_path in updated_paths:
            rel_path = updated_path.relative_to(Path(__file__).parent.parent)
            print(f"✅ Updated {rel_path}")

        # Extensions follow Kindling's major.minor (their major.minor states
        # compatibility); a new minor line moves every extension with it.
        for updated_path in align_extensions(new_version, Path(__file__).parent.parent):
            rel_path = updated_path.relative_to(Path(__file__).parent.parent)
            print(f"✅ Aligned {rel_path}")

        # uv.lock records the workspace members' versions; refresh it so
        # `uv sync --locked` keeps passing. Existing pins are kept as-is.
        result = run_command(["uv", "lock"])
        if result != 0:
            print("❌ uv lock failed")
            return result
        print("✅ Updated uv.lock")

        # Determine if we need to build/deploy
        should_build = build or deploy or bool(platform)
        should_deploy = deploy or bool(platform)

        # Build if requested
        if should_build:
            print(f"\n🔨 Building wheels...")
            result = run_command(["poe", "build"])
            if result != 0:
                print(f"❌ Build failed")
                return result

        # Deploy if requested
        if should_deploy:
            target = platform if platform else "all"
            print(f"\n📤 Deploying {target}...")

            deploy_cmd = ["poe", "deploy"]
            if platform:
                deploy_cmd.extend(["--platform", platform])

            result = run_command(deploy_cmd)
            if result != 0:
                print(f"❌ Deploy failed")
                return result

        print(f"\n🎉 Version bumped successfully: {new_version}")
        return 0

    except Exception as e:
        print(f"❌ Error: {e}")
        return 1


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Bump version and optionally build/deploy")
    parser.add_argument(
        "--type",
        default="alpha",
        choices=["alpha", "patch", "minor", "major"],
        help="Version bump type (default: alpha)",
    )
    parser.add_argument(
        "--platform",
        default="",
        choices=["", "fabric", "synapse", "databricks"],
        help="Platform to deploy (implies build+deploy)",
    )
    parser.add_argument("--build", action="store_true", help="Build wheels after version bump")
    parser.add_argument("--deploy", action="store_true", help="Deploy after build (all platforms)")

    args = parser.parse_args()

    sys.exit(
        main(bump_type=args.type, platform=args.platform, build=args.build, deploy=args.deploy)
    )
