#!/usr/bin/env python3
"""Check that each extension installs on the supported runtimes without
replacing their key packages.

The cluster bootstrap installs an extension's requirements with the
runtime's key packages pinned at their installed versions, and drops the
pins (replacing them) only when the requirements can't be met otherwise.
This resolves each extension's requirements (`uv pip compile`, against the
package index) with the key package versions each runtime ships pinned, and
fails when a runtime would need one of them replaced.

    poe check-extension-runtimes [--extension otel_azure] [--runtime dbr17.3]

RUNTIMES lists the key packages (the bootstrap's _RUNTIME_KEY_DISTRIBUTIONS)
each runtime ships, from its release notes; update it when a supported
runtime changes.
"""

from __future__ import annotations

import argparse
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

RUNTIMES = {
    # Databricks Runtime 17.3 LTS (Python 3.12.3)
    "dbr17.3": (
        "3.12",
        {
            "azure-core": "1.34.0",
            "azure-identity": "1.20.0",
            "msal": "1.32.3",
            "numpy": "2.1.3",
            "opentelemetry-api": "1.32.1",
            "opentelemetry-sdk": "1.32.1",
            "opentelemetry-semantic-conventions": "0.53b1",
            "pandas": "2.2.3",
            "protobuf": "5.29.4",
            # Spark is bundled with the runtime, not a pip install; these
            # are the versions it provides (Apache Spark 4.0.0).
            "py4j": "0.10.9.9",
            "pyarrow": "19.0.1",
            "pyspark": "4.0.0",
            "typing-extensions": "4.12.2",
        },
    ),
    # Fabric Runtime 1.3 (Spark 3.5, Python 3.11). azure-identity and msal
    # ship as conda builds versioned 2023.12.01; the pip releases of that
    # date stand in for them.
    "fabric1.3": (
        "3.11",
        {
            "azure-core": "1.30.2",
            "azure-identity": "1.15.0",
            "msal": "1.26.0",
            "numpy": "1.26.4",
            "pandas": "2.1.4",
            "protobuf": "3.20.3",
            "py4j": "0.10.9.7",
            "pyarrow": "14.0.2",
            # The runtime ships a vendor build (3.5.1.5.4.20240407, not on PyPI);
            # its public base release stands in for it.
            "pyspark": "3.5.1",
            "typing-extensions": "4.9.0",
        },
    ),
    # Azure Synapse Runtime for Apache Spark 3.5 (Python 3.11). msal ships
    # as a conda build versioned 2026.06.01; 1.34.0 stands in for it.
    "synapse3.5": (
        "3.11",
        {
            "azure-core": "1.38.2",
            "azure-identity": "1.17.0",
            "azure-monitor-opentelemetry": "1.8.2",
            "msal": "1.34.0",
            "numpy": "1.26.4",
            "opentelemetry-api": "1.44.0",
            "opentelemetry-sdk": "1.44.0",
            "opentelemetry-semantic-conventions": "0.65b0",
            "pandas": "2.3.3",
            "protobuf": "6.33.5",
            "py4j": "0.10.9.7",
            "pyarrow": "23.0.1",
            # The runtime ships a vendor build (3.5.1.5.4.20240407, not on PyPI);
            # its public base release stands in for it.
            "pyspark": "3.5.1",
            "typing-extensions": "4.16.0",
        },
    ),
}


def _requirement_name(requirement: str) -> str:
    name = requirement.split(";")[0]
    for separator in ("[", "=", "<", ">", "!", "~", " "):
        name = name.split(separator)[0]
    return name.strip().lower().replace("_", "-")


def _is_kindling_requirement(requirement: str) -> bool:
    return _requirement_name(requirement).startswith("spark-kindling")


def extension_requirements(extension_dir: Path) -> list[str]:
    """An extension's install requirements, without Kindling's own
    distributions (the bootstrap never installs those for an extension)."""
    project = tomllib.loads((extension_dir / "pyproject.toml").read_text())["project"]
    return [
        requirement
        for requirement in project.get("dependencies", [])
        if not _is_kindling_requirement(requirement)
    ]


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
