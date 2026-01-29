#!/usr/bin/env python3
"""
Generate platform-specific pyproject.toml from template.
Usage: generate_platform_config.py <platform> <version>
"""
import sys

# Platform-specific dependencies (beyond common ones)
PLATFORM_DEPS = {
    "synapse": [
        'azure-synapse-artifacts = ">=0.17.0"',
        'azure-storage-file-datalake = ">=12.0.0"',
        'azure-identity = ">=1.12.0"',
        # Pin azure-core to match Synapse runtime (1.30.x)
        'azure-core = ">=1.30.0,<1.31.0"',
    ],
    "databricks": [
        # databricks-sdk is provided by Databricks runtime (0.1.6+)
        # Omitted to avoid version conflicts with runtime packages
    ],
    "fabric": [
        'azure-storage-file-datalake = ">=12.0.0"',
        # Pin azure-core to match Fabric runtime 1.30.2 - do NOT upgrade or imports will fail
        'azure-core = ">=1.30.0,<1.31.0"',
        'azure-identity = ">=1.12.0"',
    ],
}

TEMPLATE = """[build-system]
requires = ["poetry-core>=1.0.0"]
build-backend = "poetry.core.masonry.api"

[tool.poetry]
name = "kindling-{platform}"
version = "{version}"
description = "Cross-platform data processing framework for {platform_title}"
authors = ["SEP Engineering <engineering@sep.com>"]
license = "MIT"
readme = "README.md"
packages = [
    {{include = "kindling", from = "packages"}},
]

[tool.poetry.dependencies]
python = "^3.10"
# Runtime-provided packages (available in {platform_title}):
# - pyspark, delta-spark, pandas, pyarrow
# Only include packages NOT provided by the runtime:
injector = ">=0.20.1"
dynaconf = ">=3.1.0"
pyyaml = ">=6.0"
packaging = ">=23.0"
{platform_deps}
[tool.poetry.group.dev.dependencies]
pytest = ">=7.0.0"
black = ">=23.0.0"
"""


def main():
    if len(sys.argv) != 3:
        print("Usage: generate_platform_config.py <platform> <version>", file=sys.stderr)
        sys.exit(1)

    platform = sys.argv[1]
    version = sys.argv[2]
    platform_title = platform.capitalize()

    # Get platform-specific deps
    deps = PLATFORM_DEPS.get(platform, [])
    platform_deps = "\n".join(deps) if deps else ""
    if platform_deps:
        platform_deps = platform_deps + "\n"

    print(
        TEMPLATE.format(
            platform=platform,
            version=version,
            platform_title=platform_title,
            platform_deps=platform_deps,
        )
    )


if __name__ == "__main__":
    main()
