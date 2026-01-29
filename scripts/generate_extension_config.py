#!/usr/bin/env python3
"""
Generate platform-specific extension pyproject.toml from template.
Usage: generate_extension_config.py <extension> <platform> <version>
"""
import sys

# Platform-specific extension dependencies
EXTENSION_PLATFORM_DEPS = {
    "kindling-otel-azure": {
        "fabric": [
            # Fabric has OLD azure-core pre-installed
            # Use older azure-monitor-opentelemetry compatible with old azure-core
            'azure-monitor-opentelemetry = ">=1.6.0,<1.7.0"',
            'opentelemetry-api = "^1.20.0"',
            'opentelemetry-sdk = "^1.20.0"',
            'typing-extensions = ">=4.6.0"',
        ],
        "synapse": [
            # Synapse can handle newer versions
            'azure-monitor-opentelemetry = "^1.8.0"',
            'opentelemetry-api = "^1.21.0"',
            'opentelemetry-sdk = "^1.21.0"',
            'typing-extensions = ">=4.6.0"',
        ],
        "databricks": [
            # Databricks can handle newer versions
            'azure-monitor-opentelemetry = "^1.8.0"',
            'opentelemetry-api = "^1.21.0"',
            'opentelemetry-sdk = "^1.21.0"',
            'typing-extensions = ">=4.6.0"',
        ],
    }
}

TEMPLATE = """[build-system]
requires = ["poetry-core>=1.0.0"]
build-backend = "poetry.core.masonry.api"

[tool.poetry]
name = "{extension_name}-{platform}"
version = "{version}"
description = "Azure Monitor OpenTelemetry integration for Kindling framework ({platform_title})"
authors = ["SEP Engineering <engineering@sep.com>"]
license = "MIT"
readme = "README.md"
packages = [
    {{include = "{package_name}"}},
]
include = [
    "CHANGELOG.md",
]

[tool.poetry.dependencies]
python = "^3.10"
{platform_deps}

[tool.poetry.group.dev.dependencies]
pytest = ">=7.0.0"
black = ">=23.0.0"
"""


def generate_config(extension: str, platform: str, version: str) -> str:
    """Generate platform-specific pyproject.toml for extension"""

    if extension not in EXTENSION_PLATFORM_DEPS:
        raise ValueError(f"Unknown extension: {extension}")

    if platform not in EXTENSION_PLATFORM_DEPS[extension]:
        raise ValueError(f"Unknown platform for {extension}: {platform}")

    # Get package name from extension name (kindling-otel-azure -> kindling_otel_azure)
    package_name = extension.replace("-", "_")

    # Get platform-specific dependencies
    deps = EXTENSION_PLATFORM_DEPS[extension][platform]
    platform_deps = "\n".join(deps)

    # Generate config
    config = TEMPLATE.format(
        extension_name=extension,
        package_name=package_name,
        platform=platform,
        platform_title=platform.title(),
        version=version,
        platform_deps=platform_deps,
    )

    return config


if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Usage: generate_extension_config.py <extension> <platform> <version>")
        print("Example: generate_extension_config.py kindling-otel-azure fabric 0.3.0a2")
        sys.exit(1)

    extension = sys.argv[1]
    platform = sys.argv[2]
    version = sys.argv[3]

    config = generate_config(extension, platform, version)
    print(config)
