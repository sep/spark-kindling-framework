"""Design-time platform API factory for CI, tests, and local tooling."""

from .platform_provider import PlatformAPIRegistry, create_platform_api_from_env


def list_supported_platforms():
    """Return supported platform names."""
    return PlatformAPIRegistry.list_platforms()


__all__ = ["create_platform_api_from_env", "list_supported_platforms"]
