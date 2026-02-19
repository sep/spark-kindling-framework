"""Design-time platform API factory for CI, tests, and local tooling."""

from .platform_provider import create_platform_api_from_env, PlatformAPIRegistry


def list_supported_platforms():
    """Return supported platform names."""
    return PlatformAPIRegistry.list_platforms()


__all__ = ["create_platform_api_from_env", "list_supported_platforms"]
