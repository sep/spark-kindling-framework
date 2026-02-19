"""Design-time platform SDK for Kindling."""

from .platform_api import create_platform_api_from_env, list_supported_platforms

__all__ = ["create_platform_api_from_env", "list_supported_platforms"]
