from abc import ABC, abstractmethod
from typing import Optional

from kindling.injection import *


class PlatformServiceProvider(ABC):
    @abstractmethod
    def set_service(self, svc):
        pass

    @abstractmethod
    def get_service(self):
        pass


@GlobalInjector.singleton_autobind()
class SparkPlatformServiceProvider(PlatformServiceProvider):
    svc = None

    def set_service(self, svc):
        self.svc = svc

    def get_service(self):
        return self.svc


class SecretProvider(ABC):
    """Abstract secret lookup contract used by config loaders and runtime services."""

    @abstractmethod
    def get_secret(self, secret_name: str, default: Optional[str] = None) -> str:
        """Resolve secret value by name."""
        pass


@GlobalInjector.singleton_autobind(SecretProvider)
class PlatformServiceSecretProvider(SecretProvider):
    """Secret provider backed by the currently active PlatformService."""

    def get_secret(self, secret_name: str, default: Optional[str] = None) -> str:
        platform_provider = get_kindling_service(PlatformServiceProvider)
        platform_service = platform_provider.get_service() if platform_provider else None

        if platform_service and hasattr(platform_service, "get_secret"):
            return platform_service.get_secret(secret_name, default=default)

        if default is not None:
            return default

        raise KeyError(
            f"No platform secret provider available for '{secret_name}'. "
            "Ensure platform services are initialized."
        )
