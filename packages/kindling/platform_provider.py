import os
from abc import ABC, abstractmethod
from typing import Dict, Optional

from kindling.injection import *

AZURE_CLOUD_CONFIGS = {
    "AzurePublicCloud": {
        "aliases": {"AzurePublicCloud", "AZURE_PUBLIC_CLOUD", "public", "publiccloud"},
        "authority": "https://login.microsoftonline.com",
        "storage_suffix": "core.windows.net",
        "storage_scope": "https://storage.azure.com/.default",
        "arm_endpoint": "https://management.azure.com",
        "arm_scope": "https://management.azure.com/.default",
        "synapse_suffix": "dev.azuresynapse.net",
        "synapse_scope": "https://dev.azuresynapse.net/.default",
        "keyvault_scope": "https://vault.azure.net/.default",
        "fabric_api_base_url": "https://api.fabric.microsoft.com/v1",
        "fabric_scope": "https://api.fabric.microsoft.com/.default",
    },
    "AzureUSGovernment": {
        "aliases": {
            "AzureUSGovernment",
            "AZURE_US_GOVERNMENT",
            "AzureGovernment",
            "government",
            "gov",
            "usgov",
        },
        "authority": "https://login.microsoftonline.us",
        "storage_suffix": "core.usgovcloudapi.net",
        "storage_scope": "https://storage.usgovcloudapi.net/.default",
        "arm_endpoint": "https://management.usgovcloudapi.net",
        "arm_scope": "https://management.usgovcloudapi.net/.default",
        "synapse_suffix": "dev.azuresynapse.us",
        "synapse_scope": "https://dev.azuresynapse.us/.default",
        "keyvault_scope": "https://vault.usgovcloudapi.net/.default",
        "fabric_api_base_url": "https://api.fabric.microsoft.us/v1",
        "fabric_scope": "https://api.fabric.microsoft.us/.default",
    },
    "AzureChinaCloud": {
        "aliases": {"AzureChinaCloud", "AZURE_CHINA_CLOUD", "china"},
        "authority": "https://login.chinacloudapi.cn",
        "storage_suffix": "core.chinacloudapi.cn",
        "storage_scope": "https://storage.chinacloudapi.cn/.default",
        "arm_endpoint": "https://management.chinacloudapi.cn",
        "arm_scope": "https://management.chinacloudapi.cn/.default",
        "synapse_suffix": "dev.azuresynapse.azure.cn",
        "synapse_scope": "https://dev.azuresynapse.azure.cn/.default",
        "keyvault_scope": "https://vault.azure.cn/.default",
        "fabric_api_base_url": "https://api.fabric.azure.cn/v1",
        "fabric_scope": "https://api.fabric.azure.cn/.default",
    },
}


def _normalize_azure_cloud(value: str) -> str:
    normalized = (value or "").strip()
    if not normalized:
        return "AzurePublicCloud"
    folded = normalized.replace("-", "").replace("_", "").lower()
    for name, config in AZURE_CLOUD_CONFIGS.items():
        aliases = {name, *config["aliases"]}
        if any(alias.replace("-", "").replace("_", "").lower() == folded for alias in aliases):
            return name
    raise ValueError(
        f"Unsupported Azure cloud `{value}`. Expected one of: {', '.join(AZURE_CLOUD_CONFIGS)}."
    )


def azure_cloud_config() -> Dict[str, str]:
    config = AZURE_CLOUD_CONFIGS[
        _normalize_azure_cloud(os.getenv("AZURE_CLOUD") or "AzurePublicCloud")
    ]
    return {key: value for key, value in config.items() if key != "aliases"}


def azure_env(name: str, default: str) -> str:
    return os.getenv(name, default).strip().rstrip("/")


def azure_token_scope(name: str, default: str) -> str:
    cloud_defaults = {
        "AZURE_STORAGE_TOKEN_SCOPE": "storage_scope",
        "AZURE_MANAGEMENT_TOKEN_SCOPE": "arm_scope",
        "AZURE_SYNAPSE_TOKEN_SCOPE": "synapse_scope",
        "AZURE_KEYVAULT_TOKEN_SCOPE": "keyvault_scope",
        "FABRIC_TOKEN_SCOPE": "fabric_scope",
    }
    config_key = cloud_defaults.get(name)
    if config_key:
        default = azure_cloud_config()[config_key]
    return azure_env(name, default)


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

    def secret_exists(self, secret_name: str) -> bool:
        """Return True if the secret exists and can be retrieved."""
        try:
            self.get_secret(secret_name)
            return True
        except KeyError:
            return False

    def list_secrets(self) -> list:
        """List available secret names. Returns empty list if not supported by the platform."""
        return []


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

    def secret_exists(self, secret_name: str) -> bool:
        platform_provider = get_kindling_service(PlatformServiceProvider)
        platform_service = platform_provider.get_service() if platform_provider else None

        if platform_service and hasattr(platform_service, "secret_exists"):
            return platform_service.secret_exists(secret_name)

        try:
            self.get_secret(secret_name)
            return True
        except KeyError:
            return False

    def list_secrets(self) -> list:
        platform_provider = get_kindling_service(PlatformServiceProvider)
        platform_service = platform_provider.get_service() if platform_provider else None

        if platform_service and hasattr(platform_service, "list_secrets"):
            return platform_service.list_secrets()

        return []
