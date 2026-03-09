import os
from abc import ABC, abstractmethod
from typing import Any, Callable, Dict, List, Optional


class PlatformAPI(ABC):
    """Abstract interface for remote platform API operations."""

    @abstractmethod
    def get_platform_name(self) -> str:
        pass

    @abstractmethod
    def deploy_app(self, app_name: str, app_files: Dict[str, str]) -> str:
        pass

    @abstractmethod
    def cleanup_app(self, app_name: str) -> bool:
        pass

    @abstractmethod
    def create_job(self, job_name: str, job_config: Dict[str, Any]) -> Dict[str, Any]:
        pass

    @abstractmethod
    def run_job(self, job_id: str, parameters: Optional[Dict[str, Any]] = None) -> str:
        pass

    @abstractmethod
    def get_job_status(self, run_id: str) -> Dict[str, Any]:
        pass

    @abstractmethod
    def cancel_job(self, run_id: str) -> bool:
        pass

    @abstractmethod
    def delete_job(self, job_id: str) -> bool:
        pass

    @abstractmethod
    def get_job_logs(self, run_id: str, from_line: int = 0, size: int = 1000) -> Dict[str, Any]:
        pass

    @abstractmethod
    def stream_stdout_logs(
        self,
        job_id: str,
        run_id: str,
        callback: Optional[Callable[[str], None]] = None,
        poll_interval: float = 5.0,
        max_wait: float = 300.0,
    ) -> List[str]:
        pass

    @abstractmethod
    def set_secret(
        self,
        secret_name: str,
        secret_value: str,
        secret_config: Optional[Dict[str, Any]] = None,
    ) -> None:
        pass

    @abstractmethod
    def delete_secret(
        self,
        secret_name: str,
        secret_config: Optional[Dict[str, Any]] = None,
    ) -> bool:
        pass

    def deploy_spark_job(
        self, app_files: Dict[str, str], job_config: Dict[str, Any]
    ) -> Dict[str, Any]:
        app_name = job_config.get("app_name", job_config["job_name"])
        job_name = job_config["job_name"]
        deployment_path = self.deploy_app(app_name, app_files)
        result = self.create_job(job_name, job_config)
        result["deployment_path"] = deployment_path
        return result


class PlatformAPIRegistry:
    """Registry for design-time platform API classes."""

    _registry: Dict[str, type] = {}

    @classmethod
    def register(cls, platform_name: str):
        def decorator(api_class):
            cls._registry[platform_name] = api_class
            return api_class

        return decorator

    @classmethod
    def get(cls, platform_name: str):
        platform_name = (platform_name or "").strip().lower()

        if platform_name not in cls._registry:
            try:
                import importlib

                module_name = f"kindling_sdk.platform_{platform_name}"
                importlib.import_module(module_name)
            except ImportError:
                pass

        if platform_name not in cls._registry:
            available = ", ".join(cls._registry.keys()) if cls._registry else "none"
            raise ValueError(f"Unknown platform: {platform_name}. Available platforms: {available}")
        return cls._registry[platform_name]

    @classmethod
    def list_platforms(cls):
        known = {"fabric", "synapse", "databricks"}
        return sorted(set(cls._registry.keys()) | known)


def create_platform_api_from_env(platform: str):
    platform_name = (platform or "").strip().lower()
    api_class = PlatformAPIRegistry.get(platform_name)
    return api_class.from_env(), platform_name


def create_azure_credential(
    credential: Optional[Any] = None,
    tenant_id: Optional[str] = None,
    client_id: Optional[str] = None,
    client_secret: Optional[str] = None,
) -> Any:
    """Create an Azure credential, preferring explicit service principal auth.

    If a credential object is already provided, it is returned unchanged.
    Otherwise, when all three service principal values are present, this returns
    ClientSecretCredential for deterministic auth in CI and system tests.
    Falls back to DefaultAzureCredential only when explicit SP credentials are
    not available.
    """
    if credential is not None:
        return credential

    try:
        from azure.identity import ClientSecretCredential, DefaultAzureCredential
    except ImportError as exc:
        raise ImportError(
            "azure-identity not installed. Install with: pip install azure-identity"
        ) from exc

    resolved_tenant = (tenant_id or os.getenv("AZURE_TENANT_ID") or "").strip()
    resolved_client_id = (client_id or os.getenv("AZURE_CLIENT_ID") or "").strip()
    resolved_client_secret = (client_secret or os.getenv("AZURE_CLIENT_SECRET") or "").strip()

    if resolved_tenant and resolved_client_id and resolved_client_secret:
        return ClientSecretCredential(
            tenant_id=resolved_tenant,
            client_id=resolved_client_id,
            client_secret=resolved_client_secret,
        )

    return DefaultAzureCredential()
