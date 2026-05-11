import os
from abc import ABC, abstractmethod
from typing import Any, Callable, Dict, List, Optional
from urllib.parse import urlparse

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
        "synapse_suffix": "dev.azuresynapse.usgovcloudapi.net",
        "synapse_scope": "https://dev.azuresynapse.usgovcloudapi.net/.default",
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


def _normalize_azure_environment(value: str) -> str:
    normalized = (value or "").strip()
    if not normalized:
        return "AzurePublicCloud"
    folded = normalized.replace("-", "").replace("_", "").lower()
    for name, config in AZURE_CLOUD_CONFIGS.items():
        aliases = {name, *config["aliases"]}
        if any(alias.replace("-", "").replace("_", "").lower() == folded for alias in aliases):
            return name
    raise ValueError(
        f"Unsupported Azure environment `{value}`. "
        f"Expected one of: {', '.join(AZURE_CLOUD_CONFIGS)}."
    )


def azure_environment_name() -> str:
    return _normalize_azure_environment(os.getenv("AZURE_CLOUD") or "AzurePublicCloud")


def azure_cloud_config() -> Dict[str, str]:
    config = AZURE_CLOUD_CONFIGS[azure_environment_name()]
    return {key: value for key, value in config.items() if key != "aliases"}


class PlatformAPI(ABC):
    """Abstract interface for remote platform API operations."""

    RUNNER_JOB_NAME: str = "kindling-runner"

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

    @abstractmethod
    def find_job_by_name(self, name: str) -> Optional[str]:
        """Return the platform job ID for the given job name, or None if not found."""
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

    # --- Runner lifecycle ---
    # All implemented here using the abstract job primitives + find_job_by_name,
    # so every platform adapter gets full runner support for free.

    def ensure_runner(self, platform: str) -> Dict[str, Any]:
        runner_id = self.find_job_by_name(self.RUNNER_JOB_NAME)
        if runner_id is None:
            result = self.create_job(self.RUNNER_JOB_NAME, {"app_name": self.RUNNER_JOB_NAME})
            runner_id = str(result.get("job_id", self.RUNNER_JOB_NAME))
        return {"runner_id": runner_id, "state": "installed"}

    def get_runner_status(self, platform: str) -> Dict[str, Any]:
        runner_id = self.find_job_by_name(self.RUNNER_JOB_NAME)
        if runner_id is None:
            return {"runner_id": None, "state": "not_installed"}
        return {"runner_id": runner_id, "state": "installed"}

    def repair_runner(self, platform: str) -> Dict[str, Any]:
        existing_id = self.find_job_by_name(self.RUNNER_JOB_NAME)
        if existing_id is not None:
            self.delete_job(existing_id)
        result = self.create_job(self.RUNNER_JOB_NAME, {"app_name": self.RUNNER_JOB_NAME})
        runner_id = str(result.get("job_id", self.RUNNER_JOB_NAME))
        return {"runner_id": runner_id, "state": "installed"}

    def delete_runner(self, platform: str) -> bool:
        runner_id = self.find_job_by_name(self.RUNNER_JOB_NAME)
        if runner_id is None:
            return True
        return self.delete_job(runner_id)

    # --- Runner-aligned app submission ---
    # These delegate directly to the existing abstract job primitives so every
    # platform adapter gets them for free once run_job/get_job_status/etc. work.

    def submit_app_run(
        self,
        app_name: str,
        environment: Optional[str] = None,
        parameters: Optional[Dict[str, Any]] = None,
    ) -> str:
        runner_id = self.find_job_by_name(self.RUNNER_JOB_NAME)
        if runner_id is None:
            raise RuntimeError(
                f"Kindling runner '{self.RUNNER_JOB_NAME}' not found. "
                "Run 'kindling runner ensure' before submitting app runs."
            )
        run_params: Dict[str, Any] = {"app_name": app_name}
        if environment:
            run_params["environment"] = environment
        if parameters:
            run_params.update(parameters)
        return self.run_job(runner_id, parameters=run_params)

    def get_app_run_status(self, run_id: str) -> Dict[str, Any]:
        status = self.get_job_status(run_id)
        return {**status, "runner_id": self.RUNNER_JOB_NAME}

    def get_app_run_logs(self, run_id: str, from_line: int = 0, size: int = 1000) -> Dict[str, Any]:
        return self.get_job_logs(run_id, from_line=from_line, size=size)

    def stream_app_run_logs(
        self,
        run_id: str,
        callback: Optional[Callable[[str], None]] = None,
        poll_interval: float = 5.0,
        max_wait: float = 300.0,
    ) -> List[str]:
        runner_id = self.find_job_by_name(self.RUNNER_JOB_NAME) or self.RUNNER_JOB_NAME
        return self.stream_stdout_logs(
            job_id=runner_id,
            run_id=run_id,
            callback=callback,
            poll_interval=poll_interval,
            max_wait=max_wait,
        )

    def cancel_app_run(self, run_id: str) -> bool:
        return self.cancel_job(run_id)


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


def azure_env(name: str, default: str) -> str:
    """Return an Azure env override, falling back to the provided default."""
    return os.getenv(name, default).strip().rstrip("/")


def azure_storage_account_name(storage_account: str) -> str:
    """Extract an account name from an account name, endpoint host, or storage URL."""
    raw = (storage_account or "").strip()
    if not raw:
        return raw

    parsed = urlparse(raw if "://" in raw else f"//{raw}")
    host = parsed.hostname or raw.split("/", 1)[0]
    return host.split(".", 1)[0]


def azure_storage_endpoint(storage_account: str, service: str = "dfs") -> str:
    """Return a service endpoint host for the configured Azure environment."""
    account_name = azure_storage_account_name(storage_account)
    env_name = (
        "AZURE_STORAGE_BLOB_ENDPOINT_SUFFIX"
        if service == "blob"
        else "AZURE_STORAGE_DFS_ENDPOINT_SUFFIX"
    )
    suffix_override = os.getenv(env_name)
    if suffix_override:
        return f"{account_name}.{suffix_override.strip().rstrip('/').lstrip('.')}"

    storage_suffix = azure_cloud_config()["storage_suffix"].lstrip(".")
    return f"{account_name}.{service}.{storage_suffix}"


def azure_storage_dfs_endpoint(storage_account: str) -> str:
    return azure_storage_endpoint(storage_account, "dfs")


def azure_storage_blob_endpoint(storage_account: str) -> str:
    return azure_storage_endpoint(storage_account, "blob")


def azure_storage_account_url(storage_account: str, *, service: str = "dfs") -> str:
    endpoint = (
        azure_storage_blob_endpoint(storage_account)
        if service == "blob"
        else azure_storage_dfs_endpoint(storage_account)
    )
    return f"https://{endpoint}"


def azure_abfss_uri(container: str, storage_account: str, path: str = "") -> str:
    base = f"abfss://{container}@{azure_storage_dfs_endpoint(storage_account)}"
    clean_path = path.strip("/")
    return f"{base}/{clean_path}" if clean_path else base


def azure_authority_host() -> str:
    return azure_env("AZURE_AUTHORITY_HOST", azure_cloud_config()["authority"])


def azure_oauth_token_endpoint(tenant_id: str) -> str:
    return f"{azure_authority_host()}/{tenant_id}/oauth2/token"


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


def fabric_api_base_url() -> str:
    return azure_env("FABRIC_API_BASE_URL", azure_cloud_config()["fabric_api_base_url"])


def azure_management_endpoint() -> str:
    return azure_env("AZURE_MANAGEMENT_ENDPOINT", azure_cloud_config()["arm_endpoint"])


def azure_synapse_dev_endpoint_suffix() -> str:
    return azure_env(
        "AZURE_SYNAPSE_DEV_ENDPOINT_SUFFIX", azure_cloud_config()["synapse_suffix"]
    ).lstrip(".")


def create_azure_credential(
    credential: Optional[Any] = None,
    tenant_id: Optional[str] = None,
    client_id: Optional[str] = None,
    client_secret: Optional[str] = None,
    additionally_allowed_tenants: Optional[List[str]] = None,
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
            authority=azure_authority_host(),
            additionally_allowed_tenants=additionally_allowed_tenants,
        )

    return DefaultAzureCredential(
        authority=azure_authority_host(),
        additionally_allowed_tenants=additionally_allowed_tenants,
    )
