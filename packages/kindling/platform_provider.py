from abc import ABC, abstractmethod
from typing import Any, Dict, Optional

from kindling.injection import *


class PlatformAPI(ABC):
    """
    Abstract interface for remote platform API operations.

    This is for operations that can be performed remotely via REST APIs,
    such as deploying jobs, uploading files, and monitoring execution.

    Does NOT require mssparkutils/dbutils - uses only HTTP/REST APIs.
    """

    @abstractmethod
    def get_platform_name(self) -> str:
        """Get the platform name

        Returns:
            Platform name (e.g., 'fabric', 'databricks', 'synapse')
        """
        pass

    @abstractmethod
    def deploy_spark_job(
        self, app_files: Dict[str, str], job_config: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Deploy application as Spark job (convenience method)

        This is a convenience method that combines create_spark_job + upload_files + update_job_files.

        Args:
            app_files: Dictionary of {filename: content} to deploy
            job_config: Platform-specific job configuration

        Returns:
            Dictionary with job_id, deployment_path, and metadata
        """
        pass

    @abstractmethod
    def create_spark_job(self, job_name: str, job_config: Dict[str, Any]) -> Dict[str, Any]:
        """Create a Spark job definition

        Args:
            job_name: Name for the job
            job_config: Platform-specific job configuration

        Returns:
            Dictionary with job_id and other metadata
        """
        pass

    @abstractmethod
    def upload_files(self, files: Dict[str, str], target_path: str) -> str:
        """Upload files to platform storage

        Args:
            files: Dictionary of {filename: content}
            target_path: Target path in platform storage

        Returns:
            Storage path where files were uploaded
        """
        pass

    @abstractmethod
    def update_job_files(self, job_id: str, files_path: str) -> None:
        """Update job definition with file paths

        Args:
            job_id: Job ID to update
            files_path: Path to uploaded files
        """
        pass

    @abstractmethod
    def run_job(self, job_id: str, parameters: Optional[Dict[str, Any]] = None) -> str:
        """Execute a job

        Args:
            job_id: Job ID to run
            parameters: Optional runtime parameters

        Returns:
            Run ID for monitoring
        """
        pass

    @abstractmethod
    def get_job_status(self, run_id: str) -> Dict[str, Any]:
        """Get job execution status

        Args:
            run_id: Run ID to check

        Returns:
            Status dictionary with keys:
                - status: Current status
                - start_time: When job started
                - end_time: When job finished (if complete)
                - error: Error message (if failed)
                - logs: Job logs (if available)
        """
        pass

    @abstractmethod
    def cancel_job(self, run_id: str) -> bool:
        """Cancel a running job

        Args:
            run_id: Run ID to cancel

        Returns:
            True if cancelled successfully
        """
        pass

    @abstractmethod
    def delete_job(self, job_id: str) -> bool:
        """Delete a job definition

        Args:
            job_id: Job ID to delete

        Returns:
            True if deleted successfully
        """
        pass

    @abstractmethod
    def get_job_logs(self, run_id: str, from_line: int = 0, size: int = 1000) -> Dict[str, Any]:
        """Get job execution logs

        Args:
            run_id: Run ID to get logs for
            from_line: Starting line number (default: 0)
            size: Maximum number of lines to return (default: 1000)

        Returns:
            Dictionary containing logs and metadata:
                - log: List of log lines or full log content
                - total_lines: Total number of lines available (if known)
                - from_line: Starting line of returned logs
                - has_more: Whether more logs are available
        """
        pass


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


# =============================================================================
# Platform API Registry (for CI/CD, testing, CLI tools)
# =============================================================================


class PlatformAPIRegistry:
    """Registry for platform API classes"""

    _registry: Dict[str, type] = {}

    @classmethod
    def register(cls, platform_name: str):
        """Decorator to register a platform API class

        Example:
            @PlatformAPIRegistry.register("fabric")
            class FabricAPI(PlatformAPI):
                @classmethod
                def from_env(cls):
                    ...
        """

        def decorator(api_class):
            cls._registry[platform_name] = api_class
            return api_class

        return decorator

    @classmethod
    def get(cls, platform_name: str):
        """Get platform API class by name

        Args:
            platform_name: Platform name (e.g., "fabric", "synapse", "databricks")

        Returns:
            Platform API class

        Raises:
            ValueError: If platform is not registered
        """
        # If not already registered, try to import the platform module to trigger registration
        if platform_name not in cls._registry:
            try:
                import importlib

                # Dynamic import: from kindling.platform_{name} import {Name}API
                module_name = f"kindling.platform_{platform_name}"
                importlib.import_module(module_name)
                # Import triggers the @register decorator, which adds to registry
            except ImportError:
                pass

        # Check again after import attempt
        if platform_name not in cls._registry:
            available = ", ".join(cls._registry.keys()) if cls._registry else "none"
            raise ValueError(
                f"Unknown platform: {platform_name}. " f"Available platforms: {available}"
            )
        return cls._registry[platform_name]

    @classmethod
    def list_platforms(cls):
        """List all registered platform names"""
        return list(cls._registry.keys())


def create_platform_api_from_env(platform: str):
    """
    Create platform API client from environment variables.

    Uses the platform API registry to find the appropriate class
    and calls its from_env() class method.

    Args:
        platform: Platform name ("fabric", "synapse", "databricks", etc.)

    Returns:
        Tuple of (PlatformAPI client, platform_name)

    Raises:
        ValueError: If platform is unknown or required env vars are missing

    Example:
        >>> client, name = create_platform_api_from_env("fabric")
        >>> result = client.deploy_spark_job(app_files, job_config)
    """
    api_class = PlatformAPIRegistry.get(platform)
    return api_class.from_env(), platform
