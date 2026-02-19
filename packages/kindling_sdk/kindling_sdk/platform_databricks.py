import json
import os
import re
import subprocess
import sys
import time
import types
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Union
from urllib.parse import quote

import requests

from .platform_provider import PlatformAPI, PlatformAPIRegistry

# Databricks REST API Client (for remote operations)
# ============================================================================


@PlatformAPIRegistry.register("databricks")
class DatabricksAPI(PlatformAPI):
    """
    Databricks API client for remote operations using Databricks SDK.

    This class provides SDK-based access to Databricks for operations like:
    - Creating job definitions
    - Uploading files to DBFS
    - Running and monitoring jobs

    Does NOT require dbutils - uses Databricks SDK.
    Can be used from local dev, CI/CD, tests, etc.

    Authentication via Azure CLI (same as Fabric/Synapse pattern).

    Example:
        >>> from databricks_api import DatabricksAPI
        >>> api = DatabricksAPI("https://adb-xxx.azuredatabricks.net")
        >>> job = api.create_job("my-job", config)
        >>> run_id = api.run_job(job["job_id"])
        >>> status = api.get_job_status(run_id)
    """

    def __init__(
        self,
        workspace_url: str,
        token: Optional[str] = None,
        credential: Optional[Any] = None,
        storage_account: Optional[str] = None,
        container: Optional[str] = None,
        base_path: Optional[str] = None,
        default_cluster_id: Optional[str] = None,
        azure_tenant_id: Optional[str] = None,
        azure_client_id: Optional[str] = None,
        azure_client_secret: Optional[str] = None,
    ):
        """Initialize Databricks API client

        Args:
            workspace_url: Databricks workspace URL
            token: Optional Databricks token (uses Azure CLI if not provided)
            credential: Optional Azure credential for AAD auth
            storage_account: Optional storage account for ABFSS cluster logs
            container: Optional container name for ABFSS cluster logs
            base_path: Optional base path within container
            default_cluster_id: Optional existing cluster ID to use when job config omits cluster
            azure_tenant_id: Optional Azure tenant ID for service principal auth
            azure_client_id: Optional Azure client ID for service principal auth
            azure_client_secret: Optional Azure client secret for service principal auth
        """
        self.workspace_url = workspace_url.rstrip("/")
        self.token = token
        self.credential = credential
        self.storage_account = storage_account
        self.container = container
        self.base_path = base_path
        self.default_cluster_id = default_cluster_id
        self.azure_tenant_id = azure_tenant_id
        self.azure_client_id = azure_client_id
        self.azure_client_secret = azure_client_secret

        # Initialize Databricks SDK client
        self._client = self._create_sdk_client()

        # Job tracking (maps job_name to job_id)
        self._job_mapping = {}

    def _create_sdk_client(self):
        """Create Databricks SDK WorkspaceClient with service principal or Azure CLI auth"""
        try:
            from databricks.sdk import WorkspaceClient
        except ImportError:
            raise ImportError(
                "Databricks SDK not available. Install with: pip install databricks-sdk"
            )

        # Check if service principal credentials are provided
        if self.azure_tenant_id and self.azure_client_id and self.azure_client_secret:
            # Use service principal authentication
            return WorkspaceClient(
                host=self.workspace_url,
                azure_tenant_id=self.azure_tenant_id,
                azure_client_id=self.azure_client_id,
                azure_client_secret=self.azure_client_secret,
                auth_type="azure-client-secret",
            )
        else:
            # Fall back to Azure CLI authentication
            return WorkspaceClient(host=self.workspace_url, auth_type="azure-cli")

    @classmethod
    def from_env(cls):
        """
        Create DatabricksAPI client from environment variables.

        Required environment variables:
            - DATABRICKS_HOST

        Optional environment variables:
            - AZURE_STORAGE_ACCOUNT (for file uploads)
            - AZURE_CONTAINER (default: "artifacts")
            - AZURE_BASE_PATH (default: "system-tests")
            - DATABRICKS_CLUSTER_ID (default existing cluster for jobs)
            - AZURE_TENANT_ID (for service principal auth)
            - AZURE_CLIENT_ID (for service principal auth)
            - AZURE_CLIENT_SECRET (for service principal auth)

        Returns:
            DatabricksAPI client instance

        Raises:
            ValueError: If required environment variables are missing
        """
        import os

        workspace_url = os.getenv("DATABRICKS_HOST")

        if not workspace_url:
            raise ValueError("Missing required environment variable: DATABRICKS_HOST")

        return cls(
            workspace_url=workspace_url,
            storage_account=os.getenv("AZURE_STORAGE_ACCOUNT"),
            container=os.getenv("AZURE_CONTAINER", "artifacts"),
            base_path=os.getenv("AZURE_BASE_PATH", "system-tests"),
            default_cluster_id=os.getenv("DATABRICKS_CLUSTER_ID"),
            azure_tenant_id=os.getenv("AZURE_TENANT_ID"),
            azure_client_id=os.getenv("AZURE_CLIENT_ID"),
            azure_client_secret=os.getenv("AZURE_CLIENT_SECRET"),
        )

    def get_platform_name(self) -> str:
        """Get platform name"""
        return "databricks"

    def _resolve_databricks_secret_target(
        self,
        secret_name: str,
        secret_config: Optional[Dict[str, Any]] = None,
    ) -> tuple[str, str]:
        cfg = secret_config or {}
        scope = str(
            cfg.get("secret_scope")
            or os.getenv("SYSTEM_TEST_DATABRICKS_SECRET_SCOPE")
            or "kindling-system-tests"
        ).strip()
        key = secret_name

        if ":" in secret_name:
            possible_scope, possible_key = secret_name.split(":", 1)
            if possible_scope and possible_key:
                scope = possible_scope
                key = possible_key

        return scope, key

    def set_secret(
        self,
        secret_name: str,
        secret_value: str,
        secret_config: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Create/update a Databricks secret in the target secret scope."""
        scope, key = self._resolve_databricks_secret_target(secret_name, secret_config)
        scopes_response = self.client.secrets.list_scopes()
        existing_scopes = {s.name for s in (getattr(scopes_response, "scopes", None) or [])}

        if scope not in existing_scopes:
            try:
                self.client.secrets.create_scope(scope=scope, initial_manage_principal="users")
            except Exception as exc:
                # Scope may have been created concurrently.
                if "already exists" not in str(exc).lower():
                    raise

        self.client.secrets.put_secret(scope=scope, key=key, string_value=secret_value)

    def delete_secret(
        self,
        secret_name: str,
        secret_config: Optional[Dict[str, Any]] = None,
    ) -> bool:
        """Delete a Databricks secret from the target secret scope."""
        scope, key = self._resolve_databricks_secret_target(secret_name, secret_config)
        try:
            self.client.secrets.delete_secret(scope=scope, key=key)
            return True
        except Exception as exc:
            message = str(exc).lower()
            if "not found" in message or "does not exist" in message:
                return True
            raise

    def deploy_app(self, app_name: str, app_files: Dict[str, str]) -> str:
        """Upload app files to ABFSS storage

        Args:
            app_name: Application name
            app_files: Dictionary of {filename: content} to deploy

        Returns:
            Storage path where files were uploaded
        """
        target_path = f"data-apps/{app_name}"
        return self._upload_files(app_files, target_path)

    def cleanup_app(self, app_name: str) -> bool:
        """Delete app files from ABFSS storage

        Args:
            app_name: Application name to clean up

        Returns:
            True if cleanup succeeded, False otherwise
        """
        try:
            from azure.identity import DefaultAzureCredential
            from azure.storage.filedatalake import DataLakeServiceClient
        except ImportError:
            print("⚠️  azure-storage-file-datalake not installed. Cleanup skipped.")
            return False

        if not self.storage_account or not self.container:
            print("⚠️  Storage account/container not configured. Cleanup skipped.")
            return False

        try:
            account_url = f"https://{self.storage_account}.dfs.core.windows.net"
            credential = self.credential if self.credential else DefaultAzureCredential()
            storage_client = DataLakeServiceClient(account_url=account_url, credential=credential)
            file_system_client = storage_client.get_file_system_client(file_system=self.container)

            # Construct full path
            app_path = f"data-apps/{app_name}"
            if self.base_path:
                full_path = f"{self.base_path}/{app_path}"
            else:
                full_path = app_path

            # Delete the directory recursively
            dir_client = file_system_client.get_directory_client(full_path)
            dir_client.delete_directory()
            print(f"🗑️  Cleaned up app files at: {full_path}")
            return True
        except Exception as e:
            print(f"⚠️  Failed to cleanup app {app_name}: {e}")
            return False

    def deploy_spark_job(
        self, app_files: Dict[str, str], job_config: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Deploy application as Spark job (backward compat wrapper)

        Delegates to the ABC base class implementation which calls
        deploy_app + create_job.

        Args:
            app_files: Dictionary of {filename: content} to deploy
            job_config: Platform-specific job configuration

        Returns:
            Dictionary with job_id, deployment_path, and metadata
        """
        return super().deploy_spark_job(app_files, job_config)

    @property
    def client(self):
        """Get the Databricks SDK client"""
        return self._client

    def create_job(self, job_name: str, job_config: Dict[str, Any]) -> Dict[str, Any]:
        """Create a Spark job definition in Databricks using SDK

        Args:
            job_name: Name for the job
            job_config: Job configuration containing:
                - app_name: Application name for Kindling bootstrap
                - main_file: Entry point file path (default: kindling_bootstrap.py)
                - cluster_logs_path: ABFSS path for cluster logs (optional)
                - spark_config: Spark configuration (optional)
                - environment: Environment variables (optional)

        Returns:
            Dictionary with job_id and metadata
        """
        try:
            from databricks.sdk.service.compute import (
                ClusterLogConf,
                ClusterSpec,
                DataSecurityMode,
                Library,
            )
            from databricks.sdk.service.jobs import SparkPythonTask, Task
        except ImportError:
            raise ImportError("Databricks SDK service modules not available")

        app_name = job_config.get("app_name", job_name)
        main_file = job_config.get("main_file", "kindling_bootstrap.py")

        # Build bootstrap parameters similar to Fabric/Synapse
        # Construct full ABFSS path for artifacts_storage_path including base_path
        if self.storage_account and self.container:
            # Include base_path to ensure files are found in correct location
            # e.g., abfss://container@storage.dfs.core.windows.net/system-tests/run-123/databricks
            base_url = f"abfss://{self.container}@{self.storage_account}.dfs.core.windows.net"
            if self.base_path:
                artifacts_storage_path = f"{base_url}/{self.base_path.strip('/')}"
            else:
                artifacts_storage_path = base_url
        else:
            artifacts_storage_path = "artifacts"

        bootstrap_params = {
            "app_name": app_name,
            "artifacts_storage_path": artifacts_storage_path,
            "platform": "databricks",
            "use_lake_packages": "True",
            "load_local_packages": "False",
            # Pass workspace URL so DatabricksService can initialize
            "workspace_id": self.workspace_url,
        }

        # Add test_id if provided (for test tracking)
        if "test_id" in job_config:
            bootstrap_params["test_id"] = job_config["test_id"]

        # Config overrides are expected to be resolved by the caller/test harness.
        overrides = job_config.get("config_overrides", {})
        if overrides:
            # Flatten nested dict to dot notation
            def flatten_dict(d, parent_key=""):
                items = []
                for k, v in d.items():
                    new_key = f"{parent_key}.{k}" if parent_key else k
                    if isinstance(v, dict):
                        items.extend(flatten_dict(v, new_key).items())
                    else:
                        items.append((new_key, v))
                return dict(items)

            flattened = flatten_dict(overrides)
            bootstrap_params.update(flattened)

        # Build command line args using config:key=value convention
        config_args = [f"config:{k}={v}" for k, v in bootstrap_params.items()]
        additional_args = (
            job_config.get("command_line_args", "").split()
            if job_config.get("command_line_args")
            else []
        )
        all_args = config_args + additional_args

        # Configure cluster log delivery to Unity Catalog Volume
        # UC Volume /Volumes/medallion/default/logs is backed by ABFSS storage
        # allowing programmatic access to driver logs after job completion.
        #
        # Cluster logs contain driver logs with application logging output (logger.info, logger.error, etc.)
        from databricks.sdk.service.compute import VolumesStorageInfo

        # Use Unity Catalog Volume (backed by ABFSS at logs/ path in artifacts container)
        # UC Volume paths start with /Volumes/ and use VolumesStorageInfo, not DbfsStorageInfo
        uc_volume_path = job_config.get("cluster_logs_volume", "/Volumes/medallion/default/logs")
        log_path = f"{uc_volume_path}/cluster-logs/kindling-jobs/{job_name}"

        cluster_log_conf = ClusterLogConf(volumes=VolumesStorageInfo(destination=log_path))

        # Build cluster specification using compute.ClusterSpec
        # This is the actual cluster config with spark version, node types, etc.
        # Set data_security_mode to SINGLE_USER for UC Volumes cluster log delivery support
        # Filter out incompatible spark configs when using SINGLE_USER mode
        spark_conf = job_config.get("spark_config", {}).copy()
        # Remove spark.databricks.cluster.profile as it's incompatible with data_security_mode
        spark_conf.pop("spark.databricks.cluster.profile", None)

        cluster_spec = ClusterSpec(
            spark_version=job_config.get("spark_version", "13.3.x-scala2.12"),
            node_type_id=job_config.get("node_type_id", "Standard_DS3_v2"),
            num_workers=job_config.get("num_workers", 1),
            cluster_log_conf=cluster_log_conf,
            data_security_mode=DataSecurityMode.SINGLE_USER,
            spark_conf=spark_conf if spark_conf else None,
        )

        # Build Python task using SDK objects
        # Use ABFSS path directly for the Python script
        # Bootstrap script is deployed to scripts/ subdirectory
        if self.storage_account and self.container:
            # Bootstrap script is ALWAYS at scripts/kindling_bootstrap.py
            python_file = f"abfss://{self.container}@{self.storage_account}.dfs.core.windows.net/scripts/{main_file}"
        else:
            # Fallback to dbfs:/FileStore if no storage configured
            python_file = f"dbfs:/FileStore/scripts/{main_file}"

        python_task = SparkPythonTask(python_file=python_file, parameters=all_args)

        # Build libraries list for the task
        # No wheel library - kindling is expected to be pre-installed or available in the environment
        libraries = []

        # Build task using SDK Task object with new_cluster or existing_cluster_id
        # Check if existing cluster ID is provided in config
        # Support both 'cluster_id' and 'existing_cluster_id' for backward compatibility
        existing_cluster_id = (
            job_config.get("existing_cluster_id")
            or job_config.get("cluster_id")
            or self.default_cluster_id
        )

        if existing_cluster_id:
            # Use existing cluster instead of creating new one
            # NOTE: When using existing cluster, the cluster's own log configuration takes precedence.
            # Make sure the cluster is configured to write logs to the desired location
            # (e.g., /Volumes/medallion/default/logs/cluster-logs in the cluster's settings)
            task = Task(
                task_key="main",
                spark_python_task=python_task,
                existing_cluster_id=existing_cluster_id,
                libraries=libraries if libraries else None,
                max_retries=0,  # Disable auto-retry to prevent duplicate executions
            )
        else:
            # Create new cluster for this job
            task = Task(
                task_key="main",
                spark_python_task=python_task,
                new_cluster=cluster_spec,
                libraries=libraries if libraries else None,
                max_retries=0,  # Disable auto-retry to prevent duplicate executions
            )

        # Create the job via SDK using proper SDK objects
        job_response = self.client.jobs.create(
            name=job_name,
            tasks=[task],
            tags={"Purpose": "SystemTest", "Framework": "Kindling", "Platform": "Databricks"},
            timeout_seconds=3600,
            max_concurrent_runs=1,
        )

        job_id = job_response.job_id

        # Store job mapping
        self._job_mapping[job_name] = job_id

        return {
            "job_id": str(job_id),
            "job_name": job_name,
            "workspace_url": self.workspace_url,
        }

    def _upload_files(self, files: Dict[str, str], target_path: str) -> str:
        """Upload files to ABFSS storage path (same pattern as Fabric/Synapse)

        Files are uploaded to Azure Data Lake Storage which Databricks can access at runtime.

        Args:
            files: Dictionary of {filename: content}
            target_path: Relative path within container (e.g., "data-apps/my-app")

        Returns:
            ABFSS path reference for use in job definition

        Note:
            Requires storage_account and container to be configured.
            Files are uploaded using Azure Storage SDK.
            This is an internal method - use deploy_app() for public API.
        """
        try:
            from azure.identity import DefaultAzureCredential
            from azure.storage.filedatalake import DataLakeServiceClient
        except ImportError:
            print("⚠️  azure-storage-file-datalake not installed. Files not uploaded.")
            print("   Install with: pip install azure-storage-file-datalake")
            return target_path

        if not self.storage_account or not self.container:
            print("⚠️  Storage account/container not configured. Files not uploaded.")
            print(f"   Files prepared: {list(files.keys())}")
            return target_path

        # Initialize storage client
        account_url = f"https://{self.storage_account}.dfs.core.windows.net"
        credential = DefaultAzureCredential()
        storage_client = DataLakeServiceClient(account_url=account_url, credential=credential)

        # Get file system (container) client
        file_system_client = storage_client.get_file_system_client(file_system=self.container)

        # Construct full path with base_path if provided
        if self.base_path:
            full_target_path = f"{self.base_path}/{target_path}"
        else:
            full_target_path = target_path

        # Upload each file
        uploaded_count = 0
        for filename, content in files.items():
            file_path = f"{full_target_path}/{filename}"
            try:
                # Get file client
                file_client = file_system_client.get_file_client(file_path)

                # Upload file (overwrite if exists)
                file_client.upload_data(
                    content.encode("utf-8") if isinstance(content, str) else content,
                    overwrite=True,
                )
                uploaded_count += 1
            except Exception as e:
                print(f"⚠️  Failed to upload {filename}: {e}")

        # Construct ABFSS path
        abfss_path = f"abfss://{self.container}@{self.storage_account}.dfs.core.windows.net/{full_target_path}"

        print(f"📂 Uploaded {uploaded_count}/{len(files)} files to: {abfss_path}")

        return abfss_path

    def _update_job_files(self, job_id: str, files_path: str) -> None:
        """Update job definition with file paths (internal)

        For Databricks, this is typically not needed as files are
        referenced by path in the job definition.

        Args:
            job_id: Job ID to update
            files_path: Path to uploaded files
        """
        print(f"✅ Job {job_id} configured with file path: {files_path}")

    def run_job(self, job_id: str, parameters: Optional[Dict[str, Any]] = None) -> str:
        """Execute a job using SDK

        Args:
            job_id: Job ID to run
            parameters: Optional runtime parameters (Note: For Databricks, runtime python_params
                       REPLACE task-level parameters. Since bootstrap config is already in the
                       task definition, we don't pass runtime params to avoid overriding them)

        Returns:
            Run ID for monitoring
        """
        # Run job using SDK
        # NOTE: Do NOT pass python_params at runtime - they would replace the task-level parameters
        # which contain the bootstrap config (app_name, artifacts_storage_path, test_id, etc.)
        # For Databricks, all job configuration should be in the job definition, not runtime params
        run_response = self.client.jobs.run_now(job_id=int(job_id))

        return str(run_response.run_id)

    def get_job_status(self, run_id: str) -> Dict[str, Any]:
        """Get job execution status using SDK

        Args:
            run_id: Run ID to check

        Returns:
            Status dictionary with keys:
                - status: Current status
                - start_time: When job started
                - end_time: When job finished (if complete)
                - error: Error message (if failed)
        """
        # Get run info using SDK
        run_info = self.client.jobs.get_run(run_id=int(run_id))

        state = run_info.state

        return {
            "status": state.life_cycle_state.value if state.life_cycle_state else "UNKNOWN",
            "result_state": state.result_state.value if state.result_state else None,
            "start_time": run_info.start_time,
            "end_time": run_info.end_time,
            "error": state.state_message,
        }

    def get_job_logs(self, run_id: str, from_line: int = 0, size: int = 1000) -> Dict[str, Any]:
        """Get logs from a job run including driver logs from cluster log storage

        This method retrieves application logs (logger.info, logger.error, etc.) from
        the driver's log4j output stored in cluster logs (UC Volumes, DBFS mounts, or DBFS).

        Args:
            run_id: Run ID to get logs for
            from_line: Starting line offset
            size: Maximum number of log lines to return

        Returns:
            Dictionary with logs and diagnostic data
        """
        log_lines = []
        source = "diagnostic_info"

        try:
            # Get run details first (always available)
            run_info = self.client.jobs.get_run(run_id=int(run_id))

            # PRIORITY 1: Try to get run-specific logs from get_run_output API
            # This provides isolated logs for the specific run (last 5MB of stdout/stderr)
            # Available for spark_python_task and python_wheel_task
            try:
                # For multi-task jobs, get output from the task run, not the main run
                task_run_id = run_id
                if run_info.tasks and len(run_info.tasks) > 0:
                    # Use the first task's run ID
                    task = run_info.tasks[0]
                    if task.run_id:
                        task_run_id = task.run_id

                run_output = self.client.jobs.get_run_output(run_id=int(task_run_id))

                # Check for logs field (contains stdout/stderr for Python/wheel tasks)
                if hasattr(run_output, "logs") and run_output.logs:
                    log_lines = str(run_output.logs).split("\n")
                    source = "run_output_logs"

                    # Apply pagination
                    if from_line > 0 or size < len(log_lines):
                        filtered_logs = log_lines[from_line : from_line + size]
                    else:
                        filtered_logs = log_lines

                    return {
                        "id": run_id,
                        "from": from_line,
                        "total": len(log_lines),
                        "log": filtered_logs,
                        "source": source,
                        "logs_truncated": getattr(run_output, "logs_truncated", False),
                    }

            except Exception as output_error:
                # Run output may not be available yet - fall through to cluster logs
                log_lines.append(f"⚠️  Run-specific logs not available: {output_error}")
                log_lines.append("")

            # PRIORITY 2: Try to read driver logs from ABFSS storage (UC Volume backed storage)
            # NOTE: Shared cluster logs contain ALL jobs - only use as fallback
            # Get cluster ID - try run level first, then task level
            cluster_id = None
            if run_info.cluster_instance and run_info.cluster_instance.cluster_id:
                cluster_id = run_info.cluster_instance.cluster_id
            elif run_info.tasks and len(run_info.tasks) > 0:
                # For multi-task jobs, check the first task's cluster instance
                task = run_info.tasks[0]
                if task.cluster_instance and task.cluster_instance.cluster_id:
                    cluster_id = task.cluster_instance.cluster_id

            if cluster_id:
                job_name = run_info.run_name or "unknown"

                # Construct ABFSS path to driver logs
                # UC Volume /Volumes/medallion/default/logs maps to logs/ in ABFSS artifacts container
                # WARNING: These logs are shared across all jobs using the same cluster!
                if self.storage_account and self.container:
                    try:
                        # Read log file from ABFSS using Azure Storage SDK (same pattern as Fabric/Synapse)
                        from azure.identity import DefaultAzureCredential
                        from azure.storage.filedatalake import DataLakeServiceClient

                        account_url = f"https://{self.storage_account}.dfs.core.windows.net"
                        credential = DefaultAzureCredential()
                        storage_client = DataLakeServiceClient(
                            account_url=account_url, credential=credential
                        )

                        file_system_client = storage_client.get_file_system_client(
                            file_system=self.container
                        )

                        # Find the driver log directory - try multiple possible base paths
                        log_dir_found = None
                        possible_base_paths = [
                            # UC Volume backing storage (Databricks default)
                            f"logs/{cluster_id}/driver",
                            f"logs/cluster-logs/{cluster_id}/driver",
                            f"logs/cluster-logs/kindling-jobs/{job_name}/{cluster_id}/driver",
                        ]

                        for base_path in possible_base_paths:
                            try:
                                # Check if this path exists by listing it
                                paths = list(
                                    file_system_client.get_paths(path=base_path, max_results=1)
                                )
                                if paths:
                                    log_dir_found = base_path
                                    break
                            except Exception:
                                continue

                        if not log_dir_found:
                            raise Exception(
                                f"Driver log directory not found in any expected location: {possible_base_paths}"
                            )

                        # List all log files in the driver directory
                        import gzip

                        all_log_files = list(file_system_client.get_paths(path=log_dir_found))

                        # Sort log files: rotated logs first (oldest to newest), then active log
                        # This ensures we get logs in chronological order
                        log4j_files = []
                        active_log = None

                        for path_item in all_log_files:
                            filename = path_item.name.split("/")[-1]
                            if filename.startswith("log4j"):
                                if filename == "log4j-active.log":
                                    active_log = path_item.name
                                elif filename.endswith(".log.gz"):
                                    log4j_files.append(path_item.name)

                        # Sort rotated logs by filename (which includes timestamp)
                        log4j_files.sort()

                        # Add active log at the end
                        if active_log:
                            log4j_files.append(active_log)

                        if not log4j_files:
                            raise Exception(f"No log4j files found in {log_dir_found}")

                        # Read and combine all log files
                        combined_log_content = []
                        files_read = []

                        for log_file_path in log4j_files:
                            try:
                                file_client = file_system_client.get_file_client(log_file_path)
                                download = file_client.download_file()

                                if log_file_path.endswith(".gz"):
                                    # Decompress gzipped log files
                                    content = gzip.decompress(download.readall()).decode(
                                        "utf-8", errors="replace"
                                    )
                                else:
                                    content = download.readall().decode("utf-8", errors="replace")

                                combined_log_content.append(content)
                                files_read.append(log_file_path.split("/")[-1])
                            except Exception as e:
                                # Skip files we can't read
                                continue

                        if not combined_log_content:
                            raise Exception(f"Could not read any log files from {log4j_files}")

                        # Combine all logs into single content
                        log_content = "\n".join(combined_log_content)
                        log_lines = log_content.split("\n")
                        source = "driver_logs_abfss"
                        actual_path = (
                            f"{log_dir_found} ({len(files_read)} files: {', '.join(files_read)})"
                        )

                        # Success! Return the driver logs
                        if from_line > 0 or size < len(log_lines):
                            filtered_logs = log_lines[from_line : from_line + size]
                        else:
                            filtered_logs = log_lines

                        return {
                            "id": run_id,
                            "from": from_line,
                            "total": len(log_lines),
                            "log": filtered_logs,
                            "source": source,
                            "log_path": actual_path,  # Include the actual path found
                        }

                    except Exception as log_error:
                        # Log file may not be available yet or path incorrect
                        log_lines.append(f"⚠️  Could not read driver logs from ABFSS: {log_error}")
                        log_lines.append(
                            f"   ⚠️  WARNING: Cluster logs are shared across all jobs using the same cluster"
                        )
                        log_lines.append(f"   ⚠️  Unable to isolate logs for this specific run")
                        log_lines.append("")

            # PRIORITY 3: Provide diagnostic info as last resort
            # Add run status information
            if run_info.state:
                log_lines.append(f"📊 Run Status:")
                log_lines.append(
                    f"   Lifecycle State: {run_info.state.life_cycle_state.value if run_info.state.life_cycle_state else 'Unknown'}"
                )
                if run_info.state.result_state:
                    log_lines.append(f"   Result State: {run_info.state.result_state.value}")
                if run_info.state.state_message:
                    log_lines.append(f"   Message: {run_info.state.state_message}")
                log_lines.append("")

            # Try to get error details from run output
            try:
                run_output = self.client.jobs.get_run_output(run_id=int(run_id))

                # Check for error information
                if hasattr(run_output, "error") and run_output.error:
                    log_lines.append(f"❌ Execution Error:")
                    log_lines.append(f"{run_output.error}")
                    log_lines.append("")

                # Check for error trace (detailed traceback)
                if hasattr(run_output, "error_trace") and run_output.error_trace:
                    log_lines.append(f"📋 Error Traceback:")
                    log_lines.append(f"{run_output.error_trace}")
                    log_lines.append("")

                # Check for notebook output (only for notebook tasks)
                if hasattr(run_output, "notebook_output") and run_output.notebook_output:
                    log_lines.append(f"📓 Notebook Output:")
                    log_lines.extend(str(run_output.notebook_output).split("\n"))
                    log_lines.append("")

            except Exception as output_error:
                # Run output may not be available yet or for certain job types
                log_lines.append(f"⚠️  Could not retrieve run output details: {output_error}")
                log_lines.append("")

            # Add cluster information if available
            if run_info.cluster_instance:
                log_lines.append(f"🖥️  Cluster Information:")
                if run_info.cluster_instance.cluster_id:
                    log_lines.append(f"   Cluster ID: {run_info.cluster_instance.cluster_id}")
                if run_info.cluster_instance.spark_context_id:
                    log_lines.append(
                        f"   Spark Context: {run_info.cluster_instance.spark_context_id}"
                    )
                log_lines.append("")

            # Add helpful message about full logs
            log_lines.append(f"💡 To view full stdout/stderr output:")
            log_lines.append(f"   1. Open Databricks workspace UI")
            log_lines.append(f"   2. Navigate to: Workflows → Job Runs")
            log_lines.append(f"   3. Find run ID: {run_id}")
            log_lines.append(f"   4. Click 'Driver Logs' tab")
            log_lines.append(f"")
            log_lines.append(f"   OR configure Unity Catalog Volumes for automatic log delivery")

            # Apply pagination
            if from_line > 0 or size < len(log_lines):
                filtered_logs = log_lines[from_line : from_line + size]
            else:
                filtered_logs = log_lines

            return {
                "id": run_id,
                "from": from_line,
                "total": len(log_lines),
                "log": filtered_logs,
                "source": source,
            }

        except Exception as e:
            # Even basic run info failed - return error
            import traceback

            error_details = traceback.format_exc()

            return {
                "id": run_id,
                "from": 0,
                "total": 0,
                "log": [
                    f"❌ Failed to retrieve job information for run {run_id}",
                    f"",
                    f"Error: {str(e)}",
                    f"",
                    f"Details:",
                    error_details,
                    f"",
                    f"💡 To view logs, use Databricks UI:",
                    f"   Workflows → Job Runs → Run {run_id} → Driver Logs",
                ],
                "source": "error",
            }

    def cancel_job(self, run_id: str) -> bool:
        """Cancel a running job using SDK

        Args:
            run_id: Run ID to cancel

        Returns:
            True if cancelled successfully
        """
        try:
            self.client.jobs.cancel_run(run_id=int(run_id))
            return True
        except Exception:
            return False

    def stream_stdout_logs(
        self,
        job_id: str,
        run_id: str,
        callback: Optional[callable] = None,
        poll_interval: float = 5.0,
        max_wait: float = 300.0,
    ) -> List[str]:
        """Stream logs from a running Spark job by polling diagnostic emitter logs

        For Databricks, we poll the cluster log storage (UC Volumes or DBFS) where
        diagnostic emitters write driver logs. This provides application logs
        (logger.info, logger.error, etc.) with a delay due to storage flush timing.

        Note: Databricks doesn't provide real-time stdout for existing cluster jobs.
        This implementation polls diagnostic logs for a streaming-like experience.

        Args:
            job_id: Job ID (for consistency with other platforms, not used in Databricks)
            run_id: Job run ID
            callback: Optional callback function called with each new line: callback(line: str)
            poll_interval: Seconds between polls (default: 5.0)
            max_wait: Maximum seconds to wait (default: 300)

        Returns:
            Complete list of all log lines retrieved

        Example:
            >>> api = DatabricksAPI(workspace_url)
            >>> def print_log(line):
            ...     print(f"[LOG] {line}")
            >>> logs = api.stream_stdout_logs(job_id, run_id, callback=print_log)
        """
        all_logs = []
        last_line_count = 0  # Track lines already processed
        start_time = time.time()
        job_started_time = None
        log_warmup_seconds = 45  # Diagnostic emitters need time to flush to storage
        last_poll_message_time = 0

        total_timeout = max_wait + 300.0

        print(f"📡 Starting log stream for run_id={run_id}")
        print(f"   Poll interval: {poll_interval}s")
        print(f"   Max wait: {max_wait}s")
        print(f"   Total timeout: {total_timeout}s")
        print(f"   Source: Cluster diagnostic logs (UC Volumes/DBFS)")
        sys.stdout.flush()

        while True:
            elapsed = time.time() - start_time
            if elapsed > total_timeout:
                print(f"⏱️  Total timeout ({total_timeout}s) exceeded")
                sys.stdout.flush()
                break

            try:
                # Get current job status
                status_info = self.get_job_status(run_id)
                status = (status_info.get("status") or "UNKNOWN").upper()
                result_state = (status_info.get("result_state") or "").upper()

                # If job completed, do final read and exit
                if status in ["TERMINATED", "INTERNAL_ERROR", "SKIPPED"]:
                    print(
                        f"✅ Job {status.lower()} (result: {result_state or 'N/A'}) - doing final log read"
                    )
                    sys.stdout.flush()
                    # Read final logs from diagnostic emitters
                    new_logs = self._read_log_chunk_databricks(run_id, last_line_count)
                    if new_logs:
                        all_logs.extend(new_logs)
                        last_line_count = len(all_logs)
                        for log_line in new_logs:
                            if callback:
                                callback(log_line)
                    break

                # If job not started yet, wait
                if status in ["PENDING", "QUEUED", "WAITING_FOR_RETRY"]:
                    print(f"⏳ Job status: {status} - waiting...")
                    sys.stdout.flush()
                    time.sleep(poll_interval)
                    continue

                # Job is running - track when it started
                if status in ["RUNNING", "TERMINATING"]:
                    if job_started_time is None:
                        job_started_time = time.time()
                        print(
                            f"🚀 Job started running - waiting {log_warmup_seconds}s for logs to flush to storage..."
                        )
                        sys.stdout.flush()

                    # Check if we're still in the warmup period
                    time_since_start = time.time() - job_started_time
                    if time_since_start < log_warmup_seconds:
                        remaining = log_warmup_seconds - time_since_start
                        if remaining > 1:
                            print(f"⏳ Warmup period: {int(remaining)}s remaining...")
                            sys.stdout.flush()
                        time.sleep(poll_interval)
                        continue

                # Read new log content from diagnostic emitters
                new_logs = self._read_log_chunk_databricks(run_id, last_line_count)

                if new_logs:
                    all_logs.extend(new_logs)
                    last_line_count = len(all_logs)

                    # Call callback for each new line
                    for log_line in new_logs:
                        if callback:
                            callback(log_line)
                else:
                    # No new logs - print periodic progress message
                    current_time = time.time()
                    if current_time - last_poll_message_time >= 30:
                        elapsed = int(current_time - start_time)
                        print(f"🔄 Still polling for logs... ({elapsed}s elapsed)")
                        sys.stdout.flush()
                        last_poll_message_time = current_time

            except Exception as e:
                print(f"⚠️  Error during log streaming: {e}")
                sys.stdout.flush()

            # Wait before next poll
            time.sleep(poll_interval)

        print(f"📊 Streaming complete - total lines: {len(all_logs)}")
        sys.stdout.flush()
        return all_logs

    def _read_log_chunk_databricks(self, run_id: str, last_line_count: int = 0) -> List[str]:
        """Read new log content from Databricks diagnostic emitters

        Reads cluster driver logs from UC Volumes or DBFS storage where
        diagnostic emitters write application logs.

        Args:
            run_id: Job run ID
            last_line_count: Number of lines already processed

        Returns:
            List of new log lines (only lines after last_line_count)
        """
        try:
            # Use get_job_logs which already knows how to read from diagnostic emitters
            result = self.get_job_logs(run_id=run_id)

            if isinstance(result, dict) and "log" in result:
                all_lines = result["log"]
                # Return only new lines after last_line_count
                if len(all_lines) > last_line_count:
                    return all_lines[last_line_count:]

            return []

        except Exception as e:
            # Logs may not be available yet - this is expected during warmup
            return []

    def delete_job(self, job_id: str) -> bool:
        """Delete a job definition using SDK

        Args:
            job_id: Job ID to delete

        Returns:
            True if deleted successfully
        """
        try:
            self.client.jobs.delete(job_id=int(job_id))
            return True
        except Exception:
            return False

    def list_spark_jobs(self) -> list:
        """List all job definitions in the workspace using SDK

        Returns:
            List of job dictionaries
        """
        try:
            jobs = self.client.jobs.list()
            return [job.as_dict() for job in jobs]
        except Exception:
            return []

    def list_jobs(self) -> list:
        """Alias for list_spark_jobs() for compatibility

        Returns:
            List of job dictionaries
        """
        return self.list_spark_jobs()

    def create_dbfs_mount(
        self,
        mount_point: str,
        storage_account: str,
        container: str,
        client_id: str,
        client_secret: str,
        tenant_id: str,
    ) -> bool:
        """Create a DBFS mount to an ABFSS storage container

        This allows Databricks to access Azure Data Lake Storage Gen2 via DBFS paths
        like /dbfs/mnt/artifacts instead of full abfss:// URLs.

        Args:
            mount_point: Mount point path (e.g., "/mnt/artifacts")
            storage_account: Azure storage account name
            container: Container name to mount
            client_id: Azure Service Principal client ID
            client_secret: Azure Service Principal client secret
            tenant_id: Azure tenant ID

        Returns:
            True if mount created successfully

        Note:
            Requires Service Principal with Storage Blob Data Contributor role
            on the storage account. The SDK doesn't support mounts, so this
            uses the REST API directly.

        Example:
            >>> api.create_dbfs_mount(
            ...     mount_point="/mnt/artifacts",
            ...     storage_account="mystorageaccount",
            ...     container="artifacts",
            ...     client_id="...",
            ...     client_secret="...",
            ...     tenant_id="..."
            ... )
        """
        # Ensure mount point starts with /mnt/
        if not mount_point.startswith("/mnt/"):
            mount_point = f"/mnt/{mount_point.lstrip('/')}"

        # Build ABFSS source URL
        source = f"abfss://{container}@{storage_account}.dfs.core.windows.net/"

        # Build OAuth configuration for Azure
        extra_configs = {
            f"fs.azure.account.auth.type.{storage_account}.dfs.core.windows.net": "OAuth",
            f"fs.azure.account.oauth.provider.type.{storage_account}.dfs.core.windows.net": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
            f"fs.azure.account.oauth2.client.id.{storage_account}.dfs.core.windows.net": client_id,
            f"fs.azure.account.oauth2.client.secret.{storage_account}.dfs.core.windows.net": client_secret,
            f"fs.azure.account.oauth2.client.endpoint.{storage_account}.dfs.core.windows.net": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token",
        }

        # Prepare request payload
        payload = {"mount_point": mount_point, "source": source, "extra_configs": extra_configs}

        # Make REST API call using requests
        url = f"{self.workspace_url}/api/2.0/dbfs/mount"
        headers = {
            "Authorization": f"Bearer {self._get_databricks_token()}",
            "Content-Type": "application/json",
        }

        try:
            response = requests.post(url, headers=headers, json=payload)

            if response.status_code == 200:
                print(f"✅ Successfully created DBFS mount: {mount_point} → {source}")
                return True
            else:
                error_msg = response.text
                print(f"❌ Failed to create DBFS mount: HTTP {response.status_code}")
                print(f"   Response: {error_msg}")
                return False

        except Exception as e:
            print(f"❌ Error creating DBFS mount: {e}")
            return False

    def list_dbfs_mounts(self) -> list:
        """List all DBFS mounts

        Returns:
            List of mount dictionaries with mount_point and source
        """
        try:
            mounts = list(self.client.dbfs.list("/mnt"))
            return [{"mount_point": m.path, "is_dir": m.is_dir} for m in mounts]
        except Exception as e:
            print(f"⚠️  Could not list DBFS mounts: {e}")
            return []

    def unmount_dbfs(self, mount_point: str) -> bool:
        """Remove a DBFS mount

        Args:
            mount_point: Mount point to remove (e.g., "/mnt/artifacts")

        Returns:
            True if unmounted successfully
        """
        # Ensure mount point starts with /mnt/
        if not mount_point.startswith("/mnt/"):
            mount_point = f"/mnt/{mount_point.lstrip('/')}"

        url = f"{self.workspace_url}/api/2.0/dbfs/unmount"
        headers = {
            "Authorization": f"Bearer {self._get_databricks_token()}",
            "Content-Type": "application/json",
        }

        payload = {"mount_point": mount_point}

        try:
            response = requests.post(url, headers=headers, json=payload)

            if response.status_code == 200:
                print(f"✅ Successfully unmounted: {mount_point}")
                return True
            else:
                print(f"❌ Failed to unmount {mount_point}: HTTP {response.status_code}")
                print(f"   Response: {response.text}")
                return False

        except Exception as e:
            print(f"❌ Error unmounting {mount_point}: {e}")
            return False

    def _get_databricks_token(self) -> str:
        """Get Databricks API token from SDK client"""
        # The SDK client handles token acquisition via Azure CLI
        # We need to extract it for direct REST API calls
        # This is a workaround since SDK doesn't support mounts

        # Try to get token from Azure CLI directly
        try:
            result = subprocess.run(
                [
                    "az",
                    "account",
                    "get-access-token",
                    "--resource",
                    "2ff814a6-3304-4ab8-85cb-cd0e6f879c1d",
                ],
                capture_output=True,
                text=True,
                check=True,
            )
            import json

            token_data = json.loads(result.stdout)
            return token_data["accessToken"]
        except Exception as e:
            # Fallback to environment variable
            import os

            token = os.getenv("DATABRICKS_TOKEN")
            if token:
                return token
            raise Exception(f"Could not get Databricks token for REST API: {e}")


# Expose in module
__all__ = ["DatabricksAPI"]
