import json
import os
import time
import types
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Union
from urllib.parse import quote

import requests

from .platform_provider import PlatformAPI, PlatformAPIRegistry

# Synapse REST API Client (for remote operations)
# ============================================================================


@PlatformAPIRegistry.register("synapse")
class SynapseAPI(PlatformAPI):
    """
    Azure Synapse REST API client for remote operations.

    This class provides REST API access to Synapse for operations like:
    - Creating Spark job definitions
    - Uploading files to ADLS
    - Running and monitoring jobs

    Does NOT require mssparkutils - pure REST API.
    Can be used from local dev, CI/CD, tests, etc.

    Authentication via Azure DefaultAzureCredential.

    Example:
        >>> from synapse_api import SynapseAPI
        >>> api = SynapseAPI("workspace-name", "spark-pool-name")
        >>> job = api.create_job("my-job", config)
        >>> run_id = api.run_job(job["job_id"])
        >>> status = api.get_job_status(run_id)
    """

    def __init__(
        self,
        workspace_name: str,
        spark_pool_name: str,
        credential: Optional[Any] = None,
        storage_account: Optional[str] = None,
        container: Optional[str] = None,
        base_path: Optional[str] = None,
    ):
        """Initialize Synapse API client

        Args:
            workspace_name: Synapse workspace name
            spark_pool_name: Spark pool name
            credential: Optional Azure credential (uses DefaultAzureCredential if not provided)
            storage_account: Optional ADLS storage account name
            container: Optional ADLS container name (filesystem)
            base_path: Optional base path within container
        """
        self.workspace_name = workspace_name
        self.spark_pool_name = spark_pool_name
        self.storage_account = storage_account
        self.container = container
        self.base_path = base_path
        self.base_url = f"https://{workspace_name}.dev.azuresynapse.net"

        # Initialize credential
        if credential is None:
            try:
                from azure.identity import DefaultAzureCredential

                self.credential = DefaultAzureCredential()
            except ImportError:
                print("⚠️  azure-identity not installed. Install with: pip install azure-identity")
                self.credential = None
        else:
            self.credential = credential

        # Storage client (lazy initialized)
        self._storage_client = None

        # Job tracking (maps job_name to batch_id for our simple implementation)
        self._job_mapping = {}

        # Rate limiting: Synapse has 2 requests/second limit
        self._last_request_time = 0
        # 600ms between requests = ~1.67 req/sec (safe margin)
        self._min_request_interval = 0.6

    @classmethod
    def from_env(cls):
        """
        Create SynapseAPI client from environment variables.

        Required environment variables:
            - SYNAPSE_WORKSPACE_NAME

        Optional environment variables:
            - SYNAPSE_SPARK_POOL or SYNAPSE_SPARK_POOL_NAME
            - AZURE_STORAGE_ACCOUNT (for file uploads)
            - AZURE_CONTAINER (default: "artifacts")
            - AZURE_BASE_PATH (default: "system-tests")

        Returns:
            SynapseAPI client instance

        Raises:
            ValueError: If required environment variables are missing
        """
        import os

        workspace_name = os.getenv("SYNAPSE_WORKSPACE_NAME")

        if not workspace_name:
            raise ValueError("Missing required environment variable: SYNAPSE_WORKSPACE_NAME")

        return cls(
            workspace_name=workspace_name,
            spark_pool_name=os.getenv("SYNAPSE_SPARK_POOL") or os.getenv("SYNAPSE_SPARK_POOL_NAME"),
            storage_account=os.getenv("AZURE_STORAGE_ACCOUNT"),
            container=os.getenv("AZURE_CONTAINER", "artifacts"),
            base_path=os.getenv("AZURE_BASE_PATH", "system-tests"),
        )

    def get_platform_name(self) -> str:
        """Get platform name"""
        return "synapse"

    def _resolve_key_vault_url(self, secret_config: Optional[Dict[str, Any]] = None) -> str:
        cfg = secret_config or {}
        return str(cfg.get("key_vault_url") or os.getenv("SYSTEM_TEST_KEY_VAULT_URL") or "").strip()

    def set_secret(
        self,
        secret_name: str,
        secret_value: str,
        secret_config: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Create/update a Key Vault secret for Synapse/Fabric-compatible secret providers."""
        key_vault_url = self._resolve_key_vault_url(secret_config)
        if not key_vault_url:
            raise ValueError(
                "Synapse secret provisioning requires key_vault_url. "
                "Set kindling.secrets.key_vault_url or SYSTEM_TEST_KEY_VAULT_URL."
            )

        import requests

        token = self.credential.get_token("https://vault.azure.net/.default").token
        encoded_name = quote(secret_name, safe="")
        url = f"{key_vault_url.rstrip('/')}/secrets/{encoded_name}?api-version=7.4"
        response = requests.put(
            url,
            headers={
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json",
            },
            json={"value": secret_value},
            timeout=30,
        )
        if response.status_code == 403:
            raise PermissionError(
                "Synapse secret provisioning was forbidden by Key Vault. "
                "Grant this principal secret write permissions (set/get/delete), "
                "for example 'Key Vault Secrets Officer' (RBAC) or equivalent access policy. "
                f"vault={key_vault_url} secret={secret_name} details={response.text}"
            )
        response.raise_for_status()

    def delete_secret(
        self,
        secret_name: str,
        secret_config: Optional[Dict[str, Any]] = None,
    ) -> bool:
        """Delete Key Vault secret used by Synapse/Fabric-compatible secret providers."""
        key_vault_url = self._resolve_key_vault_url(secret_config)
        if not key_vault_url:
            return False

        import requests

        token = self.credential.get_token("https://vault.azure.net/.default").token
        encoded_name = quote(secret_name, safe="")
        url = f"{key_vault_url.rstrip('/')}/secrets/{encoded_name}?api-version=7.4"
        response = requests.delete(
            url,
            headers={"Authorization": f"Bearer {token}"},
            timeout=30,
        )
        if response.status_code in (200, 404):
            return True
        if response.status_code == 403:
            raise PermissionError(
                "Synapse secret cleanup was forbidden by Key Vault. "
                "Grant this principal secret delete permissions. "
                f"vault={key_vault_url} secret={secret_name} details={response.text}"
            )
        response.raise_for_status()
        return True

    def deploy_app(self, app_name: str, app_files: Dict[str, str]) -> str:
        """Deploy app files to platform storage

        Uploads app files to the conventional location:
            {base_path}/data-apps/{app_name}/

        Args:
            app_name: Application name (used as storage directory)
            app_files: Dictionary of {filename: content}

        Returns:
            Storage path where app was deployed (abfss://...)
        """
        target_path = f"data-apps/{app_name}"
        return self._upload_files(app_files, target_path)

    def cleanup_app(self, app_name: str) -> bool:
        """Remove deployed app files from storage

        Args:
            app_name: Application name to clean up

        Returns:
            True if cleanup succeeded
        """
        try:
            from azure.storage.filedatalake import DataLakeServiceClient
        except ImportError:
            print("⚠️  azure-storage-file-datalake not installed")
            return False

        if not self.storage_account or not self.container:
            print("⚠️  storage_account and container must be configured for cleanup")
            return False

        try:
            account_url = f"https://{self.storage_account}.dfs.core.windows.net"
            service_client = DataLakeServiceClient(
                account_url=account_url, credential=self.credential
            )
            file_system_client = service_client.get_file_system_client(file_system=self.container)

            # Construct full path
            app_path = f"data-apps/{app_name}"
            if self.base_path:
                app_path = f"{self.base_path}/{app_path}"

            # Delete the directory recursively
            dir_client = file_system_client.get_directory_client(app_path)
            dir_client.delete_directory()
            print(f"🗑️  Cleaned up app: {app_path}")
            return True
        except Exception as e:
            print(f"⚠️  Failed to clean up app {app_name}: {e}")
            return False

    def deploy_spark_job(
        self, app_files: Dict[str, str], job_config: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Deploy app and create job in one call (backward compat)

        Delegates to the ABC base class default implementation which
        calls deploy_app() + create_job().
        """
        return super().deploy_spark_job(app_files, job_config)

    def _get_access_token(self) -> str:
        """Get Azure access token for Synapse API"""
        if not self.credential:
            raise ValueError("No credential configured")

        # Synapse uses the .default scope
        token = self.credential.get_token("https://dev.azuresynapse.net/.default")
        return token.token

    def _make_request(
        self, method: str, url: str, **kwargs
    ) -> Any:  # Returns requests.Response or similar
        """Make authenticated HTTP request to Synapse API with rate limiting"""
        import requests

        # Rate limiting: Ensure minimum interval between requests
        # Synapse has 2 req/sec limit, we stay safely under with 600ms interval
        current_time = time.time()
        time_since_last = current_time - self._last_request_time
        if time_since_last < self._min_request_interval:
            sleep_time = self._min_request_interval - time_since_last
            time.sleep(sleep_time)

        # Update last request time
        self._last_request_time = time.time()

        headers = kwargs.pop("headers", {})
        headers["Authorization"] = f"Bearer {self._get_access_token()}"
        headers["Content-Type"] = "application/json"

        response = requests.request(method, url, headers=headers, **kwargs)

        # Handle errors
        if response.status_code >= 400:
            try:
                error_detail = response.json()
                error_msg = error_detail.get("error", {}).get("message", response.text)
            except:
                error_msg = response.text

            raise Exception(f"Synapse API request failed: {response.status_code} - {error_msg}")

        return response

    def create_job(self, job_name: str, job_config: Dict[str, Any]) -> Dict[str, Any]:
        """Create a Spark job definition in Synapse

        NOTE: Synapse doesn't have persistent job definitions like Fabric.
        Instead, we submit Spark batch jobs directly. This method stores the
        job config for later use when run_job() is called.

        Args:
            job_name: Name for the job
            job_config: Job configuration containing:
                - main_file: Entry point file path in ADLS
                - command_line_args: Command line arguments (optional)
                - spark_config: Spark configuration (optional)
                - environment: Environment variables (optional)

        Returns:
            Dictionary with job_id (same as job_name for Synapse)
        """
        # In Synapse, there's no separate "job definition" creation step
        # Jobs are submitted directly as Spark batch jobs
        # We'll store the config and return the job_name as job_id

        # Generate a unique job ID
        job_id = job_name

        # Store config for later use in run_job()
        self._job_mapping[job_id] = {
            "job_name": job_name,
            "job_config": job_config,
            "created_at": None,  # Will be set when run_job() is called
        }

        return {
            "job_id": job_id,
            "job_name": job_name,
            "workspace_name": self.workspace_name,
        }

    def _upload_files(self, files: Dict[str, str], target_path: str) -> str:
        """Upload files to ADLS Gen2 (internal)

        Args:
            files: Dictionary of {filename: content}
            target_path: Target path in container (e.g., "data-apps/my-app")

        Returns:
            ABFSS path where files were uploaded
        """
        try:
            from azure.storage.filedatalake import DataLakeServiceClient
        except ImportError:
            print("⚠️  azure-storage-file-datalake not installed")
            print("   Install with: pip install azure-storage-file-datalake")
            raise ImportError("azure-storage-file-datalake required for file uploads")

        if not self.storage_account or not self.container:
            raise ValueError("storage_account and container must be configured for file uploads")

        # Initialize storage client if needed
        if not self._storage_client:
            account_url = f"https://{self.storage_account}.dfs.core.windows.net"
            self._storage_client = DataLakeServiceClient(
                account_url=account_url, credential=self.credential
            )

        # Get file system (container) client
        file_system_client = self._storage_client.get_file_system_client(file_system=self.container)

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

        For Synapse, this updates the stored job config with the files path
        which will be used when the job is run.

        Args:
            job_id: Job ID to update
            files_path: ABFSS path to uploaded files
        """
        if job_id not in self._job_mapping:
            raise ValueError(f"Job {job_id} not found")

        # Store the files path in the job config
        self._job_mapping[job_id]["files_path"] = files_path

    def run_job(self, job_id: str, parameters: Optional[Dict[str, Any]] = None) -> str:
        """Execute a Spark batch job

        Args:
            job_id: Job ID to run (from create_job)
            parameters: Optional runtime parameters

        Returns:
            Batch ID (run ID) for monitoring
        """
        if job_id not in self._job_mapping:
            raise ValueError(f"Job {job_id} not found. Call create_job first.")

        job_info = self._job_mapping[job_id]
        job_config = job_info["job_config"]

        # Build Spark batch job request
        # Reference: https://docs.microsoft.com/en-us/rest/api/synapse/data-plane/spark-batch/create-spark-batch-job

        app_name = job_config.get("app_name", job_id)
        main_file = job_config.get("main_file", "scripts/kindling_bootstrap.py")

        # Convert to ABFSS path if needed
        if not main_file.startswith("abfss://"):
            if self.storage_account and self.container:
                main_file = f"abfss://{self.container}@{self.storage_account}.dfs.core.windows.net/{main_file}"
            else:
                raise ValueError(
                    "main_file must be ABFSS path or storage_account/container must be configured"
                )

        # Build command line args with bootstrap config
        # For Synapse, we need to provide the FULL ABFSS path for artifacts
        # since Synapse doesn't support shortcuts like "Files/artifacts"
        if self.storage_account and self.container:
            # Use self.base_path instead of job_config to ensure correct storage location
            # e.g., system-tests/run-123/synapse/data-apps/my-app/
            artifacts_subpath = self.base_path.strip("/") if self.base_path else ""

            # If artifacts_path matches container name, don't duplicate it in the path
            # e.g., if container is "artifacts" and artifacts_path is "artifacts",
            # use abfss://artifacts@.../ not abfss://artifacts@.../artifacts
            if artifacts_subpath == self.container:
                artifacts_storage_path = (
                    f"abfss://{self.container}@{self.storage_account}.dfs.core.windows.net/"
                )
            elif artifacts_subpath:
                artifacts_storage_path = f"abfss://{self.container}@{self.storage_account}.dfs.core.windows.net/{artifacts_subpath}"
            else:
                # No subpath specified, use root of container
                artifacts_storage_path = (
                    f"abfss://{self.container}@{self.storage_account}.dfs.core.windows.net/"
                )
        else:
            # Fallback to relative path (will likely fail without storage config)
            artifacts_storage_path = job_config.get("artifacts_path", "artifacts")

        bootstrap_params = {
            "app_name": app_name,
            "artifacts_storage_path": artifacts_storage_path,  # Full ABFSS path
            "platform": "synapse",
            "use_lake_packages": "True",
            "load_local_packages": "False",
        }

        # Add test_id if provided (for test tracking)
        if "test_id" in job_config:
            bootstrap_params["test_id"] = job_config["test_id"]

        overrides = job_config.get("config_overrides", {})
        if overrides:

            def flatten_dict(d, parent_key=""):
                items = []
                for k, v in d.items():
                    new_key = f"{parent_key}.{k}" if parent_key else k
                    if isinstance(v, dict):
                        items.extend(flatten_dict(v, new_key).items())
                    else:
                        items.append((new_key, v))
                return dict(items)

            bootstrap_params.update(flatten_dict(overrides))

        config_args = [f"config:{k}={v}" for k, v in bootstrap_params.items()]
        additional_args = job_config.get("command_line_args", "").split()
        all_args = config_args + additional_args

        # Build Spark configuration
        spark_conf = job_config.get("spark_config", {})
        conf_dict = {}

        # Synapse requires executor instances to be set
        if "executor_instances" in spark_conf:
            conf_dict["spark.executor.instances"] = str(spark_conf["executor_instances"])
        else:
            # Default to 2 executors
            conf_dict["spark.executor.instances"] = "2"

        # Synapse requires executor_cores to be set to a valid integer
        if "executor_cores" in spark_conf:
            conf_dict["spark.executor.cores"] = str(spark_conf["executor_cores"])
        else:
            # Default to 4 cores per executor
            conf_dict["spark.executor.cores"] = "4"

        # Synapse requires executor_memory to be set
        if "executor_memory" in spark_conf:
            conf_dict["spark.executor.memory"] = spark_conf["executor_memory"]
        else:
            # Default to 28GB per executor
            conf_dict["spark.executor.memory"] = "28g"

        # Synapse requires driver_cores to be set to a valid integer
        if "driver_cores" in spark_conf:
            conf_dict["spark.driver.cores"] = str(spark_conf["driver_cores"])
        else:
            # Default to 4 cores for driver
            conf_dict["spark.driver.cores"] = "4"

        # Synapse requires driver_memory to be set
        if "driver_memory" in spark_conf:
            conf_dict["spark.driver.memory"] = spark_conf["driver_memory"]
        else:
            # Default to 28GB for driver
            conf_dict["spark.driver.memory"] = "28g"

        # Configure Synapse diagnostic emitters only if explicitly requested via job config
        # If not specified, use the Spark pool's default configuration
        # Reference: https://learn.microsoft.com/en-us/azure/synapse-analytics/spark/azure-synapse-diagnostic-emitters-azure-storage
        if job_config.get("configure_diagnostic_emitters", False):
            if self.storage_account and self.container:
                # Use blob endpoint (not ADLS/abfss) as per Synapse documentation
                log_uri = (
                    f"https://{self.storage_account}.blob.core.windows.net/{self.container}/logs"
                )

                # Configure diagnostic emitter to write logs, event logs, and metrics
                conf_dict["spark.synapse.diagnostic.emitters"] = "AzureStorageEmitter"
                conf_dict["spark.synapse.diagnostic.emitter.AzureStorageEmitter.type"] = (
                    "AzureStorage"
                )
                conf_dict["spark.synapse.diagnostic.emitter.AzureStorageEmitter.categories"] = (
                    "Log,EventLog,Metrics"
                )
                conf_dict["spark.synapse.diagnostic.emitter.AzureStorageEmitter.uri"] = log_uri

                # Use AccessKey authentication if provided in job config, otherwise try ManagedIdentity
                storage_key = job_config.get("storage_access_key")
                if storage_key:
                    conf_dict["spark.synapse.diagnostic.emitter.AzureStorageEmitter.auth"] = (
                        "AccessKey"
                    )
                    conf_dict["spark.synapse.diagnostic.emitter.AzureStorageEmitter.secret"] = (
                        storage_key
                    )
                else:
                    # Fallback to managed identity (may not work without proper permissions)
                    conf_dict["spark.synapse.diagnostic.emitter.AzureStorageEmitter.auth"] = (
                        "ManagedIdentity"
                    )

        # Build batch job payload
        batch_payload = {
            "name": job_id,  # Batch job name
            "file": main_file,  # Main Python file (ABFSS path)
            "args": all_args,  # Command line arguments
            "conf": conf_dict,  # Spark configuration
        }

        # Add environment variables if provided
        env_vars = job_config.get("environment", {})
        if env_vars:
            # Synapse doesn't directly support env vars in batch API
            # They would need to be set via Spark conf or passed as args
            print(f"⚠️  Environment variables not directly supported in Synapse batch jobs")
            print(f"   Consider passing via Spark conf or command line args")

        # Submit batch job
        url = f"{self.base_url}/livyApi/versions/2019-11-01-preview/sparkPools/{self.spark_pool_name}/batches"

        response = self._make_request("POST", url, json=batch_payload)
        result = response.json()

        # Extract batch ID
        batch_id = result.get("id")
        if batch_id is None:
            raise Exception(f"No batch ID in response: {result}")

        # Update job mapping with batch ID
        self._job_mapping[job_id]["batch_id"] = batch_id
        self._job_mapping[job_id]["created_at"] = result.get("submittedAt")

        return str(batch_id)

    def get_job_status(self, run_id: str) -> Dict[str, Any]:
        """Get job execution status

        Args:
            run_id: Batch ID from run_job()

        Returns:
            Status dictionary with keys:
                - status: Job status (e.g., "not_started", "starting", "running", "success", "dead", "killed")
                - start_time: Start time (if available)
                - end_time: End time (if available)
                - error: Error message (if failed)
        """
        try:
            # Get batch job status
            url = f"{self.base_url}/livyApi/versions/2019-11-01-preview/sparkPools/{self.spark_pool_name}/batches/{run_id}"

            response = self._make_request("GET", url)
            result = response.json()

            # Map Livy status to our standard status
            # Livy statuses: not_started, starting, running, idle, busy, shutting_down, error, dead, killed, success
            livy_state = result.get("state", "unknown").lower()

            # Map to our standardized statuses
            status_mapping = {
                "not_started": "NotStarted",
                "starting": "InProgress",
                "running": "InProgress",
                "idle": "InProgress",
                "busy": "InProgress",
                "shutting_down": "InProgress",
                "success": "Completed",
                "dead": "Failed",
                "killed": "Cancelled",
                "error": "Failed",
            }

            status = status_mapping.get(livy_state, livy_state)

            # Build response
            status_dict = {
                "status": status,
                "livy_state": livy_state,  # Keep original for debugging
                "batch_id": run_id,
            }

            # Include app_id for log retrieval (needed by stream_stdout_logs)
            if "appId" in result and result["appId"]:
                status_dict["app_id"] = result["appId"]

            # Add timestamps if available
            if "submittedAt" in result:
                status_dict["start_time"] = result["submittedAt"]
            if "endedAt" in result:
                status_dict["end_time"] = result["endedAt"]

            # Add error info if failed
            if livy_state in ["error", "dead"]:
                errors = result.get("errors", [])
                if errors:
                    status_dict["error"] = "; ".join(errors)
                else:
                    # Check for error message in other fields
                    if "errorMessage" in result:
                        status_dict["error"] = result["errorMessage"]

            return status_dict

        except Exception as e:
            # If we can't get status, return an error status
            return {
                "status": "Unknown",
                "error": f"Failed to get job status: {str(e)}",
                "batch_id": run_id,
            }

    def get_job_logs(self, run_id: str, from_line: int = 0, size: int = 1000) -> Dict[str, Any]:
        """Get logs from a Spark batch job

        Args:
            run_id: Batch ID to get logs for
            from_line: Starting line number (0-based, for compatibility - not used in Synapse)
            size: Number of lines to retrieve (for compatibility - Synapse returns all logs)

        Returns:
            Dictionary with log lines and total count
        """
        try:
            # First, get batch info to find the application ID
            url = f"{self.base_url}/livyApi/versions/2019-11-01-preview/sparkPools/{self.spark_pool_name}/batches/{run_id}"
            response = self._make_request("GET", url)
            result = response.json()

            app_id = result.get("appId")

            # If we have storage configured, try to read event logs from Azure Storage
            if self.storage_account and self.container and app_id:
                try:
                    print(
                        f"🔍 Attempting to read diagnostic logs for app_id={app_id}, batch_id={run_id}"
                    )
                    log_lines = self._read_event_logs_from_storage(app_id, run_id)
                    print(f"📊 Retrieved {len(log_lines)} log lines from storage")
                    if log_lines:
                        # Apply from_line and size filtering if needed
                        if from_line > 0 or size < len(log_lines):
                            filtered_logs = log_lines[from_line : from_line + size]
                        else:
                            filtered_logs = log_lines

                        return {
                            "id": run_id,
                            "from": from_line,
                            "total": len(log_lines),
                            "log": filtered_logs,
                            "source": "diagnostic_emitters",
                        }
                except Exception as e:
                    print(f"⚠️  Could not read event logs from storage: {e}")
                    import traceback

                    traceback.print_exc()
                    print(f"   Falling back to Livy API logs")

            # Fallback to Livy API logs (incomplete but better than nothing)
            log_lines = result.get("log", [])

            # Apply from_line and size filtering if needed
            if from_line > 0 or size < len(log_lines):
                filtered_logs = log_lines[from_line : from_line + size]
            else:
                filtered_logs = log_lines

            return {"id": run_id, "from": from_line, "total": len(log_lines), "log": filtered_logs}
        except Exception as e:
            print(f"⚠️  Failed to get logs for job {run_id}: {e}")
            return {"id": run_id, "from": 0, "total": 0, "log": []}

    def _read_event_logs_from_storage(self, app_id: str, batch_id: str) -> List[str]:
        """Read Spark diagnostic logs from Azure Storage (Synapse diagnostic emitters)

        Synapse diagnostic emitters write logs to:
        logs/<workspaceName>.<sparkPoolName>.<batchId>/driver/spark-logs

        Format: JSON lines with categories: Log, EventLog, Metrics

        Reference: https://learn.microsoft.com/en-us/azure/synapse-analytics/spark/azure-synapse-diagnostic-emitters-azure-storage

        Args:
            app_id: Spark application ID (e.g., application_1234567890_0001)
            batch_id: Batch/session ID

        Returns:
            List of log lines extracted from diagnostic log files
        """
        try:
            import gzip
            import json

            from azure.storage.filedatalake import DataLakeServiceClient
        except ImportError:
            print("⚠️  azure-storage-file-datalake not installed")
            return []

        if not self.storage_account or not self.container:
            return []

        # Initialize storage client if needed
        if not self._storage_client:
            account_url = f"https://{self.storage_account}.dfs.core.windows.net"
            self._storage_client = DataLakeServiceClient(
                account_url=account_url, credential=self.credential
            )

        file_system_client = self._storage_client.get_file_system_client(file_system=self.container)

        # Synapse diagnostic logs path pattern: logs/<workspaceName>.<sparkPoolName>.<batchId>/driver/spark-logs
        # Construct the exact path using known values
        log_folder = f"{self.workspace_name}.{self.spark_pool_name}.{batch_id}"
        log_path = f"logs/{log_folder}/driver/spark-logs"

        print(f"📂 Reading logs from: {log_path}")

        log_lines = []

        try:
            # Read the driver spark-logs file
            file_client = file_system_client.get_file_client(log_path)

            # Check if file exists first
            try:
                file_props = file_client.get_file_properties()
            except Exception as exists_error:
                # File doesn't exist yet (diagnostic emitters may not have written logs)
                print(f"ℹ️  Log file not found (may not be written yet): {log_path}")
                print(
                    f"   This is normal if the job just completed or diagnostic emitters are not configured"
                )
                return []

            download = file_client.download_file()
            content = download.readall()

            # Logs may be compressed
            if log_path.endswith(".gz"):
                content = gzip.decompress(content)

            # Parse diagnostic log entries (JSON lines format)
            # Format: {timestamp, category, workspaceName, sparkPool, applicationId, executorId, properties: {message, level, ...}}
            for line in content.decode("utf-8").splitlines():
                if line.strip():
                    try:
                        entry = json.loads(line)

                        # Filter to only this application
                        if entry.get("applicationId") != app_id:
                            continue

                        category = entry.get("category", "")
                        properties = entry.get("properties", {})
                        timestamp = entry.get("timestamp", "")

                        # Extract log messages
                        if category == "Log":
                            # Driver and executor logs
                            level = properties.get("level", "INFO")
                            message = properties.get("message", "")
                            logger_name = properties.get("logger_name", "")

                            # Format like standard Spark logs
                            log_line = f"[{timestamp}] {level} {logger_name}: {message}"
                            log_lines.append(log_line)

                        elif category == "EventLog":
                            # Spark events
                            event_data = properties.get("Event", {})
                            if isinstance(event_data, str):
                                log_lines.append(f"[{timestamp}] EVENT: {event_data}")
                            elif isinstance(event_data, dict):
                                event_type = event_data.get("type", "Unknown")
                                log_lines.append(f"[{timestamp}] EVENT: {event_type}")

                        elif category == "Metrics":
                            # Optionally include metrics
                            metric_name = properties.get("name", "")
                            metric_value = properties.get("value", "")
                            if metric_name:
                                log_lines.append(
                                    f"[{timestamp}] METRIC {metric_name}: {metric_value}"
                                )

                    except json.JSONDecodeError:
                        # Skip invalid JSON lines
                        continue

            return log_lines

        except Exception as e:
            print(f"⚠️  Error reading diagnostic logs from {log_path}: {e}")
            return []

        log_lines = []

        try:
            # List all subdirectories in synapse-logs to find the one matching our app
            paths = file_system_client.get_paths(path=log_base_path)

            # Find the directory that contains our application ID
            target_dir = None
            for path in paths:
                if path.is_directory:
                    # Check if this directory contains files for our app
                    # Directory name format: <workspaceName>.<sparkPoolName>.<livySessionId>
                    # We'll need to check files inside to find matching app_id
                    try:
                        subpaths = file_system_client.get_paths(path=path.name)
                        for subpath in subpaths:
                            if not subpath.is_directory:
                                # Try to read first few lines to check if it's our app
                                file_client = file_system_client.get_file_client(subpath.name)
                                download = file_client.download_file()
                                # Read first 1KB
                                sample = download.readall()[:1000]

                                if (
                                    app_id.encode() in sample
                                    or self.workspace_name.encode() in sample
                                ):
                                    target_dir = path.name
                                    break
                    except:
                        continue

                if target_dir:
                    break

            if not target_dir:
                print(f"⚠️  No diagnostic logs found for application {app_id}")
                return []

            # Now read all files in the target directory
            all_paths = file_system_client.get_paths(path=target_dir)

            for path in all_paths:
                if path.is_directory:
                    continue

                try:
                    # Read the diagnostic log file
                    file_client = file_system_client.get_file_client(path.name)
                    download = file_client.download_file()
                    content = download.readall()

                    # Logs may be compressed
                    if path.name.endswith(".gz"):
                        content = gzip.decompress(content)

                    # Parse diagnostic log entries (JSON lines format)
                    # Format per docs: {timestamp, category, workspaceName, sparkPool, livyId, applicationId, properties: {message, level, ...}}
                    for line in content.decode("utf-8").splitlines():
                        if line.strip():
                            try:
                                entry = json.loads(line)

                                # Filter to only this application
                                if entry.get("applicationId") != app_id:
                                    continue

                                category = entry.get("category", "")
                                properties = entry.get("properties", {})

                                # Extract log messages
                                if category == "Log":
                                    # Driver and executor logs
                                    level = properties.get("level", "INFO")
                                    message = properties.get("message", "")
                                    logger_name = properties.get("logger_name", "")
                                    timestamp = entry.get("timestamp", "")

                                    log_line = f"[{timestamp}] {level} {logger_name}: {message}"
                                    log_lines.append(log_line)

                                elif category == "EventLog":
                                    # Spark events (similar to standard event logs)
                                    event_type = properties.get("Event", "")

                                    if event_type == "SparkListenerApplicationStart":
                                        log_lines.append(f"=== Application Started ===")
                                        log_lines.append(
                                            f"App: {properties.get('App Name', 'N/A')}"
                                        )
                                        log_lines.append(f"User: {properties.get('User', 'N/A')}")

                                    elif event_type == "SparkListenerJobStart":
                                        job_id = properties.get("Job ID", "N/A")
                                        log_lines.append(f"Job {job_id} started")

                                    elif event_type == "SparkListenerJobEnd":
                                        job_id = properties.get("Job ID", "N/A")
                                        result = properties.get("Job Result", {}).get(
                                            "Result", "N/A"
                                        )
                                        log_lines.append(f"Job {job_id}: {result}")

                                    elif event_type == "SparkListenerTaskEnd":
                                        task_info = properties.get("Task Info", {})
                                        if not task_info.get("Successful", True):
                                            reason = properties.get("Task End Reason", {}).get(
                                                "Reason", "Unknown"
                                            )
                                            log_lines.append(f"Task failed: {reason}")

                                    elif event_type == "SparkListenerApplicationEnd":
                                        log_lines.append(f"=== Application Completed ===")

                                elif category == "Metrics":
                                    # Can optionally include metrics
                                    metric_name = properties.get("name", "")
                                    metric_value = properties.get("value", "")
                                    if metric_name and metric_value:
                                        log_lines.append(f"Metric {metric_name}: {metric_value}")

                            except json.JSONDecodeError:
                                # Skip invalid JSON lines
                                continue

                except Exception as file_error:
                    print(f"⚠️  Error reading file {path.name}: {file_error}")
                    continue

            return log_lines

        except Exception as e:
            print(f"⚠️  Error reading event logs from storage: {e}")
            return []

    def cancel_job(self, run_id: str) -> bool:
        """Cancel a running job

        Args:
            run_id: Batch ID to cancel

        Returns:
            True if cancelled successfully
        """
        # Delete the batch job to cancel it
        url = f"{self.base_url}/livyApi/versions/2019-11-01-preview/sparkPools/{self.spark_pool_name}/batches/{run_id}"

        try:
            response = self._make_request("DELETE", url)
            return response.status_code in [200, 204]
        except Exception as e:
            print(f"⚠️  Failed to cancel job {run_id}: {e}")
            return False

    def stream_stdout_logs(
        self,
        job_id: str,
        run_id: str,
        callback: Optional[callable] = None,
        poll_interval: float = 5.0,
        max_wait: float = 300.0,
    ) -> List[str]:
        """Stream stdout logs from a running Spark job in real-time

        For Synapse Spark jobs, stdout logs are written to diagnostic emitter storage.
        This method polls the batch status and reads new log content incrementally.

        Note: Synapse diagnostic logs have a delay (typically 2-5 minutes) before
        appearing in storage after job completion.

        Args:
            job_id: Job name (for consistency with other platforms, not used in Synapse)
            run_id: Batch ID
            callback: Optional callback function called with each new line: callback(line: str)
            poll_interval: Seconds between polls (default: 5.0)
            max_wait: Maximum seconds to wait (default: 300)

        Returns:
            Complete list of all log lines retrieved

        Example:
            >>> api = SynapseAPI(workspace_name, spark_pool_name)
            >>> def print_log(line):
            ...     print(f"[STDOUT] {line}")
            >>> logs = api.stream_stdout_logs(job_id, run_id, callback=print_log)
        """
        all_logs = []
        last_byte_offset = 0
        start_time = time.time()
        job_started_time = None
        stdout_warmup_seconds = 45  # Similar to Fabric
        last_poll_message_time = 0
        app_id = None  # Spark application ID

        total_timeout = max_wait + 300.0

        print(f"📡 Starting stdout log stream for batch_id={run_id}")
        print(f"   Poll interval: {poll_interval}s")
        print(f"   Max wait: {max_wait}s")
        print(f"   Total timeout: {total_timeout}s")
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
                status = status_info.get("status", "UNKNOWN").upper()

                # Extract application ID when available
                if not app_id and status_info.get("app_id"):
                    app_id = status_info.get("app_id")
                    print(f"📋 Got application ID: {app_id}")
                    sys.stdout.flush()

                # If job completed, do final read and exit
                if status in ["COMPLETED", "SUCCEEDED", "FAILED", "CANCELLED"]:
                    print(f"✅ Job {status.lower()} - doing final log read")
                    sys.stdout.flush()
                    # Read final logs
                    new_logs, last_byte_offset = self._read_stdout_chunk_synapse(
                        run_id, app_id, last_byte_offset
                    )
                    if new_logs:
                        all_logs.extend(new_logs)
                        for log_line in new_logs:
                            if callback:
                                callback(log_line)
                    break

                # If job not started yet, wait
                if status in ["NOTSTARTED", "PENDING", "STARTING"]:
                    print(f"⏳ Job status: {status} - waiting...")
                    sys.stdout.flush()
                    time.sleep(poll_interval)
                    continue

                # Job is running - track when it started
                if status in ["INPROGRESS", "RUNNING"]:
                    if job_started_time is None:
                        job_started_time = time.time()
                        print(
                            f"🚀 Job started running - waiting {stdout_warmup_seconds}s for stdout to become available..."
                        )
                        sys.stdout.flush()

                    # Check if we're still in the warmup period
                    time_since_start = time.time() - job_started_time
                    if time_since_start < stdout_warmup_seconds:
                        remaining = stdout_warmup_seconds - time_since_start
                        if remaining > 1:
                            print(f"⏳ Warmup period: {int(remaining)}s remaining...")
                            sys.stdout.flush()
                        time.sleep(poll_interval)
                        continue

                # Read new log content (requires app_id for log location)
                new_logs, last_byte_offset = self._read_stdout_chunk_synapse(
                    run_id, app_id, last_byte_offset
                )

                if new_logs:
                    all_logs.extend(new_logs)

                    # Call callback for each new line
                    for log_line in new_logs:
                        if callback:
                            callback(log_line)
                else:
                    # No new logs - print periodic progress message
                    current_time = time.time()
                    if current_time - last_poll_message_time >= 30:
                        elapsed = int(current_time - start_time)
                        print(f"🔄 Still polling for stdout... ({elapsed}s elapsed)")
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

    def _read_stdout_chunk_synapse(
        self, run_id: str, app_id: Optional[str], byte_offset: int = 0
    ) -> tuple:
        """Read new stdout content from Synapse diagnostic emitter logs

        Synapse doesn't have a raw stdout file. Instead, diagnostic emitters write
        JSON-lines to: logs/{workspace}.{pool}.{batch_id}/driver/spark-logs

        Each line is a JSON object with category, applicationId, properties.message etc.
        We extract "Log" category messages that match our app_id.

        Args:
            run_id: Batch ID
            app_id: Spark application ID (if available)
            byte_offset: Starting byte position

        Returns:
            Tuple of (new_lines, new_byte_offset)
        """
        if not app_id:
            # Can't read logs without application ID
            return [], byte_offset

        if not self.storage_account or not self.container:
            return [], byte_offset

        try:
            import json

            from azure.storage.filedatalake import DataLakeServiceClient

            # Reuse or create storage client
            if not self._storage_client:
                account_url = f"https://{self.storage_account}.dfs.core.windows.net"
                self._storage_client = DataLakeServiceClient(
                    account_url=account_url, credential=self.credential
                )

            file_system_client = self._storage_client.get_file_system_client(
                file_system=self.container
            )

            # Synapse diagnostic emitter path (single file, not a directory)
            log_path = (
                f"logs/{self.workspace_name}.{self.spark_pool_name}.{run_id}/driver/spark-logs"
            )

            file_client = file_system_client.get_file_client(log_path)

            # Get file properties to check size
            properties = file_client.get_file_properties()
            file_size = properties.size

            if file_size <= byte_offset:
                # No new content
                return [], byte_offset

            # Read new content from byte_offset
            download = file_client.download_file(offset=byte_offset, length=file_size - byte_offset)
            content = download.readall()
            new_text = content.decode("utf-8")
            new_byte_offset = byte_offset + len(content)

            # Parse JSON lines and extract log messages
            new_lines = []
            for line in new_text.splitlines():
                if not line.strip():
                    continue
                try:
                    entry = json.loads(line)

                    # Filter to our application
                    if entry.get("applicationId") != app_id:
                        continue

                    category = entry.get("category", "")
                    properties_dict = entry.get("properties", {})

                    if category == "Log":
                        message = properties_dict.get("message", "")
                        if message:
                            new_lines.append(message)

                except json.JSONDecodeError:
                    continue

            return new_lines, new_byte_offset

        except Exception as e:
            # Expected during early execution - logs not available yet
            return [], byte_offset

    def delete_job(self, job_id: str) -> bool:
        """Delete a job definition

        For Synapse, this removes the job from our internal tracking.
        If the job is currently running, it will cancel the batch.
        Completed/failed jobs are left as-is.

        Args:
            job_id: Job ID to delete

        Returns:
            True if deleted successfully
        """
        if job_id not in self._job_mapping:
            print(f"⚠️  Job {job_id} not found in tracking")
            return False

        job_info = self._job_mapping[job_id]

        # Only cancel if job is still running
        if "batch_id" in job_info:
            batch_id = job_info["batch_id"]
            try:
                # Check current status before cancelling
                status_result = self.get_job_status(run_id=str(batch_id))
                status = status_result.get("status", "").upper()

                # Only cancel if still in progress
                if status in ["NOTSTARTED", "STARTING", "RUNNING", "INPROGRESS"]:
                    self.cancel_job(str(batch_id))
                # Leave completed/failed jobs as-is
            except:
                pass  # Best effort

        # Remove from tracking
        del self._job_mapping[job_id]
        return True

    def list_spark_jobs(self) -> list:
        """List Spark batch jobs

        Returns:
            List of job dictionaries with id and displayName
        """
        # List all batches in the Spark pool
        url = f"{self.base_url}/livyApi/versions/2019-11-01-preview/sparkPools/{self.spark_pool_name}/batches"

        try:
            response = self._make_request("GET", url)
            result = response.json()

            # Extract batches
            batches = result.get("sessions", [])  # Livy calls them "sessions"

            # Convert to our format
            jobs = []
            for batch in batches:
                jobs.append(
                    {
                        "id": str(batch.get("id")),
                        "displayName": batch.get("name", f"batch-{batch.get('id')}"),
                        "state": batch.get("state"),
                    }
                )

            return jobs
        except Exception as e:
            print(f"⚠️  Failed to list jobs: {e}")
            return []


# Expose in module
__all__ = ["SynapseAPI"]
