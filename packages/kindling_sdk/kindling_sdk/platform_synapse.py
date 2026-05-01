import json
import os
import sys
import time
import types
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Union
from urllib.parse import quote

import requests

from .platform_provider import PlatformAPI, PlatformAPIRegistry, create_azure_credential

# Synapse REST API Client (for remote operations)
# ============================================================================


def _emit_stream_progress(message: str) -> None:
    """Emit streaming progress to stdout and the optional xdist heartbeat channel."""
    print(message)
    sys.stdout.flush()

    heartbeat_path = os.getenv("KINDLING_SYSTEM_HEARTBEAT_FILE")
    if not heartbeat_path:
        return

    with open(heartbeat_path, "a", encoding="utf-8") as heartbeat_file:
        heartbeat_file.write(message + "\n")


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

        self.credential = create_azure_credential(credential=credential)

        # Storage client (lazy initialized)
        self._storage_client = None

        # Rate limiting: Synapse has 2 requests/second limit
        self._last_request_time = 0
        # 600ms between requests = ~1.67 req/sec (safe margin)
        self._min_request_interval = 0.6
        # Track batches where the diagnostic emitter root doesn't exist in storage (PathNotFound).
        # In those cases, waiting a long grace period for logs is wasted time.
        self._diag_root_missing: set[str] = set()

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

    def _resolve_default_main_file(self, job_config: Dict[str, Any]) -> str:
        explicit_main_file = str(job_config.get("main_file") or "").strip()
        if explicit_main_file:
            return explicit_main_file

        if self.base_path:
            return f"{self.base_path.strip('/')}/scripts/kindling_bootstrap.py"

        return "scripts/kindling_bootstrap.py"

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

        headers = kwargs.pop("headers", {})
        headers["Authorization"] = f"Bearer {self._get_access_token()}"
        headers["Content-Type"] = "application/json"

        for attempt in range(4):
            # Rate limiting: Ensure minimum interval between requests
            # Synapse has 2 req/sec limit, we stay safely under with 600ms interval
            current_time = time.time()
            time_since_last = current_time - self._last_request_time
            if time_since_last < self._min_request_interval:
                sleep_time = self._min_request_interval - time_since_last
                time.sleep(sleep_time)

            # Update last request time
            self._last_request_time = time.time()

            response = requests.request(method, url, headers=headers, **kwargs)

            if response.status_code != 429:
                break

            retry_after_header = response.headers.get("Retry-After", "").strip()
            retry_delay = 1.0
            if retry_after_header:
                try:
                    retry_delay = max(float(retry_after_header), retry_delay)
                except ValueError:
                    pass
            time.sleep(retry_delay * (attempt + 1))

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
        """Create a persistent Spark job definition in Azure Synapse.

        Creates (or replaces) a SparkJobDefinition resource via the Synapse REST API
        (PUT /sparkJobDefinitions/{name}?api-version=2020-12-01). The definition is
        stored server-side so run_job() works from any process without requiring the
        original job_config to be re-passed.

        Args:
            job_name: Name for the job definition (unique per workspace).
            job_config: Job configuration containing:
                - main_file: Entry point ABFSS path (required unless storage_account
                  and container are configured for default resolution)
                - command_line_args: Extra CLI arguments (optional)
                - spark_config: Spark executor/driver settings (optional)
                - app_name: Display name (optional, defaults to job_name)
                - config_overrides: Nested dict of kindling config overrides (optional)
                - configure_diagnostic_emitters: bool (optional, default False)
                - test_id: Test tracking ID injected into bootstrap args (optional)

        Returns:
            Dictionary with job_id, job_name, workspace_name.
        """
        job_id = job_name
        app_name = job_config.get("app_name", job_id)

        # Resolve main file
        main_file = self._resolve_default_main_file(job_config)
        if not main_file.startswith("abfss://"):
            if self.storage_account and self.container:
                main_file = (
                    f"abfss://{self.container}@{self.storage_account}.dfs.core.windows.net"
                    f"/{main_file}"
                )
            else:
                raise ValueError(
                    "main_file must be an ABFSS path, or storage_account and container "
                    "must be configured so the default path can be resolved."
                )

        # Build artifacts storage path (passed to bootstrap as config arg)
        if self.storage_account and self.container:
            artifacts_subpath = self.base_path.strip("/") if self.base_path else ""
            if artifacts_subpath == self.container:
                artifacts_storage_path = (
                    f"abfss://{self.container}@{self.storage_account}.dfs.core.windows.net/"
                )
            elif artifacts_subpath:
                artifacts_storage_path = (
                    f"abfss://{self.container}@{self.storage_account}.dfs.core.windows.net"
                    f"/{artifacts_subpath}"
                )
            else:
                artifacts_storage_path = (
                    f"abfss://{self.container}@{self.storage_account}.dfs.core.windows.net/"
                )
        else:
            artifacts_storage_path = job_config.get("artifacts_path", "artifacts")

        # Bootstrap params passed as positional args to the entry-point script
        bootstrap_params: Dict[str, str] = {
            "app_name": app_name,
            "artifacts_storage_path": artifacts_storage_path,
            "platform": "synapse",
            "use_lake_packages": "True",
            "load_local_packages": "False",
        }
        if "test_id" in job_config:
            bootstrap_params["test_id"] = job_config["test_id"]

        overrides = job_config.get("config_overrides", {})
        if overrides:

            def _serialize_config_value(value: Any) -> str:
                if isinstance(value, (dict, list)):
                    return json.dumps(value)
                if isinstance(value, bool):
                    return "true" if value else "false"
                if value is None:
                    return "null"
                return str(value)

            def _flatten_dict(d: Dict[str, Any], parent_key: str = "") -> Dict[str, str]:
                items: list = []
                for k, v in d.items():
                    new_key = f"{parent_key}.{k}" if parent_key else k
                    if isinstance(v, dict):
                        items.extend(_flatten_dict(v, new_key).items())
                    else:
                        items.append((new_key, _serialize_config_value(v)))
                return dict(items)

            bootstrap_params.update(_flatten_dict(overrides))

        config_args = [f"config:{k}={v}" for k, v in bootstrap_params.items()]
        additional_args = job_config.get("command_line_args", "").split()
        all_args = config_args + additional_args

        # Spark configuration
        spark_conf = job_config.get("spark_config", {})
        executor_instances = int(spark_conf.get("executor_instances", 2))
        executor_cores = int(spark_conf.get("executor_cores", 4))
        executor_memory = spark_conf.get("executor_memory", "28g")
        driver_cores = int(spark_conf.get("driver_cores", 4))
        driver_memory = spark_conf.get("driver_memory", "28g")

        conf_dict: Dict[str, str] = {
            "spark.executor.instances": str(executor_instances),
            "spark.executor.cores": str(executor_cores),
            "spark.executor.memory": executor_memory,
            "spark.driver.cores": str(driver_cores),
            "spark.driver.memory": driver_memory,
        }

        if (
            job_config.get("configure_diagnostic_emitters", False)
            and self.storage_account
            and self.container
        ):
            log_uri = f"https://{self.storage_account}.blob.core.windows.net/{self.container}/logs"
            conf_dict.update(
                {
                    "spark.synapse.diagnostic.emitters": "AzureStorageEmitter",
                    "spark.synapse.diagnostic.emitter.AzureStorageEmitter.type": "AzureStorage",
                    "spark.synapse.diagnostic.emitter.AzureStorageEmitter.categories": "Log,EventLog,Metrics",
                    "spark.synapse.diagnostic.emitter.AzureStorageEmitter.uri": log_uri,
                }
            )
            storage_key = job_config.get("storage_access_key") or self._try_get_storage_access_key()
            if storage_key:
                if job_config.get("storage_access_key"):
                    pass
                else:
                    print(
                        "🔑 Using Storage Account AccessKey for Synapse diagnostic emitters "
                        "(derived via ARM using current login)."
                    )
                conf_dict["spark.synapse.diagnostic.emitter.AzureStorageEmitter.auth"] = "AccessKey"
                conf_dict["spark.synapse.diagnostic.emitter.AzureStorageEmitter.secret"] = (
                    storage_key
                )
            else:
                print(
                    "ℹ️  No storage_access_key available; configuring Synapse diagnostic emitters "
                    "with ManagedIdentity (stdout streaming may be unavailable if MI lacks storage access)."
                )
                conf_dict["spark.synapse.diagnostic.emitter.AzureStorageEmitter.auth"] = (
                    "ManagedIdentity"
                )

        env_vars = job_config.get("environment", {})
        if env_vars:
            print("⚠️  Environment variables are not supported in Synapse SparkJobDefinition.")
            print("   Pass them via spark_config or command_line_args instead.")

        # Build SparkJobDefinition request body
        # Reference: https://learn.microsoft.com/en-us/rest/api/synapse/data-plane/spark-job-definition
        job_def_body = {
            "name": job_name,
            "properties": {
                "description": job_config.get("description", ""),
                "targetBigDataPool": {
                    "referenceName": self.spark_pool_name,
                    "type": "BigDataPoolReference",
                },
                "language": "PySpark",
                "jobProperties": {
                    "name": job_name,
                    "file": main_file,
                    "className": "",
                    "args": all_args,
                    "jars": [],
                    "files": [],
                    "archives": [],
                    "conf": conf_dict,
                    "driverMemory": driver_memory,
                    "driverCores": driver_cores,
                    "executorMemory": executor_memory,
                    "executorCores": executor_cores,
                    "numExecutors": executor_instances,
                },
            },
        }

        url = (
            f"{self.base_url}/sparkJobDefinitions/{quote(job_name, safe='')}"
            f"?api-version=2020-12-01"
        )
        self._make_request("PUT", url, json=job_def_body)
        print(f"✅ Created Synapse SparkJobDefinition: {job_name}")

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
        failed_uploads: list[str] = []
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
                failed_uploads.append(f"{filename}: {e}")

        # Construct ABFSS path
        abfss_path = f"abfss://{self.container}@{self.storage_account}.dfs.core.windows.net/{full_target_path}"

        print(f"📂 Uploaded {uploaded_count}/{len(files)} files to: {abfss_path}")

        if uploaded_count != len(files):
            failure_summary = "; ".join(failed_uploads) if failed_uploads else "unknown error"
            raise RuntimeError(
                f"Failed to upload all app files to {abfss_path}: "
                f"uploaded {uploaded_count}/{len(files)} files. Details: {failure_summary}"
            )

        return abfss_path

    def _update_job_files(self, job_id: str, files_path: str) -> None:
        """No-op: file paths are embedded in the SparkJobDefinition at create_job() time."""

    def run_job(self, job_id: str, parameters: Optional[Dict[str, Any]] = None) -> str:
        """Trigger execution of a Synapse SparkJobDefinition created by create_job().

        Calls POST /sparkJobDefinitions/{name}/execute?api-version=2020-12-01.
        The job configuration (main file, args, Spark settings) was baked into the
        definition at create_job() time and does not need to be re-passed here.

        Args:
            job_id: Job definition name (returned by create_job as job_id)
            parameters: Unused for Synapse — configuration is set at create_job() time.

        Returns:
            Batch ID (run ID) for monitoring via get_job_status() / stream_stdout_logs().

        Raises:
            Exception: If the execute call fails or returns no batch ID.
        """
        url = (
            f"{self.base_url}/sparkJobDefinitions/{quote(job_id, safe='')}"
            f"/execute?api-version=2020-12-01"
        )
        response = self._make_request("POST", url)
        result = response.json()

        batch_id = result.get("id")
        if batch_id is None:
            raise Exception(
                f"Synapse SparkJobDefinition execute did not return a batch ID "
                f"(job='{job_id}'): {result}"
            )

        print(f"🚀 Submitted Synapse batch job: job={job_id} batch_id={batch_id}")
        return str(batch_id)

    def _try_get_storage_access_key(self) -> Optional[str]:
        """Best-effort: fetch a storage account access key using ARM.

        Used only to configure Synapse diagnostic emitters for system tests.
        If RBAC doesn't permit listing keys, returns None.
        """
        import os
        import time

        # Allow callers to provide the key explicitly (useful when ARM access is restricted).
        for env_key in [
            "AZURE_STORAGE_ACCESS_KEY",
            "AZURE_STORAGE_KEY",
            "KINDLING_STORAGE_ACCESS_KEY",
            "KINDLING_SYSTEM_TEST_STORAGE_ACCESS_KEY",
        ]:
            val = (os.getenv(env_key) or "").strip()
            if val:
                return val

        subscription_id = (os.getenv("AZURE_SUBSCRIPTION_ID") or "").strip()
        resource_group = (os.getenv("AZURE_RESOURCE_GROUP") or "").strip()
        account_name = (self.storage_account or "").strip()
        if not subscription_id or not resource_group or not account_name:
            return None

        last_error: Optional[str] = None
        for attempt in range(1, 4):
            try:
                import requests

                token = self.credential.get_token("https://management.azure.com/.default").token
                url = (
                    "https://management.azure.com/subscriptions/"
                    f"{subscription_id}/resourceGroups/{resource_group}/providers/"
                    f"Microsoft.Storage/storageAccounts/{account_name}/listKeys"
                    "?api-version=2023-01-01"
                )
                resp = requests.post(
                    url,
                    headers={
                        "Authorization": f"Bearer {token}",
                        "Content-Type": "application/json",
                    },
                    timeout=30,
                )
                if resp.status_code >= 400:
                    last_error = f"HTTP {resp.status_code}: {resp.text[:300]}"
                else:
                    data = resp.json() or {}
                    keys = data.get("keys") or []
                    for item in keys:
                        value = item.get("value")
                        if value:
                            return str(value)
                    last_error = "No keys returned by ARM listKeys"
            except Exception as e:
                last_error = f"{type(e).__name__}: {e}"
            time.sleep(1.0 * attempt)

        if last_error:
            print(
                "ℹ️  Unable to fetch storage access key via ARM for diagnostic emitters: "
                f"storage_account={account_name} resource_group={resource_group} subscription_id={subscription_id} "
                f"error={last_error}"
            )

        return None

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
            # First, get batch info to find the application ID. We use this for the
            # sparkhistory driver log endpoint which is the most reliable source of
            # driver stdout/stderr in many Synapse workspaces.
            url = f"{self.base_url}/livyApi/versions/2019-11-01-preview/sparkPools/{self.spark_pool_name}/batches/{run_id}"
            response = self._make_request("GET", url)
            result = response.json()

            app_id = result.get("appId")

            # Preferred: Spark History "driverlog" endpoint (plain text download).
            # Example paths:
            #   /sparkhistory/api/v1/sparkpools/{pool}/livyid/{livyId}/applications/{appId}/driverlog/stdout/?isDownload=true
            #   /sparkhistory/api/v1/sparkpools/{pool}/livyid/{livyId}/applications/{appId}/driverlog/stderr/?isDownload=true
            if app_id:
                try:
                    token = self._get_access_token()
                    stdout_url = (
                        f"{self.base_url}/sparkhistory/api/v1/sparkpools/{self.spark_pool_name}"
                        f"/livyid/{run_id}/applications/{app_id}/driverlog/stdout/?isDownload=true"
                    )
                    stderr_url = (
                        f"{self.base_url}/sparkhistory/api/v1/sparkpools/{self.spark_pool_name}"
                        f"/livyid/{run_id}/applications/{app_id}/driverlog/stderr/?isDownload=true"
                    )

                    def _fetch_text(url: str) -> str:
                        # Don't use _make_request() because these endpoints return text.
                        resp = requests.get(
                            url,
                            headers={"Authorization": f"Bearer {token}"},
                            timeout=30,
                        )
                        if resp.status_code >= 400:
                            raise Exception(f"HTTP {resp.status_code}: {resp.text[:300]}")
                        return resp.text or ""

                    out_text = _fetch_text(stdout_url)
                    err_text = _fetch_text(stderr_url)
                    lines: List[str] = []
                    if out_text:
                        lines.extend(out_text.splitlines())
                    if err_text:
                        # Keep stderr lines distinguishable; marker matching only needs substrings.
                        lines.extend([f"[stderr] {line}" for line in err_text.splitlines()])

                    if lines:
                        # Reduce noise: system tests validate via TEST_ID markers; keep those
                        # plus a small set of error/debug lines.
                        keep_tokens = (
                            "TEST_ID=",
                            "BOOTSTRAP",
                            "Traceback",
                            "AnalysisException",
                            "Exception:",
                            "ERROR:",
                            "status=FAILED",
                            "status=COMPLETED",
                            "status=STARTED",
                            "KindlingBootstrap",
                            "complex-tracing-test",
                            "streaming-pipes-test-app",
                        )
                        filtered_all = [ln for ln in lines if any(tok in ln for tok in keep_tokens)]
                        # If filtering produces nothing (unexpected), fall back to raw lines.
                        if filtered_all:
                            lines = filtered_all

                        filtered = lines[from_line : from_line + size]
                        return {
                            "id": run_id,
                            "from": from_line,
                            "total": len(lines),
                            "log": filtered,
                            "source": "sparkhistory_driverlog",
                        }
                except Exception as e:
                    print(f"⚠️  sparkhistory driverlog endpoint failed for batch {run_id}: {e}")

            # Preferred: Livy log endpoint (returns incremental logs).
            # This captures driver stdout/stderr much better than the batch "log" field.
            try:
                log_url = (
                    f"{self.base_url}/livyApi/versions/2019-11-01-preview/sparkPools/"
                    f"{self.spark_pool_name}/batches/{run_id}/log?from={from_line}&size={size}"
                )
                log_response = self._make_request("GET", log_url)
                log_result = log_response.json() or {}
                log_lines = log_result.get("log", []) or []
                return {
                    "id": run_id,
                    "from": from_line,
                    "total": from_line + len(log_lines),
                    "log": log_lines,
                    "source": "livy_log_endpoint",
                }
            except Exception as e:
                # Fall back to batch status payload below.
                #
                # Keep this noisy: when stdout streaming is broken, this is the most actionable
                # clue for fixing the API path/permissions in a given Synapse workspace.
                print(f"⚠️  Livy log endpoint failed for batch {run_id}: {type(e).__name__}: {e}")

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

            # Some Synapse/Livy variants return logs directly on the batch payload.
            # Keep this as a fallback: it is often incomplete.
            payload_logs = result.get("log")
            if isinstance(payload_logs, list) and payload_logs:
                return {
                    "id": run_id,
                    "from": from_line,
                    "total": from_line + len(payload_logs[from_line : from_line + size]),
                    "log": payload_logs[from_line : from_line + size],
                    "source": "batch_payload",
                }

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

        def _extract_lines_from_content(raw: bytes) -> List[str]:
            out: List[str] = []
            for line in raw.decode("utf-8", errors="replace").splitlines():
                if not line.strip():
                    continue
                try:
                    entry = json.loads(line)
                except json.JSONDecodeError:
                    continue

                # Filter to only this application
                if entry.get("applicationId") != app_id:
                    continue

                category = entry.get("category", "")
                properties = entry.get("properties", {}) or {}
                timestamp = entry.get("timestamp", "")

                if category == "Log":
                    level = properties.get("level", "INFO")
                    message = properties.get("message", "")
                    logger_name = properties.get("logger_name", "")
                    out.append(f"[{timestamp}] {level} {logger_name}: {message}")
                elif category == "EventLog":
                    event_data = properties.get("Event", {})
                    if isinstance(event_data, str):
                        out.append(f"[{timestamp}] EVENT: {event_data}")
                    elif isinstance(event_data, dict):
                        event_type = event_data.get("type", "Unknown")
                        out.append(f"[{timestamp}] EVENT: {event_type}")
                elif category == "Metrics":
                    metric_name = properties.get("name", "")
                    metric_value = properties.get("value", "")
                    if metric_name:
                        out.append(f"[{timestamp}] METRIC {metric_name}: {metric_value}")

            return out

        def _read_file(path: str) -> List[str]:
            file_client = file_system_client.get_file_client(path)
            download = file_client.download_file()
            content = download.readall()
            if path.endswith(".gz"):
                content = gzip.decompress(content)
            return _extract_lines_from_content(content)

        # First try the documented single-file path.
        try:
            file_client = file_system_client.get_file_client(log_path)
            file_client.get_file_properties()
            return _read_file(log_path)
        except Exception:
            pass

        # Fallback: some workspaces emit a directory tree (multiple files) instead of a single file.
        driver_root = f"logs/{log_folder}/driver"
        candidates: List[str] = []
        try:
            for p in file_system_client.get_paths(path=driver_root, recursive=True):
                if p.is_directory:
                    continue
                name = p.name or ""
                # Capture the common patterns we've seen in Synapse workspaces.
                if (
                    "/spark-logs" in name
                    or name.endswith("spark-logs")
                    or name.endswith("spark-logs.gz")
                ):
                    candidates.append(name)
        except Exception as e:
            print(f"⚠️  Could not list diagnostic log paths under {driver_root}: {e}")
            msg = str(e)
            if "PathNotFound" in msg or "specified path does not exist" in msg.lower():
                self._diag_root_missing.add(str(batch_id))
            return []

        if not candidates:
            print(
                f"ℹ️  Log file(s) not found under {driver_root} (not written yet or emitters disabled)."
            )
            return []

        all_lines: List[str] = []
        for path in sorted(candidates):
            try:
                all_lines.extend(_read_file(path))
            except Exception as e:
                print(f"⚠️  Error reading diagnostic log file {path}: {e}")
                continue

        return all_lines

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
        last_status = None

        total_timeout = max_wait + 300.0

        _emit_stream_progress(f"📡 Starting stdout log stream for batch_id={run_id}")
        _emit_stream_progress(f"   Poll interval: {poll_interval}s")
        _emit_stream_progress(f"   Max wait: {max_wait}s")
        _emit_stream_progress(f"   Total timeout: {total_timeout}s")

        while True:
            elapsed = time.time() - start_time
            if elapsed > total_timeout:
                _emit_stream_progress(f"⏱️  Total timeout ({total_timeout}s) exceeded")
                break

            try:
                # Get current job status
                status_info = self.get_job_status(run_id)
                status = status_info.get("status", "UNKNOWN").upper()

                if status != last_status:
                    elapsed = int(time.time() - start_time)
                    _emit_stream_progress(
                        f"📊 Synapse job status changed: {last_status or 'UNKNOWN'} -> {status} "
                        f"({elapsed}s elapsed)"
                    )
                    last_status = status

                # Extract application ID when available
                if not app_id and status_info.get("app_id"):
                    app_id = status_info.get("app_id")
                    _emit_stream_progress(f"📋 Got application ID: {app_id}")

                # If job completed, do final read and exit
                if status in ["COMPLETED", "SUCCEEDED", "FAILED", "CANCELLED"]:
                    _emit_stream_progress(f"✅ Job {status.lower()} - doing final log read")
                    # Synapse log materialization can be delayed (even after the batch enters a
                    # terminal state). Retry for a short grace period so tests can see failures.
                    grace_seconds = 60.0 if str(run_id) in self._diag_root_missing else 600.0
                    grace_deadline = time.time() + grace_seconds
                    seen_useful = False
                    stagnant_polls = 0
                    while True:
                        new_logs, last_byte_offset = self._read_stdout_chunk_synapse(
                            run_id, app_id, last_byte_offset
                        )
                        if new_logs:
                            all_logs.extend(new_logs)
                            for log_line in new_logs:
                                if callback:
                                    callback(log_line)
                            stagnant_polls = 0
                        else:
                            stagnant_polls += 1
                            current_time = time.time()
                            if current_time - last_poll_message_time >= 30:
                                remaining = int(max(0, grace_deadline - current_time))
                                _emit_stream_progress(
                                    "🔄 Synapse terminal-state log materialization still in progress "
                                    f"(status={status}, stagnant_polls={stagnant_polls}, remaining_grace={remaining}s)"
                                )
                                last_poll_message_time = current_time

                        if not seen_useful and all_logs:
                            # Wait for actual app output (TEST_ID markers) instead of exiting just
                            # because we got minimal batch payload logs.
                            upper = "\n".join(all_logs[-200:]).upper()
                            if "TEST_ID=" in upper or "BOOTSTRAP SCRIPT" in upper:
                                seen_useful = True

                        # If we've seen useful output, allow an early exit once logs have stopped changing.
                        if seen_useful and stagnant_polls >= 2:
                            break

                        if time.time() >= grace_deadline:
                            break

                        time.sleep(poll_interval)
                    break

                # If job not started yet, wait
                if status in ["NOTSTARTED", "PENDING", "STARTING"]:
                    current_time = time.time()
                    if current_time - last_poll_message_time >= 30:
                        elapsed = int(current_time - start_time)
                        _emit_stream_progress(
                            f"⏳ Synapse job still waiting to start "
                            f"(status={status}, elapsed={elapsed}s)"
                        )
                        last_poll_message_time = current_time
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
                        current_time = time.time()
                        if remaining > 1 and current_time - last_poll_message_time >= 30:
                            _emit_stream_progress(
                                "⏳ Synapse stdout warmup in progress "
                                f"(status={status}, remaining={int(remaining)}s)"
                            )
                            last_poll_message_time = current_time
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
                        app_id_hint = app_id or "pending"
                        _emit_stream_progress(
                            "🔄 Synapse stdout still pending "
                            f"(status={status}, elapsed={elapsed}s, lines={len(all_logs)}, app_id={app_id_hint})"
                        )
                        last_poll_message_time = current_time

            except Exception as e:
                _emit_stream_progress(f"⚠️  Error during log streaming: {e}")

            # Wait before next poll
            time.sleep(poll_interval)

        _emit_stream_progress(f"📊 Streaming complete - total lines: {len(all_logs)}")
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
        # IMPORTANT: we treat byte_offset as a *line offset* for both Livy and storage logs.
        # This keeps offsets monotonic even when we fall back between sources.
        fallback_lines: List[str] = []
        try:
            logs = self.get_job_logs(run_id=run_id, from_line=byte_offset, size=1000000)
            src = (logs.get("source") or "").lower()
            new_lines = logs.get("log", []) or []
            # Only short-circuit for the incremental Livy endpoint. Batch payload logs are often
            # incomplete, and returning them here can prevent storage-based stdout from being read.
            if new_lines and src in ("livy_log_endpoint", "sparkhistory_driverlog"):
                return list(new_lines), byte_offset + len(new_lines)
            if new_lines:
                fallback_lines = list(new_lines)
        except Exception:
            pass

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

            # Read the whole file and treat byte_offset as a line offset.
            # This avoids mixing byte offsets (storage) with line offsets (Livy).
            download = file_client.download_file()
            content = download.readall()
            text = content.decode("utf-8", errors="replace")

            # Parse JSON lines and extract log messages
            all_messages: List[str] = []
            for line in text.splitlines():
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
                            all_messages.append(message)

                except json.JSONDecodeError:
                    continue

            if byte_offset < len(all_messages):
                new_lines = all_messages[byte_offset:]
                return list(new_lines), len(all_messages)

            # No new storage logs since last read.
            return [], byte_offset

        except Exception as e:
            # Expected during early execution - logs not available yet.
            if fallback_lines:
                return fallback_lines, byte_offset + len(fallback_lines)
            return [], byte_offset

    def delete_job(self, job_id: str) -> bool:
        """Delete a Spark job definition from Azure Synapse.

        Calls DELETE /sparkJobDefinitions/{name}?api-version=2020-12-01.
        Does not cancel active batch executions started from this definition —
        use cancel_job(run_id) for that.

        Args:
            job_id: Job definition name to delete.

        Returns:
            True if deleted (or already absent), False on error.
        """
        url = (
            f"{self.base_url}/sparkJobDefinitions/{quote(job_id, safe='')}"
            f"?api-version=2020-12-01"
        )
        try:
            response = self._make_request("DELETE", url)
            return response.status_code in (200, 204)
        except Exception as exc:
            err = str(exc)
            if "404" in err or "JobDefinitionNotFound" in err or "not found" in err.lower():
                return True  # Already gone
            print(f"⚠️  Failed to delete Synapse job definition '{job_id}': {exc}")
            return False

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
