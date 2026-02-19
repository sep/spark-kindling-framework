import base64
import json
import os
import shlex
import subprocess
import sys
import time
import types
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Union
from urllib.parse import quote

import requests
from azure.core.exceptions import *
from azure.identity import DefaultAzureCredential

from .platform_provider import PlatformAPI, PlatformAPIRegistry

# Fabric REST API Client (for remote operations)
# ============================================================================


try:
    from azure.storage.filedatalake import DataLakeServiceClient

    HAS_STORAGE_SDK = True
except ImportError:
    HAS_STORAGE_SDK = False


@PlatformAPIRegistry.register("fabric")
class FabricAPI(PlatformAPI):
    """
    Microsoft Fabric REST API client for remote operations.

    This class provides REST API access to Fabric for operations like:
    - Creating Spark job definitions
    - Uploading files to OneLake
    - Running and monitoring jobs

    Does NOT require mssparkutils/dbutils - pure REST API.
    Can be used from local dev, CI/CD, tests, etc.

    Authentication via Azure DefaultAzureCredential which supports:
    - Service Principal (env vars)
    - Azure CLI (az login)
    - Managed Identity
    - Interactive login

    Example:
        >>> from fabric_api import FabricAPI
        >>> api = FabricAPI("workspace-id", "lakehouse-id")
        >>> job = api.create_job("my-job", config)
        >>> run_id = api.run_job(job["job_id"])
        >>> status = api.get_job_status(run_id)
    """

    def __init__(
        self,
        workspace_id: str,
        lakehouse_id: Optional[str] = None,
        credential: Optional[DefaultAzureCredential] = None,
        storage_account: Optional[str] = None,
        container: Optional[str] = None,
        base_path: Optional[str] = None,
    ):
        """Initialize Fabric API client

        Args:
            workspace_id: Fabric workspace ID
            lakehouse_id: Optional Fabric lakehouse ID (required for job definitions that use lakehouse)
            credential: Optional Azure credential. If not provided, uses DefaultAzureCredential
            storage_account: Optional storage account for ABFSS uploads (required for file uploads)
            container: Optional container name for ABFSS uploads (required for file uploads)
            base_path: Optional base path within container (e.g., "packages" or "kindling")
        """
        self.workspace_id = workspace_id
        self.lakehouse_id = lakehouse_id
        self.credential = credential or DefaultAzureCredential()
        self.storage_account = storage_account
        self.container = container
        self.base_path = base_path
        self.base_url = "https://api.fabric.microsoft.com/v1"
        self._token = None
        self._token_expiry = 0
        self._storage_client = None
        """Initialize Fabric API client

        Args:
            workspace_id: Fabric workspace ID
            lakehouse_id: Optional Fabric lakehouse ID (required for job definitions that use lakehouse)
            credential: Optional Azure credential. If not provided, uses DefaultAzureCredential
            storage_account: Optional storage account for ABFSS uploads (required for file uploads)
            container: Optional container name for ABFSS uploads (required for file uploads)
            base_path: Optional base path within container (e.g., "packages" or "artifacts")
        """
        self.workspace_id = workspace_id
        self.lakehouse_id = lakehouse_id
        self.credential = credential or DefaultAzureCredential()
        self.storage_account = storage_account
        self.container = container
        self.base_path = base_path
        self.base_url = "https://api.fabric.microsoft.com/v1"
        self._token = None
        self._token_expiry = 0
        self._storage_client = None

    @classmethod
    def from_env(cls):
        """
        Create FabricAPI client from environment variables.

        Required environment variables:
            - FABRIC_WORKSPACE_ID
            - FABRIC_LAKEHOUSE_ID

        Optional environment variables:
            - AZURE_STORAGE_ACCOUNT (for ABFSS uploads)
            - AZURE_CONTAINER (default: "artifacts")
            - AZURE_BASE_PATH (default: "")

        Returns:
            FabricAPI client instance

        Raises:
            ValueError: If required environment variables are missing
        """
        workspace_id = os.getenv("FABRIC_WORKSPACE_ID")
        lakehouse_id = os.getenv("FABRIC_LAKEHOUSE_ID")

        if not workspace_id or not lakehouse_id:
            raise ValueError(
                "Missing required environment variables: FABRIC_WORKSPACE_ID and FABRIC_LAKEHOUSE_ID"
            )

        return cls(
            workspace_id=workspace_id,
            lakehouse_id=lakehouse_id,
            storage_account=os.getenv("AZURE_STORAGE_ACCOUNT"),
            container=os.getenv("AZURE_CONTAINER", "artifacts"),
            base_path=os.getenv("AZURE_BASE_PATH", ""),
        )

    def get_platform_name(self) -> str:
        """Get platform name"""
        return "fabric"

    def _resolve_key_vault_url(self, secret_config: Optional[Dict[str, Any]] = None) -> str:
        cfg = secret_config or {}
        return str(cfg.get("key_vault_url") or os.getenv("SYSTEM_TEST_KEY_VAULT_URL") or "").strip()

    def set_secret(
        self,
        secret_name: str,
        secret_value: str,
        secret_config: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Create/update a Key Vault secret for Fabric/Synapse-compatible secret providers."""
        key_vault_url = self._resolve_key_vault_url(secret_config)
        if not key_vault_url:
            raise ValueError(
                "Fabric secret provisioning requires key_vault_url. "
                "Set kindling.secrets.key_vault_url or SYSTEM_TEST_KEY_VAULT_URL."
            )

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
                "Fabric secret provisioning was forbidden by Key Vault. "
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
        """Delete Key Vault secret used by Fabric/Synapse-compatible secret providers."""
        key_vault_url = self._resolve_key_vault_url(secret_config)
        if not key_vault_url:
            return False

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
                "Fabric secret cleanup was forbidden by Key Vault. "
                "Grant this principal secret delete permissions. "
                f"vault={key_vault_url} secret={secret_name} details={response.text}"
            )
        response.raise_for_status()
        return True

    def deploy_app(self, app_name: str, app_files: Dict[str, str]) -> str:
        """Upload app files to data-apps/{app_name}/ in storage

        Args:
            app_name: Application name (used as subdirectory)
            app_files: Dictionary of {filename: content} to deploy

        Returns:
            Storage path where files were uploaded
        """
        target_path = f"data-apps/{app_name}"
        return self._upload_files(app_files, target_path)

    def cleanup_app(self, app_name: str) -> bool:
        """Delete app files from storage

        Removes the data-apps/{app_name}/ directory from ADLS Gen2 storage.

        Args:
            app_name: Application name to clean up

        Returns:
            True if cleanup succeeded, False otherwise
        """
        try:
            if not HAS_STORAGE_SDK:
                print("⚠️  azure-storage-file-datalake not installed. Cannot cleanup.")
                return False

            if not self.storage_account or not self.container:
                print("⚠️  Storage account/container not configured. Cannot cleanup.")
                return False

            # Initialize storage client if needed
            if not self._storage_client:
                account_url = f"https://{self.storage_account}.dfs.core.windows.net"
                self._storage_client = DataLakeServiceClient(
                    account_url=account_url, credential=self.credential
                )

            file_system_client = self._storage_client.get_file_system_client(
                file_system=self.container
            )

            # Construct full path
            if self.base_path:
                full_path = f"{self.base_path}/data-apps/{app_name}"
            else:
                full_path = f"data-apps/{app_name}"

            # Delete directory recursively
            dir_client = file_system_client.get_directory_client(full_path)
            dir_client.delete_directory()
            print(f"🗑️  Cleaned up app files at: {full_path}")
            return True
        except Exception as e:
            print(f"⚠️  Failed to cleanup app '{app_name}': {e}")
            return False

    def deploy_spark_job(
        self, app_files: Dict[str, str], job_config: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Deploy application as Spark job (convenience method)

        Combines deploy_app + create_job into one call.
        Delegates to the ABC default implementation via super().

        Args:
            app_files: Dictionary of {filename: content} to deploy
            job_config: Platform-specific job configuration (must include lakehouse_id)

        Returns:
            Dictionary with job_id, deployment_path, and metadata
        """
        return super().deploy_spark_job(app_files, job_config)

    def _get_token(self, scope: str = "fabric") -> str:
        """Get access token for Fabric or OneLake API

        Args:
            scope: "fabric" for Fabric API or "storage" for OneLake/ADLS

        Returns:
            Valid access token
        """
        # Different tokens for different APIs
        if scope == "storage":
            # OneLake uses Azure Storage scope
            token_obj = self.credential.get_token("https://storage.azure.com/.default")
            return token_obj.token
        else:
            # Fabric API uses Fabric-specific scope
            # Refresh token if expired
            if not self._token or time.time() >= self._token_expiry:
                token_obj = self.credential.get_token("https://api.fabric.microsoft.com/.default")
                self._token = token_obj.token
                # Set expiry 5 minutes before actual expiry
                self._token_expiry = token_obj.expires_on - 300

            return self._token

    def _get_headers(self) -> Dict[str, str]:
        """Get API request headers

        Returns:
            Headers dictionary with authorization
        """
        return {"Authorization": f"Bearer {self._get_token()}", "Content-Type": "application/json"}

    def _upload_file_to_lakehouse(self, file_path: str, content: str) -> None:
        """Upload a single file to lakehouse via OneLake API

        Uses the Create → Append → Flush pattern for OneLake uploads.

        Args:
            file_path: Path within lakehouse (e.g., "Files/data-apps/my-app/main.py")
            content: File content as string
        """
        # Build OneLake URL for lakehouse
        onelake_base = f"https://onelake.dfs.fabric.microsoft.com/{self.workspace_id}/{self.lakehouse_id}/{file_path}"

        # Get storage token
        storage_token = self._get_token(scope="storage")
        headers = {"Authorization": f"Bearer {storage_token}", "x-ms-version": "2023-11-03"}

        # Step 1: Create file
        create_url = f"{onelake_base}?resource=file"
        create_response = requests.put(create_url, headers=headers)
        if create_response.status_code not in [200, 201]:
            raise Exception(
                f"Failed to create file {file_path}: {create_response.status_code} - {create_response.text}"
            )

        # Step 2: Append content
        content_bytes = content.encode("utf-8")
        append_url = f"{onelake_base}?position=0&action=append"
        append_headers = headers.copy()
        append_headers["Content-Type"] = "text/plain"
        append_response = requests.patch(append_url, headers=append_headers, data=content_bytes)
        if append_response.status_code not in [200, 202]:
            raise Exception(
                f"Failed to append content to {file_path}: {append_response.status_code} - {append_response.text}"
            )

        # Step 3: Flush file
        flush_url = f"{onelake_base}?position={len(content_bytes)}&action=flush"
        flush_response = requests.patch(flush_url, headers=headers)
        if flush_response.status_code not in [200, 201]:
            raise Exception(
                f"Failed to flush file {file_path}: {flush_response.status_code} - {flush_response.text}"
            )

    def _make_request(self, method: str, url: str, **kwargs) -> requests.Response:
        """Make authenticated API request

        Args:
            method: HTTP method (GET, POST, etc.)
            url: Full URL to request
            **kwargs: Additional arguments to pass to requests

        Returns:
            Response object

        Raises:
            requests.HTTPError: If request fails
        """
        kwargs.setdefault("headers", self._get_headers())
        response = requests.request(method, url, **kwargs)

        # Enhanced error handling with response body
        if not response.ok:
            try:
                error_body = response.json()
                error_msg = f"{response.status_code} Error: {error_body}"
            except:
                error_msg = f"{response.status_code} Error: {response.text}"
            raise requests.HTTPError(error_msg, response=response)

        return response

    def create_job(self, job_name: str, job_config: Dict[str, Any]) -> Dict[str, Any]:
        """Create a Spark job definition

        Args:
            job_name: Name for the job
            job_config: Job configuration dict with:
                - app_name: Application name for Kindling bootstrap (optional)
                - artifacts_path: Path to artifacts in lakehouse (optional, defaults to 'Files/artifacts')
                - main_file: Entry point file (optional, defaults to 'Files/scripts/kindling_bootstrap.py')
                - command_line_args: Additional command line arguments (optional)
                - spark_config: Spark configuration (optional)
                - environment: Environment variables (optional)

        Returns:
            Dictionary with:
                - job_id: Created job ID
                - job_name: Job name
                - workspace_id: Workspace ID
        """
        url = f"{self.base_url}/workspaces/{self.workspace_id}/items"

        # For Fabric jobs with Kindling bootstrap:
        # - executableFile points to kindling_bootstrap.py in lakehouse Files/scripts/
        # - Bootstrap config is passed via command line arguments as JSON
        # - Lakehouse context provides access to Files/ and packages

        app_name = job_config.get("app_name", job_name)
        artifacts_path = job_config.get("artifacts_path", "Files/artifacts")

        # Build artifacts_storage_path based on upload mode
        # If lakehouse_id configured: use Files/{base_path}
        # If storage_account configured: use abfss://.../{base_path}
        if self.lakehouse_id:
            # Lakehouse mode: Files uploaded to Files/{base_path}/data-apps/
            if self.base_path:
                artifacts_storage_path = f"Files/{self.base_path.strip('/')}"
            else:
                artifacts_storage_path = "Files/artifacts"
        elif self.storage_account and self.container:
            # ABFSS mode: Files uploaded to abfss://.../{base_path}/data-apps/
            base_url = f"abfss://{self.container}@{self.storage_account}.dfs.core.windows.net"
            if self.base_path:
                artifacts_storage_path = f"{base_url}/{self.base_path.strip('/')}"
            else:
                artifacts_storage_path = base_url
        else:
            # Fallback to relative path
            artifacts_storage_path = "Files/artifacts"

        # Build bootstrap config that will be passed to kindling_bootstrap.py
        # Using convention: config:key=value sets BOOTSTRAP_CONFIG[key] = value
        bootstrap_params = {
            "app_name": app_name,
            # Base path for artifacts - data-apps and config are under artifacts_storage_path/
            # Bootstrap will look for apps in {artifacts_storage_path}/data-apps/{app_name}/
            # and config in {artifacts_storage_path}/config/
            "artifacts_storage_path": artifacts_storage_path,
            "platform": "fabric",  # Explicitly set platform to avoid detection issues
            "use_lake_packages": "True",
            "load_local_packages": "False",
            # Set Spark app name for test ID extraction
            "spark_app_name": f"kindling-app-{app_name}",
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

        # Build command line args using config:key=value convention.
        # Quote each arg so values with spaces (e.g. "@secret my-secret") survive transport.
        config_args = " ".join(
            [shlex.quote(f"config:{k}={v}") for k, v in bootstrap_params.items()]
        )

        # Combine with any additional command line args
        additional_args = job_config.get("command_line_args", "")
        command_line_args = f"{config_args} {additional_args}".strip()

        # Map our config to Fabric Spark Job Definition format
        # The definition payload must be base64 encoded
        # Note: executableFile must be an absolute ABFSS path in Fabric
        main_file = job_config.get("main_file", "Files/scripts/kindling_bootstrap.py")

        # Validate main_file
        if not main_file or not main_file.strip():
            raise ValueError("main_file cannot be empty")

        # Convert to full ABFSS path if not already absolute
        if not main_file.startswith("abfss://"):
            # Use storage account and container from environment or instance
            storage_account = self.storage_account or os.getenv("AZURE_STORAGE_ACCOUNT")
            container = self.container or os.getenv("AZURE_CONTAINER", "artifacts")

            # Convert Files/scripts/kindling_bootstrap.py to scripts/kindling_bootstrap.py
            if main_file.startswith("Files/"):
                main_file = main_file[6:]  # Remove "Files/" prefix

            # Construct full ABFSS path
            main_file = f"abfss://{container}@{storage_account}.dfs.core.windows.net/{main_file}"

        # Check for environment_id in job_config
        environment_id = job_config.get("environment_id")

        # NOTE: Fabric does not support sparkConf in the job definition like Synapse does.
        # Spark configuration must be set via:
        # 1. Fabric Environment (environmentArtifactId)
        # 2. Runtime parameters (not yet fully supported by API)
        # 3. SparkConf in the application code itself
        #
        # For diagnostic emitters, we'll need to set them programmatically in the bootstrap code
        # or create a Fabric Environment with the required Spark configs.

        definition_json = {
            "executableFile": main_file,
            "defaultLakehouseArtifactId": self.lakehouse_id,
            "mainClass": "",
            "additionalLakehouseIds": [],
            "retryPolicy": None,
            "commandLineArguments": command_line_args,
            "additionalLibraryUris": [],
            "language": "python",
            "environmentArtifactId": environment_id,  # Use environment if provided
        }

        # Base64 encode the definition
        definition_bytes = json.dumps(definition_json).encode("utf-8")
        definition_base64 = base64.b64encode(definition_bytes).decode("utf-8")

        payload = {
            "displayName": job_name,
            "type": "SparkJobDefinition",
            "definition": {
                "format": "SparkJobDefinitionV1",
                "parts": [
                    {
                        "path": "SparkJobDefinitionV1.json",
                        "payload": definition_base64,
                        "payloadType": "InlineBase64",
                    }
                ],
            },
        }

        response = self._make_request("POST", url, json=payload)
        result = response.json()

        return {
            "job_id": result["id"],
            "job_name": result["displayName"],
            "workspace_id": self.workspace_id,
        }

    def _upload_files(self, files: Dict[str, str], target_path: str) -> str:
        """Upload files to ABFSS storage (lakehouse shortcut provides runtime access)

        ALL artifacts are stored in ABFSS (ADLS Gen2):
            Files uploaded to ABFSS container via Azure Storage SDK
            Runtime access via lakehouse shortcut Files/artifacts -> abfss://container/
            Returns relative path (accessible via shortcut at runtime)

        Args:
            files: Dictionary of {filename: content}
            target_path: Relative path (e.g., "data-apps/my-app")

        Returns:
            Storage path reference (relative path for runtime via shortcut)

        Note:
            Lakehouse shortcut Files/artifacts points to ABFSS container root.
            This allows runtime to access abfss://container/data-apps/app via Files/artifacts/data-apps/app
        """
        # ALL files go to ABFSS storage (not lakehouse OneLake API)
        if not HAS_STORAGE_SDK:
            print("⚠️  azure-storage-file-datalake not installed. Files not uploaded.")
            print("   Install with: pip install azure-storage-file-datalake")
            print("   Bundling files for later reference instead.")
            # Fallback to bundling
            if not hasattr(self, "_pending_files"):
                self._pending_files = {}
            self._pending_files.update(files)
            return target_path

        if not self.storage_account or not self.container:
            print("⚠️  Storage account/container not configured. Files not uploaded.")
            print(f"   Files prepared: {list(files.keys())}")
            print("   Configure storage_account and container in FabricAPI.__init__()")
            # Fallback to bundling
            if not hasattr(self, "_pending_files"):
                self._pending_files = {}
            self._pending_files.update(files)
            return target_path

        print(f"📤 Uploading {len(files)} files to ABFSS storage...")

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
                    content.encode("utf-8") if isinstance(content, str) else content, overwrite=True
                )
                uploaded_count += 1
            except Exception as e:
                print(f"⚠️  Failed to upload {filename}: {e}")

        # Construct ABFSS path
        abfss_path = f"abfss://{self.container}@{self.storage_account}.dfs.core.windows.net/{full_target_path}"

        print(f"📂 Uploaded {uploaded_count}/{len(files)} files to: {abfss_path}")

        return abfss_path

    def _update_job_files(self, job_id: str, files_path: str) -> None:
        """Update job definition with file path reference (internal)

        For Fabric, this is a no-op as files are referenced via command line args
        to the bootstrap script. The executableFile is set during create_job
        and points to kindling_bootstrap.py, which then loads the app based on
        the app_name parameter passed in command line arguments.

        Args:
            job_id: Job ID to update
            files_path: Path where files were uploaded (from _upload_files)
        """
        print(f"✅ Job {job_id} configured with file path: {files_path}")

    def run_job(self, job_id: str, parameters: Optional[Dict[str, Any]] = None) -> str:
        """Execute a Spark job

        Args:
            job_id: Job ID to run
            parameters: Optional runtime parameters

        Returns:
            Run ID for monitoring
        """
        url = f"{self.base_url}/workspaces/{self.workspace_id}/items/{job_id}/jobs/instances?jobType=sparkjob"

        payload = {}
        if parameters:
            payload["executionData"] = {"parameters": parameters}

        response = self._make_request("POST", url, json=payload)

        # 202 Accepted with empty body is typical for async operations
        # Run ID is in Location header
        if response.status_code == 202:
            location = response.headers.get("Location", "")
            if location:
                # Extract run ID from Location header
                # Format: https://api.fabric.microsoft.com/v1/workspaces/{workspace}/items/{item}/jobs/instances/{run_id}
                run_id = location.split("/")[-1].split("?")[0]
                # Store job_id -> run_id mapping for status queries
                if not hasattr(self, "_run_to_job_map"):
                    self._run_to_job_map = {}
                self._run_to_job_map[run_id] = job_id
                return run_id
            else:
                # If no Location header, return job_id as fallback
                # This allows tests to continue even if API structure is different
                return job_id

        # If 200/201 with body, parse JSON
        result = response.json()
        run_id = result.get("id", job_id)
        # Store mapping
        if not hasattr(self, "_run_to_job_map"):
            self._run_to_job_map = {}
        self._run_to_job_map[run_id] = job_id
        return run_id

    def get_job_status(self, run_id: str) -> Dict[str, Any]:
        """Get job execution status

        Args:
            run_id: Run ID to check

        Returns:
            Status dictionary with keys:
                - status: Current status (RUNNING, SUCCEEDED, FAILED, etc.)
                - start_time: When job started
                - end_time: When job finished (if complete)
                - error: Error message (if failed)
        """
        # Get job_id from mapping
        job_id = getattr(self, "_run_to_job_map", {}).get(run_id, run_id)

        # Correct endpoint format with job_id
        url = (
            f"{self.base_url}/workspaces/{self.workspace_id}/items/{job_id}/jobs/instances/{run_id}"
        )

        response = self._make_request("GET", url)
        result = response.json()

        # Store rootActivityId for log retrieval
        # Fabric uses this ID in the log folder path: logs/{workspace_id}.{rootActivityId}
        root_activity_id = result.get("rootActivityId")

        # DEBUG: Trace job status response for troubleshooting
        print(f"🔍 DEBUG: Job Status Response for run_id={run_id}")
        print(f"   Full response keys: {list(result.keys())}")
        print(f"   rootActivityId: {root_activity_id}")
        print(f"   status: {result.get('status')}")
        print(f"   id: {result.get('id')}")
        print(f"   jobType: {result.get('jobType')}")
        if root_activity_id:
            print(
                f"   Expected log path: logs/{self.workspace_id}.{root_activity_id}/driver/spark-logs"
            )
        else:
            print(f"   ⚠️  NO rootActivityId in response - will use discovery fallback")

        if root_activity_id and not hasattr(self, "_run_to_activity_map"):
            self._run_to_activity_map = {}
        if root_activity_id:
            self._run_to_activity_map[run_id] = root_activity_id

        return {
            "status": result.get("status", "UNKNOWN"),
            "start_time": result.get("startTimeUtc"),
            "end_time": result.get("endTimeUtc"),
            "error": result.get("failureReason"),
            "logs": None,  # Logs require separate API call - use get_job_logs()
            "activity_id": root_activity_id,  # For internal use
        }

    def get_job_logs(self, run_id: str, from_line: int = 0, size: int = 1000) -> Dict[str, Any]:
        """Get job execution logs

        Args:
            run_id: Run ID to get logs for
            from_line: Starting line offset (default: 0)
            size: Maximum number of log lines to return (default: 1000)

        Returns:
            Dictionary with:
                - logs: List of log lines (or single string if not in Livy format)
                - from: Starting offset
                - size: Number of lines returned
                - total: Total lines available (if known)
                - source: Which endpoint provided the logs
        """
        # Get job_id from mapping
        job_id = getattr(self, "_run_to_job_map", {}).get(run_id, run_id)

        # PRIORITY 1: Try to read diagnostic logs from ABFSS storage (primary method)
        # Spark diagnostic emitters write to ABFSS at logs/<workspace_id>.<activity_id>/driver/spark-logs
        # We get the activity_id from get_job_status() which stores it in _run_to_activity_map
        # Runtime accesses this via lakehouse shortcut Files/artifacts -> abfss://container/
        # API should read directly from ABFSS, not via OneLake (OneLake is for runtime only)
        if self.storage_account and self.container:
            try:
                # Get activity_id from mapping (populated by get_job_status)
                activity_id = getattr(self, "_run_to_activity_map", {}).get(run_id)

                # DEBUG: Trace log retrieval attempt
                print(f"🔍 DEBUG: Log Retrieval for run_id={run_id}")
                print(f"   storage_account: {self.storage_account}")
                print(f"   container: {self.container}")
                print(f"   workspace_id: {self.workspace_id}")
                print(f"   activity_id from mapping: {activity_id}")
                print(
                    f"   _run_to_activity_map contents: {getattr(self, '_run_to_activity_map', {})}"
                )

                # Try run_id first (this is what Fabric actually uses)
                log_path = f"logs/{self.workspace_id}.{run_id}/driver/spark-logs"
                print(f"📂 Reading logs from: {log_path}")
                log_lines = self._read_diagnostic_logs_from_adls(log_path)

                if not log_lines and activity_id:
                    # Fallback: try activity_id (in case it changes)
                    log_path = f"logs/{self.workspace_id}.{activity_id}/driver/spark-logs"
                    print(f"📂 Fallback: trying activity_id path: {log_path}")
                    log_lines = self._read_diagnostic_logs_from_adls(log_path)

                if not log_lines:
                    # Last resort: discover by finding most recent folder for this workspace
                    print(f"⚠️  Direct paths failed, using discovery fallback")
                    log_lines = self._discover_and_read_diagnostic_logs(run_id)

                if log_lines:
                    # Apply from_line and size filtering
                    if from_line > 0 or size < len(log_lines):
                        filtered_logs = log_lines[from_line : from_line + size]
                    else:
                        filtered_logs = log_lines

                    return {
                        "id": run_id,
                        "from": from_line,
                        "total": len(log_lines),
                        "log": filtered_logs,
                        "source": "abfss_diagnostic_emitters",
                    }
            except Exception as e:
                # Don't print stack trace - logs might just not be ready yet
                pass  # Fall through to Fabric API

        # PRIORITY 2: Try Fabric job instances log endpoint (undocumented fallback)
        try:
            url = f"{self.base_url}/workspaces/{self.workspace_id}/items/{job_id}/jobs/instances/{run_id}/logs"
            response = self._make_request("GET", url)

            # Response might be JSON with log content or direct text
            if response.headers.get("content-type", "").startswith("application/json"):
                result = response.json()
                log_content = result.get("content", result.get("logs", str(result)))
            else:
                log_content = response.text

            # Convert to list of lines if it's a string
            if isinstance(log_content, str):
                log_lines = log_content.split("\n")
            else:
                log_lines = log_content if isinstance(log_content, list) else [str(log_content)]

            return {
                "logs": log_lines,
                "from": 0,
                "size": len(log_lines),
                "total": len(log_lines),
                "source": "fabric_instances_api",
            }

        except requests.HTTPError as e:
            # Log the error for debugging
            error_status = e.response.status_code if hasattr(e, "response") else "unknown"
            pass  # Try next method

        # No logs available via any method
        return {
            "logs": [
                "❌ Logs not available via REST API",
                "",
                "📍 Access logs via Fabric Portal:",
                "   1. Go to your Fabric workspace",
                "   2. Open Monitoring Hub",
                "   3. Find your job run",
                "   4. View logs in the UI",
                "",
                f"   Run ID: {run_id}",
                f"   Job ID: {job_id}",
                "",
                "💡 Alternative: Configure diagnostic emitters in job config with storage_access_key",
            ],
            "from": 0,
            "size": 1,
            "total": 1,
            "source": "unavailable",
        }

    def _discover_and_read_diagnostic_logs(self, run_id: str) -> List[str]:
        """Discover log folder and read diagnostic logs from ADLS Gen2 storage

        Fabric creates logs with pattern: logs/<some_uuid>.<run_id>/driver/spark-logs
        The first UUID is NOT the workspace_id - it appears to be randomly generated.
        We must discover it by listing the logs directory.

        Args:
            run_id: Run ID to find logs for

        Returns:
            List of log message strings
        """
        if not HAS_STORAGE_SDK:
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

        try:
            # List all folders in the logs directory for this workspace
            # Fabric uses pattern: logs/{workspace_id}.{activity_id}
            # The activity_id is NOT the same as run_id - we must find the most recent folder
            paths = file_system_client.get_paths(path="logs")

            matching_folders = []
            for path_item in paths:
                if path_item.is_directory:
                    folder_name = path_item.name.replace("logs/", "")
                    # Check if folder starts with workspace_id
                    if folder_name.startswith(f"{self.workspace_id}."):
                        # Store with last_modified time
                        matching_folders.append((folder_name, path_item.last_modified))

            if not matching_folders:
                print(f"⚠️  No log folders found for workspace {self.workspace_id}")
                return []

            # Sort by last_modified descending and take the most recent
            matching_folders.sort(key=lambda x: x[1], reverse=True)
            matching_folder = matching_folders[0][0]
            print(f"✅ Found most recent log folder: {matching_folder}")

            # Now read from the discovered folder
            log_path = f"logs/{matching_folder}/driver/spark-logs"
            print(f"📂 Reading logs from: {log_path}")

            return self._read_diagnostic_logs_from_adls(log_path)

        except Exception as e:
            print(f"⚠️  Failed to discover log folder: {e}")
            return []

    def _read_diagnostic_logs_from_adls(self, log_path: str) -> List[str]:
        """Read Spark diagnostic logs directly from ADLS Gen2 storage

        Fabric writes diagnostic logs to external ADLS storage which is accessed
        via a lakehouse shortcut (Files/artifacts) at runtime.

        Args:
            log_path: Relative path to log file (e.g., "logs/<workspace>.<run_id>/driver/spark-logs")

        Returns:
            List of log message strings extracted from JSON log entries
        """
        if not HAS_STORAGE_SDK:
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

        log_lines = []

        try:
            # Read the log file directly
            file_client = file_system_client.get_file_client(log_path)
            download = file_client.download_file()
            content = download.readall()

            # Parse JSON log entries (diagnostic emitter format)
            for line in content.decode("utf-8").splitlines():
                if not line.strip():
                    continue

                try:
                    log_entry = json.loads(line)
                    # Extract message from properties
                    if "properties" in log_entry:
                        message = log_entry["properties"].get("message", "")
                        if message:
                            log_lines.append(message)
                    elif "message" in log_entry:
                        log_lines.append(log_entry["message"])
                    else:
                        # Unknown format, keep raw line
                        log_lines.append(line)
                except json.JSONDecodeError:
                    # Not JSON, keep as-is
                    log_lines.append(line)

            if log_lines:
                print(f"✅ Found {len(log_lines)} log lines from diagnostic emitters")

            return log_lines

        except Exception as e:
            print(f"⚠️  Failed to read diagnostic logs from {log_path}: {e}")
            return []

    def _read_event_logs_from_storage(self, app_id: str, run_id: str, job_id: str) -> List[str]:
        """Read Spark diagnostic logs from Azure Storage (diagnostic emitters)

        Fabric uses the same Spark runtime as Synapse and supports diagnostic emitters.
        Diagnostic logs are written to: logs/<workspaceName>.<jobName>.<runId>/driver/spark-logs

        Format: JSON lines with categories: Log, EventLog, Metrics

        Reference: https://learn.microsoft.com/en-us/azure/synapse-analytics/spark/azure-synapse-diagnostic-emitters-azure-storage

        Args:
            app_id: Spark application ID (e.g., application_1234567890_0001)
            run_id: Run/session ID
            job_id: Fabric job definition ID

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

        # Fabric diagnostic logs path pattern: logs/<some_uuid>.<run_id>/driver/spark-logs
        # The first UUID appears to be randomly generated or an internal Fabric identifier
        # We need to discover it by listing the logs directory

        log_lines = []

        try:
            # List all folders in the logs directory to find one ending with our run_id
            paths = file_system_client.get_paths(path="logs")

            matching_folder = None
            for path_item in paths:
                if path_item.is_directory:
                    folder_name = path_item.name.replace("logs/", "")
                    # Check if folder ends with .<run_id>
                    if folder_name.endswith(f".{run_id}"):
                        matching_folder = folder_name
                        print(f"✅ Found log folder: {folder_name}")
                        break

            if not matching_folder:
                print(f"⚠️  No log folder found ending with .{run_id}")
                return []

            # Now try to read from the discovered folder
            log_path = f"logs/{matching_folder}/driver/spark-logs"
            print(f"📂 Reading logs from: {log_path}")

        except Exception as e:
            print(f"⚠️  Failed to list logs directory: {e}")
            # Fallback to trying known patterns
            possible_paths = [
                f"logs/{self.workspace_id}.{run_id}/driver/spark-logs",
                f"logs/{self.workspace_id}.{job_id}.{run_id}/driver/spark-logs",
            ]
            log_path = possible_paths[0]  # Try first pattern

        # Try to read the log file
        try:
            # Read the driver spark-logs file
            file_client = file_system_client.get_file_client(log_path)
            download = file_client.download_file()
            content = download.readall()

            # Logs may be compressed
            if log_path.endswith(".gz"):
                content = gzip.decompress(content)

            # Parse diagnostic log entries (JSON lines format)
            for line in content.decode("utf-8").splitlines():
                if line.strip():
                    try:
                        entry = json.loads(line)

                        # Filter to only this application (if app_id is provided)
                        if app_id and entry.get("applicationId") != app_id:
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

                            # DEBUG: Check message structure
                            if isinstance(message, list):
                                # Message is an array of log lines
                                for msg in message:
                                    if isinstance(msg, str) and msg.strip():
                                        log_line = (
                                            f"[{timestamp}] {level} {logger_name}: {msg.strip()}"
                                        )
                                        log_lines.append(log_line)
                            elif isinstance(message, str):
                                # Message field may contain multiple log lines separated by newlines
                                messages = message.strip().split("\n") if message else [""]
                                for msg in messages:
                                    msg = msg.strip()
                                    if msg:  # Only include non-empty messages
                                        log_line = f"[{timestamp}] {level} {logger_name}: {msg}"
                                        log_lines.append(log_line)
                            else:
                                # Handle other types
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

            # If we found logs, return them
            if log_lines:
                print(f"✅ Found {len(log_lines)} log lines from diagnostic emitters at {log_path}")
                return log_lines
            else:
                print(f"⚠️  No log entries found at {log_path}")
                return []

        except Exception as e:
            print(f"⚠️  Failed to read diagnostic logs from {log_path}: {e}")
            return []

    def get_livy_session_info(
        self, job_id: str, run_id: str, max_wait: int = 30
    ) -> Optional[Dict[str, Any]]:
        """Get Livy session information for a running Spark job

        Based on Microsoft Fabric REST API documentation:
        https://learn.microsoft.com/en-us/rest/api/fabric/spark/item-job-instances/list-spark-job-definition-instances

        Args:
            job_id: Spark job definition ID
            run_id: Job run/instance ID
            max_wait: Maximum seconds to wait for session to appear (default: 30)

        Returns:
            Dictionary with livy_id, app_id, state if available, None otherwise
        """
        try:
            # First check if the job is running
            status_info = self.get_job_status(run_id)
            if status_info.get("status", "").upper() not in ["INPROGRESS", "RUNNING"]:
                print(
                    f"⚠️  Job not running, cannot get session info (status: {status_info.get('status')})"
                )
                sys.stdout.flush()
                return None

            # Try to get Livy sessions via official Fabric API
            # Response structure per Microsoft docs: { "value": [ {...session objects...} ] }
            url = f"{self.base_url}/workspaces/{self.workspace_id}/sparkJobDefinitions/{job_id}/livySessions"

            start_time = time.time()
            attempt = 0

            while time.time() - start_time < max_wait:
                attempt += 1

                try:
                    response = self._make_request("GET", url)

                    if response.status_code == 200:
                        data = response.json()
                        # Per Microsoft docs: sessions are in 'value' array, not 'sessions'
                        sessions = data.get("value", [])

                        if sessions:
                            # Get the most recent session (last in array)
                            session = sessions[-1]

                            # Per actual API response: field is 'livyId' not 'id'
                            livy_id = session.get("livyId")
                            state = session.get("state")

                            if livy_id:
                                # Now get the specific session details to get application ID
                                # Per Microsoft docs: GET /livySessions/{livyId}
                                session_detail_url = f"{url}/{livy_id}"

                                try:
                                    detail_response = self._make_request("GET", session_detail_url)
                                    if detail_response.status_code == 200:
                                        detail_data = detail_response.json()

                                        # Per actual API: field is 'sparkApplicationId' at top level
                                        app_id = detail_data.get("sparkApplicationId")

                                        # Fallback: check other possible locations
                                        if not app_id:
                                            app_id = detail_data.get(
                                                "applicationId"
                                            ) or detail_data.get("appId")
                                            spark_app = detail_data.get("sparkApplication", {})
                                            if not app_id and spark_app:
                                                app_id = spark_app.get("id") or spark_app.get(
                                                    "applicationId"
                                                )

                                        if app_id:
                                            return {
                                                "livy_id": livy_id,
                                                "app_id": app_id,
                                                "state": state,
                                                "job_id": job_id,
                                                "run_id": run_id,
                                            }
                                except requests.HTTPError:
                                    pass
                        else:
                            print(f"⏳ No sessions yet, waiting... (attempt {attempt})")
                            sys.stdout.flush()

                except requests.HTTPError as e:
                    print(f"⚠️  HTTP {e.response.status_code}: {e.response.text[:200]}")
                    sys.stdout.flush()
                    if e.response.status_code != 404:
                        # Non-404 errors are unexpected, stop retrying
                        break

                # Wait before retry
                if time.time() - start_time < max_wait:
                    time.sleep(5)

            print(f"⚠️  No Livy session found after {max_wait}s")
            sys.stdout.flush()
            return None

        except Exception as e:
            print(f"⚠️  Failed to get Livy session info: {e}")
            import traceback

            traceback.print_exc()
            sys.stdout.flush()
            return None

    def stream_stdout_logs(
        self,
        job_id: str,
        run_id: str,
        callback: Optional[callable] = None,
        poll_interval: float = 5.0,
        max_wait: float = 300.0,
    ) -> List[str]:
        """Stream stdout logs from a running Spark job in real-time

        This method polls the Fabric Spark REST API for stdout logs and streams
        new content as it becomes available. It tracks the last read position
        to only display new content (append-only behavior).

        Args:
            job_id: Spark job definition ID
            run_id: Job run/instance ID
            callback: Optional callback function called with each new line: callback(line: str)
            poll_interval: Seconds between polls (default: 5.0)
            max_wait: Maximum seconds to wait for job to start (default: 300)

        Returns:
            Complete list of all log lines retrieved

        Example:
            >>> api = FabricAPI(workspace_id, lakehouse_id)
            >>> def print_log(line):
            ...     print(f"[STDOUT] {line}")
            >>> logs = api.stream_stdout_logs(job_id, run_id, callback=print_log)
        """
        all_logs = []
        last_byte_offset = 0  # Track byte position in file, not line count
        start_time = time.time()
        session_info = None
        job_started_time = None  # Track when job actually started running
        stdout_warmup_seconds = 45  # Wait this long after job starts before expecting stdout
        last_poll_message_time = 0  # Track last time we printed a polling message

        # Total timeout for entire streaming operation (not just job start)
        total_timeout = max_wait + 300.0  # max_wait for start + 5 minutes for execution

        print(f"📡 Starting stdout log stream for run_id={run_id}")
        print(f"   Poll interval: {poll_interval}s")
        print(f"   Max wait for job start: {max_wait}s")
        print(f"   Stdout warmup time: {stdout_warmup_seconds}s")
        print(f"   Total timeout: {total_timeout}s")
        sys.stdout.flush()

        while True:
            # Check if we've exceeded total timeout
            elapsed = time.time() - start_time
            if elapsed > total_timeout:
                print(f"⏱️  Total timeout ({total_timeout}s) exceeded")
                sys.stdout.flush()
                break

            # Get current job status
            try:
                status_info = self.get_job_status(run_id)
                status = status_info.get("status", "UNKNOWN").upper()

                # If job completed, do final read and exit
                if status in ["COMPLETED", "SUCCEEDED", "FAILED", "CANCELLED"]:
                    print(f"✅ Job {status.lower()} - doing final log read")
                    sys.stdout.flush()
                    # Read any remaining logs
                    new_logs, last_byte_offset = self._read_stdout_chunk(
                        job_id, run_id, session_info, last_byte_offset
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
                        if remaining > 1:  # Only print if meaningful time remains
                            print(f"⏳ Warmup period: {int(remaining)}s remaining...")
                            sys.stdout.flush()
                        time.sleep(poll_interval)
                        continue

                    # Warmup complete - try to get session info if we don't have it
                    if not session_info:
                        session_info = self.get_livy_session_info(job_id, run_id)

                # Read new log content (works with or without session info now)
                new_logs, last_byte_offset = self._read_stdout_chunk(
                    job_id, run_id, session_info, last_byte_offset
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
                    if current_time - last_poll_message_time >= 30:  # Every 30 seconds
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
        return all_logs

    def _read_stdout_chunk(
        self, job_id: str, run_id: str, session_info: Optional[Dict[str, Any]], byte_offset: int = 0
    ) -> tuple[List[str], int]:
        """Read a chunk of stdout logs starting from a specific byte offset

        Uses Microsoft's official Fabric Spark monitoring API with proper parameters:
        - isDownload=true: Download log file as stream
        - isPartial=true: Download partial content
        - offset: Starting byte position (only valid while app is running)
        - size: Number of bytes to read (default 1MB per docs)

        Args:
            job_id: Spark job definition ID
            run_id: Job run/instance ID
            session_info: Optional session info with livy_id and app_id
            byte_offset: Byte position to start reading from (0-based)

        Returns:
            Tuple of (new log lines, new byte offset for next read)
        """
        chunk_size = 1024 * 1024  # 1MB chunks as per Microsoft docs default

        # APPROACH 1: Use session info if available (official driver log API)
        # https://learn.microsoft.com/en-us/fabric/data-engineering/driver-log
        if session_info:
            livy_id = session_info.get("livy_id")
            app_id = session_info.get("app_id")

            if livy_id and app_id:
                try:
                    # Official API endpoint with proper streaming parameters
                    url = (
                        f"{self.base_url}/workspaces/{self.workspace_id}"
                        f"/sparkJobDefinitions/{job_id}"
                        f"/livySessions/{livy_id}"
                        f"/applications/{app_id}"
                        f"/logs?type=driver&fileName=stdout"
                        f"&isDownload=true&isPartial=true"
                        f"&offset={byte_offset}&size={chunk_size}"
                    )

                    print(f"🔍 DEBUG: Attempting driver log API")
                    print(f"   URL: {url}")
                    sys.stdout.flush()

                    response = self._make_request("GET", url)

                    print(f"🔍 DEBUG: Driver log API response")
                    print(f"   Status: {response.status_code}")
                    print(f"   Content-Type: {response.headers.get('content-type', 'N/A')}")
                    print(f"   Content length: {len(response.text)} bytes")
                    print(f"   Has content: {bool(response.text)}")
                    sys.stdout.flush()

                    # Response should be raw text content (not JSON) when using isDownload=true
                    if response.status_code == 200 and response.text:
                        content = response.text
                        lines = content.split("\n") if content else []

                        # Calculate new byte offset
                        bytes_read = len(content.encode("utf-8"))
                        new_offset = byte_offset + bytes_read

                        if lines:
                            print(f"✅ Read {len(lines)} lines ({bytes_read} bytes) from stdout")
                            sys.stdout.flush()
                            return lines, new_offset

                except requests.HTTPError as e:
                    # 404 is expected early on - stdout doesn't exist yet
                    if e.response.status_code != 404:
                        print(f"⚠️  Driver log API error: {e.response.status_code}")
                        sys.stdout.flush()
                except Exception as e:
                    print(f"⚠️  Driver log API failed: {e}")
                    sys.stdout.flush()

        # APPROACH 2: Try to get rolling driver logs metadata first, then content
        # This can help us understand what log files are available
        if session_info:
            livy_id = session_info.get("livy_id")
            app_id = session_info.get("app_id")

            if livy_id and app_id:
                try:
                    # Get metadata about stdout file
                    meta_url = (
                        f"{self.base_url}/workspaces/{self.workspace_id}"
                        f"/sparkJobDefinitions/{job_id}"
                        f"/livySessions/{livy_id}"
                        f"/applications/{app_id}"
                        f"/logs?type=driver&meta=true&fileName=stdout"
                    )

                    meta_response = self._make_request("GET", meta_url)

                    if meta_response.status_code == 200:
                        meta_data = meta_response.json()
                        container_log_meta = meta_data.get("containerLogMeta", {})
                        file_length = container_log_meta.get("length", 0)

                        # If file exists and has content beyond our current offset
                        if file_length > byte_offset:
                            # Now get the actual content
                            content_url = (
                                f"{self.base_url}/workspaces/{self.workspace_id}"
                                f"/sparkJobDefinitions/{job_id}"
                                f"/livySessions/{livy_id}"
                                f"/applications/{app_id}"
                                f"/logs?type=driver&fileName=stdout"
                                f"&isDownload=true&isPartial=true"
                                f"&offset={byte_offset}&size={chunk_size}"
                            )

                            content_response = self._make_request("GET", content_url)

                            if content_response.status_code == 200 and content_response.text:
                                content = content_response.text
                                lines = content.split("\n") if content else []
                                bytes_read = len(content.encode("utf-8"))
                                new_offset = byte_offset + bytes_read

                                if lines:
                                    print(
                                        f"✅ Read {len(lines)} lines ({bytes_read} bytes) via metadata approach"
                                    )
                                    sys.stdout.flush()
                                    return lines, new_offset

                except requests.HTTPError as e:
                    if e.response.status_code != 404:
                        print(f"⚠️  Metadata approach error: {e.response.status_code}")
                        sys.stdout.flush()
                except Exception as e:
                    print(f"⚠️  Metadata approach failed: {e}")
                    sys.stdout.flush()

        # No new logs available from any source
        return [], byte_offset

    def cancel_job(self, run_id: str) -> bool:
        """Cancel a running job

        Args:
            run_id: Run ID to cancel

        Returns:
            True if cancelled successfully
        """
        # Get job_id from mapping
        job_id = getattr(self, "_run_to_job_map", {}).get(run_id, run_id)

        url = f"{self.base_url}/workspaces/{self.workspace_id}/items/{job_id}/jobs/instances/{run_id}/cancel"

        try:
            self._make_request("POST", url)
            return True
        except requests.HTTPError:
            return False

    def delete_job(self, job_id: str) -> bool:
        """Delete a job definition

        Args:
            job_id: Job ID to delete

        Returns:
            True if deleted successfully
        """
        url = f"{self.base_url}/workspaces/{self.workspace_id}/items/{job_id}"

        try:
            self._make_request("DELETE", url)
            return True
        except requests.HTTPError:
            return False

    def list_spark_jobs(self) -> list:
        """List all Spark job definitions in the workspace

        Returns:
            List of job definition dictionaries with id, displayName, type, etc.
        """
        url = f"{self.base_url}/workspaces/{self.workspace_id}/items?type=SparkJobDefinition"

        response = self._make_request("GET", url)
        return response.json().get("value", [])

    def list_jobs(self) -> list:
        """Alias for list_spark_jobs() for compatibility

        Returns:
            List of job definition dictionaries with id, displayName, type, etc.
        """
        return self.list_spark_jobs()


# Expose in module
__all__ = ["FabricAPI"]
