import json
import re
import subprocess
import sys
import time
import types
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Union
from urllib.parse import quote

import requests
from kindling.notebook_framework import *

from .platform_provider import PlatformAPI, PlatformAPIRegistry


class DatabricksService(PlatformService):
    def __init__(self, config, logger):
        self.config = config
        self.logger = logger
        self._base_url = self._build_base_url()

        # Cache system from original code
        self._token_cache = {}
        self._items_cache = []
        self._folders_cache = {}
        self._notebooks_cache = []
        self.credential = self

        self._initialize_cache()

    def _initialize_cache(self):
        """Initialize the cache with all workspace items and folders"""
        try:
            self.logger.debug("Initializing workspace cache...")

            # Get all items in the workspace using the workspace list API
            url = f"{self._base_url}/api/2.0/workspace/list"
            params = {"path": "/", "fmt": "SOURCE"}
            self.logger.debug(f"Fetching items from: {url}")
            response = requests.get(url, headers=self._get_headers(), params=params)

            if response.status_code == 200:
                data = self._handle_response(response)

                if isinstance(data, dict):
                    self._items_cache = data.get("objects", [])
                elif isinstance(data, list):
                    self._items_cache = data
                else:
                    self._items_cache = []

                # Debug: Print all item types found
                item_types = {}
                for item in self._items_cache:
                    item_type = item.get("object_type", "Unknown")
                    if item_type not in item_types:
                        item_types[item_type] = 0
                    item_types[item_type] += 1

                self.logger.debug(f"Item types found: {item_types}")

                # Separate items by type
                self._notebooks_cache = []
                self._folders_cache = {}

                for item in self._items_cache:
                    item_type = item.get("object_type", "").upper()

                    if item_type == "NOTEBOOK":
                        self._notebooks_cache.append(item)
                    elif item_type == "DIRECTORY":
                        folder_path = item.get("path")
                        folder_name = folder_path.split("/")[-1] if folder_path else ""
                        self.logger.debug(
                            f"Found folder - Path: {folder_path}, Name: {folder_name}"
                        )
                        if folder_path:
                            self._folders_cache[folder_path] = folder_name

                # Recursively get items from subdirectories
                self._populate_subdirectories()

                self._cache_initialized = True
                self.logger.debug(f"Cache initialized with {len(self._items_cache)} total items")
                self.logger.debug(f"Found {len(self._notebooks_cache)} notebooks")
                self.logger.debug(f"Found {len(self._folders_cache)} folders")

                # Debug: Print folder cache contents
                if self._folders_cache:
                    self.logger.debug(f"Folder cache: {self._folders_cache}")

            else:
                self.logger.warning(f"Failed to initialize cache. Status: {response.status_code}")
                self.logger.debug(f"Response text: {response.text}")
                self._cache_initialized = False

        except Exception as e:
            self.logger.warning(f"Failed to initialize cache: {e}")
            import traceback

            traceback.print_exc()
            self._cache_initialized = False

    def _populate_subdirectories(self):
        """Recursively populate cache with items from subdirectories"""
        processed_directories = set()
        directories_to_process = [
            item["path"] for item in self._items_cache if item.get("object_type") == "DIRECTORY"
        ]

        while directories_to_process:
            directory_path = directories_to_process.pop(0)

            # Skip if already processed (avoid infinite loops)
            if directory_path in processed_directories:
                continue

            processed_directories.add(directory_path)

            try:
                url = f"{self._base_url}/api/2.0/workspace/list"
                params = {"path": directory_path, "fmt": "SOURCE"}
                response = requests.get(url, headers=self._get_headers(), params=params)

                if response.status_code == 200:
                    data = self._handle_response(response)
                    sub_items = data.get("objects", [])

                    for item in sub_items:
                        item_type = item.get("object_type", "").upper()

                        if item_type == "NOTEBOOK":
                            self._notebooks_cache.append(item)
                        elif item_type == "DIRECTORY":
                            folder_path = item.get("path")
                            folder_name = folder_path.split("/")[-1] if folder_path else ""
                            self.logger.debug(
                                f"Found nested folder - Path: {folder_path}, Name: {folder_name}"
                            )
                            if folder_path:
                                self._folders_cache[folder_path] = folder_name
                                # Add newly discovered directory to processing queue
                                if folder_path not in processed_directories:
                                    directories_to_process.append(folder_path)

                    # Add sub-items to main cache
                    self._items_cache.extend(sub_items)

                else:
                    self.logger.debug(
                        f"Failed to access directory {directory_path}: HTTP {response.status_code}"
                    )

            except Exception as e:
                self.logger.debug(f"Failed to process directory {directory_path}: {e}")

    def get_platform_name(self):
        return "databricks"

    def get_token(self, audience: str = None) -> str:
        """Get Databricks authentication token using dbutils"""
        try:
            # Try to get dbutils from the global namespace
            import __main__

            dbutils = getattr(__main__, "dbutils", None)
            if dbutils:
                # Get the current notebook context token
                return (
                    dbutils.notebook.entry_point.getDbutils()
                    .notebook()
                    .getContext()
                    .apiToken()
                    .get()
                )
            else:
                # Fallback: try to access dbutils directly (simplified)
                try:
                    from pyspark.sql import SparkSession

                    spark = SparkSession.getActiveSession()
                    if spark:
                        # Try to get token from Spark context if available
                        sc = spark.sparkContext
                        if hasattr(sc, "_jvm") and hasattr(sc._jvm, "com"):
                            # This is a simplified approach - may not work in all cases
                            pass
                except ImportError:
                    pass

                raise Exception("dbutils not available - not running in Databricks environment")
        except Exception as e:
            # If dbutils token fails, try environment variable
            import os

            token = os.getenv("DATABRICKS_TOKEN")
            if token:
                return token
            raise Exception(f"Failed to get Databricks token: {e}")

    def exists(self, path: str) -> bool:
        """Check if file/path exists using dbutils"""
        import __main__

        dbutils = getattr(__main__, "dbutils", None)
        if not dbutils:
            raise Exception("dbutils not available")

        try:
            dbutils.fs.ls(path)
            return True
        except:
            return False

    def copy(self, source: str, destination: str, overwrite: bool = False) -> None:
        """Copy file using dbutils"""
        import __main__

        dbutils = getattr(__main__, "dbutils", None)
        if not dbutils:
            raise Exception("dbutils not available")

        dbutils.fs.cp(source, destination, overwrite)

    def read(self, path: str, encoding: str = "utf-8") -> Union[str, bytes]:
        """Read file content"""
        if path.startswith("/dbfs/") or path.startswith("dbfs:/") or path.startswith("abfss://"):
            # Use dbutils for DBFS and ABFSS paths
            import __main__

            dbutils = getattr(__main__, "dbutils", None)
            if dbutils:
                # dbutils.fs.head returns up to 1MB by default
                return dbutils.fs.head(path, 1024 * 1024 * 10)  # 10MB max

        # For local paths, use standard file operations
        with open(path, "r" if encoding else "rb") as f:
            return f.read()

    def write(self, path: str, content: Union[str, bytes], overwrite: bool = False) -> None:
        """Write file content"""
        if path.startswith("/dbfs/") or path.startswith("dbfs:/"):
            # Use dbutils for DBFS paths
            import __main__

            dbutils = getattr(__main__, "dbutils", None)
            if dbutils:
                # For DBFS, we need to write to a temp file first then copy
                from kindling.bootstrap import get_temp_path

                temp_dir = get_temp_path()
                temp_path = f"{temp_dir}/{path.split('/')[-1]}"
                mode = "w" if isinstance(content, str) else "wb"
                with open(temp_path, mode) as f:
                    f.write(content)
                dbutils.fs.cp(f"file://{temp_path}", path, overwrite)
                return

        # For local paths, use standard file operations
        mode = "w" if isinstance(content, str) else "wb"
        with open(path, mode) as f:
            f.write(content)

    def list(self, path: str) -> List[str]:
        """List files in directory"""
        import __main__

        dbutils = getattr(__main__, "dbutils", None)
        if not dbutils:
            raise Exception("dbutils not available")

        files = dbutils.fs.ls(path)
        return [f.name for f in files]

    def _build_base_url(self) -> str:
        """Build Databricks API base URL from workspace_id or DATABRICKS_HOST env var"""
        import os

        # Try to get workspace_id from config using get() method (for ConfigService)
        # or getattr() for direct attribute access
        workspace_id = None
        if hasattr(self.config, "get"):
            workspace_id = self.config.get("workspace_id", None)
        if not workspace_id:
            workspace_id = getattr(self.config, "workspace_id", None)

        # Parse workspace_id which could be:
        # 1. Full URL: https://adb-123456789.4.azuredatabricks.net
        # 2. Just hostname: adb-123456789.4.azuredatabricks.net
        # 3. Custom domain URL for gov/private clouds
        if workspace_id:
            # If it's already a URL, use it directly
            if workspace_id.startswith("https://") or workspace_id.startswith("http://"):
                return workspace_id.rstrip("/")
            else:
                # Assume it's just the hostname
                return f"https://{workspace_id}"

        # Check DATABRICKS_HOST environment variable (for local testing)
        databricks_host = os.environ.get("DATABRICKS_HOST", "").strip()
        if databricks_host:
            # Remove trailing slash if present
            return databricks_host.rstrip("/")

        # Auto-detect from environment as fallback
        try:
            import __main__

            dbutils = getattr(__main__, "dbutils", None)
            if dbutils:
                # Get workspace URL from notebook context
                context = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
                browser_host_name = context.browserHostName().get()
                return f"https://{browser_host_name}"
        except:
            pass

        raise Exception("No workspace_id provided and unable to detect from environment")

    def _get_token(self) -> str:
        """Get access token for Databricks API with caching"""
        current_time = time.time()

        # Check if we have a valid cached token
        if (
            "token" in self._token_cache
            and "expires_at" in self._token_cache
            and current_time < self._token_cache["expires_at"]
        ):
            return self._token_cache["token"]

        # Get new token
        token = self.get_token()

        # Cache the token (Databricks tokens typically last 24 hours)
        self._token_cache = {
            "token": token,
            "expires_at": (
                datetime.now() + timedelta(hours=23)
            ).timestamp(),  # Refresh an hour early
        }

        return token

    def _get_headers(self) -> Dict[str, str]:
        """Get request headers with authorization"""
        return {
            "Authorization": f"Bearer {self._get_token()}",
            "Content-Type": "application/json",
            "Accept": "application/json",
            "User-Agent": "databricks-notebooks/1.0.0",
        }

    def _handle_response(self, response: requests.Response) -> Any:
        """Handle HTTP response and convert to appropriate exceptions"""
        if 200 <= response.status_code < 300:
            try:
                return response.json() if response.content else None
            except json.JSONDecodeError:
                return response.text if response.content else None

        # Convert HTTP errors to appropriate exceptions
        error_map = {
            401: Exception,  # ClientAuthenticationError
            404: Exception,  # ResourceNotFoundError
            409: Exception,  # ResourceExistsError
        }

        exception_class = error_map.get(response.status_code, Exception)

        try:
            error_data = response.json()
            message = error_data.get("error_code", error_data.get("message", response.text))
        except:
            message = response.text or f"HTTP {response.status_code}"

        raise exception_class(message)

    def list_notebooks(self) -> List[Dict[str, Any]]:
        """List all notebooks in the workspace"""
        if hasattr(self, "_notebooks_cache"):
            return self._notebooks_cache

        # If cache not available, fetch directly
        url = f"{self._base_url}/api/2.0/workspace/list"
        params = {"path": "/", "fmt": "SOURCE"}

        response = requests.get(url, headers=self._get_headers(), params=params)
        data = self._handle_response(response)

        notebooks = []
        if isinstance(data, dict):
            objects = data.get("objects", [])
            notebooks = [obj for obj in objects if obj.get("object_type") == "NOTEBOOK"]

        # nbnames = [obj.name for obj in objects if obj.get('object_type') == 'NOTEBOOK']
        # print("Notebooks before rename {nbnames}")

        # for nb in notebooks:
        #    nb.name = nb.name.split('/')[-1]

        # nbnames =  [nb.name for nb in notebooks]
        # print("Notebooks post rename {nbnames}")

        return notebooks

    def get_notebook_by_path(self, notebook_path: str) -> Optional[Dict[str, Any]]:
        """Get notebook by its workspace path"""
        notebooks = self.list_notebooks()
        for notebook in notebooks:
            if notebook.get("path") == notebook_path:
                return notebook
        return None

    def get_notebooks(self):
        """Get all notebooks as NotebookResource objects"""
        return [self._convert_to_notebook_resource(d) for d in self.list_notebooks()]

    def get_notebook(self, notebook_identifier: str, include_content: bool = True):
        """Get a specific notebook by name or path"""
        # Determine if this is already a full path or just a name
        if notebook_identifier.startswith("/"):
            # Already a full path
            notebook_path = notebook_identifier
        else:
            # Need to resolve the name using cache
            notebook_path = self._resolve_notebook_path(notebook_identifier)
            if not notebook_path:
                raise Exception(f"Notebook '{notebook_identifier}' not found in workspace")

        self.logger.debug(
            f"Resolved notebook identifier '{notebook_identifier}' to path '{notebook_path}'"
        )

        # Get notebook info
        url = f"{self._base_url}/api/2.0/workspace/get-status"
        params = {"path": notebook_path}
        response = requests.get(url, headers=self._get_headers(), params=params)

        if response.status_code == 404:
            raise Exception(f"Notebook '{notebook_path}' not found")
        elif response.status_code != 200:
            self.logger.error(
                f"Failed to get notebook status: {response.status_code} - {response.text}"
            )
            raise Exception(
                f"Failed to get notebook '{notebook_path}': HTTP {response.status_code}"
            )

        notebook_data = self._handle_response(response)

        if include_content:
            # Get notebook content
            content_url = f"{self._base_url}/api/2.0/workspace/export"
            content_params = {"path": notebook_path, "format": "SOURCE"}

            self.logger.debug(f"Getting notebook content: {content_url}")
            content_response = requests.get(
                content_url, headers=self._get_headers(), params=content_params
            )

            if content_response.status_code == 200:
                content_data = content_response.json()

                # Handle different response formats
                if "content" in content_data:
                    import base64

                    try:
                        decoded_content = base64.b64decode(content_data["content"]).decode("utf-8")
                        # For SOURCE format, content is raw Python/SQL code, not JSON
                        notebook_data["content"] = {
                            "nbformat": 4,
                            "nbformat_minor": 2,
                            "metadata": {},
                            "cells": [
                                {
                                    "cell_type": "code",
                                    "source": decoded_content.split("\n"),
                                    "metadata": {},
                                    "outputs": [],
                                    "execution_count": None,
                                }
                            ],
                        }
                        self.logger.debug("Successfully got notebook content")
                    except Exception as e:
                        self.logger.debug(f"Failed to decode content: {e}")
                        notebook_data["content"] = {"cells": []}
                else:
                    self.logger.debug("No content field in response")
                    notebook_data["content"] = {"cells": []}
            else:
                self.logger.debug(
                    f"Failed to get notebook content: Status {content_response.status_code}, Response: {content_response.text}"
                )
                notebook_data["content"] = {"cells": []}

        return self._convert_to_notebook_resource(notebook_data)

    def _resolve_notebook_path(self, notebook_name: str) -> Optional[str]:
        """Resolve a notebook name to its full workspace path using the cache"""
        if not hasattr(self, "_notebooks_cache") or not self._notebooks_cache:
            self.logger.debug("Cache not available, trying to initialize...")
            self._initialize_cache()

        # Search through cached notebooks
        for notebook in self._notebooks_cache:
            notebook_path = notebook.get("path", "")
            if not notebook_path:
                continue

            # Extract the base name from the path
            base_name = notebook_path.split("/")[-1] if "/" in notebook_path else notebook_path

            # Check for exact match
            if base_name == notebook_name:
                self.logger.debug(f"Found exact match: '{notebook_name}' -> '{notebook_path}'")
                return notebook_path

            # Check for match without extension
            base_name_no_ext = base_name.replace(".ipynb", "").replace(".py", "")
            notebook_name_no_ext = notebook_name.replace(".ipynb", "").replace(".py", "")
            if base_name_no_ext == notebook_name_no_ext:
                self.logger.debug(
                    f"Found match without extension: '{notebook_name}' -> '{notebook_path}'"
                )
                return notebook_path

        # Log available notebooks for debugging
        available_notebooks = [nb.get("path", "no-path") for nb in self._notebooks_cache[:10]]
        self.logger.debug(
            f"Could not resolve '{notebook_name}'. Available notebooks (first 10): {available_notebooks}"
        )

        return None

    def get_notebook_by_path(self, notebook_path: str) -> Optional[Dict[str, Any]]:
        """Get notebook by its workspace path"""
        if not hasattr(self, "_notebooks_cache") or not self._notebooks_cache:
            self._initialize_cache()

        for notebook in self._notebooks_cache:
            if notebook.get("path") == notebook_path:
                return notebook
        return None

    def get_notebook_by_name(self, notebook_name: str) -> Optional[Dict[str, Any]]:
        """Get notebook by its base name (searches cache)"""
        resolved_path = self._resolve_notebook_path(notebook_name)
        if resolved_path:
            return self.get_notebook_by_path(resolved_path)
        return None

    def create_or_update_notebook(
        self, notebook_path: str, notebook_data: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Create or update a notebook"""
        url = f"{self._base_url}/api/2.0/workspace/import"

        # Convert notebook content to base64
        import base64

        if isinstance(notebook_data.get("content"), dict):
            content_str = json.dumps(notebook_data["content"])
        else:
            content_str = str(notebook_data.get("content", ""))

        content_base64 = base64.b64encode(content_str.encode("utf-8")).decode("utf-8")

        payload = {
            "path": notebook_path,
            "format": "JUPYTER",
            "content": content_base64,
            "overwrite": True,
        }

        try:
            self.logger.debug(f"Creating/updating notebook - URL: {url}")
            self.logger.debug(f"Notebook path: {notebook_path}")

            response = requests.post(url, headers=self._get_headers(), json=payload)
            self.logger.debug(f"Response Status: {response.status_code}")

            if response.status_code not in [200, 201]:
                self.logger.debug(f"Response Text: {response.text}")

            return self._handle_response(response)
        except Exception as e:
            self.logger.debug(f"Exception in create_or_update_notebook: {e}")
            raise

    def delete_notebook(self, notebook_path: str) -> None:
        """Delete a notebook"""
        url = f"{self._base_url}/api/2.0/workspace/delete"
        payload = {"path": notebook_path}

        response = requests.post(url, headers=self._get_headers(), json=payload)
        self._handle_response(response)

    def get_folder_name(self, folder_path: str) -> str:
        """Get folder name from folder path"""
        if not folder_path:
            return ""

        # Extract folder name from path
        return folder_path.split("/")[-1] if folder_path else ""

    def _convert_to_notebook_resource(
        self, databricks_data: Dict[str, Any], include_content: bool = True
    ) -> "NotebookResource":
        """Convert Databricks workspace object to NotebookResource"""

        notebook_path = databricks_data.get("path", "")
        notebook_name = notebook_path.split("/")[-1] if notebook_path else ""

        notebook_data = {
            "id": databricks_data.get("object_id", notebook_path),
            "name": notebook_name,
            "type": "Microsoft.Synapse/workspaces/notebooks",  # Keep compatible format
            "etag": str(databricks_data.get("modified_at", "")),
            "properties": {},
        }

        # Extract folder information from path
        folder_path = "/".join(notebook_path.split("/")[:-1]) if "/" in notebook_path else ""
        folder_info = {"name": self.get_folder_name(folder_path), "path": folder_path}

        if include_content and "content" in databricks_data:
            # Parse notebook content
            content = databricks_data["content"]
            if isinstance(content, dict):
                notebook_content = content
            else:
                # If content is string, try to parse as JSON
                try:
                    notebook_content = json.loads(content) if isinstance(content, str) else {}
                except json.JSONDecodeError:
                    # Create basic notebook structure
                    notebook_content = {
                        "nbformat": 4,
                        "nbformat_minor": 2,
                        "metadata": {},
                        "cells": [
                            {
                                "cell_type": "code",
                                "source": content.split("\n") if isinstance(content, str) else [],
                                "metadata": {},
                                "outputs": [],
                                "execution_count": None,
                            }
                        ],
                    }

            # Add folder information
            notebook_content["folder"] = folder_info
            notebook_data["properties"] = notebook_content
        else:
            # Even without content, always add folder info
            notebook_data["properties"] = {"folder": folder_info}

        return NotebookResource(**notebook_data)

    def _convert_from_notebook_resource(self, notebook: "NotebookResource") -> Dict[str, Any]:
        """Convert NotebookResource to Databricks format"""
        return {
            "name": notebook.name,
            "content": {
                "cells": [
                    {
                        "cell_type": getattr(cell, "cell_type", "code"),
                        "source": getattr(cell, "source", []),
                    }
                    for cell in getattr(notebook.properties, "cells", [])
                ]
            },
        }

    def is_interactive_session(self) -> bool:
        """Check if running in interactive Databricks session"""
        try:
            # Check for Databricks environment
            import __main__

            dbutils = getattr(__main__, "dbutils", None)
            if dbutils:
                return True

            # Check for IPython/Jupyter
            if "get_ipython" not in globals():
                try:
                    from IPython import get_ipython
                except ImportError:
                    return False
            else:
                get_ipython = globals()["get_ipython"]

            ipython = get_ipython()
            return ipython is not None
        except:
            return False

    def get_workspace_info(self) -> Dict[str, Any]:
        """Get Databricks workspace information"""
        try:
            import __main__

            dbutils = getattr(__main__, "dbutils", None)
            if dbutils:
                context = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
                return {
                    "workspace_id": context.workspaceId().get(),
                    "workspace_url": context.browserHostName().get(),
                    "cluster_id": context.clusterId().get(),
                    "environment": "databricks",
                }
        except Exception:
            pass
        return {"environment": "databricks"}

    def get_cluster_info(self) -> Dict[str, Any]:
        """Get Databricks cluster information"""
        try:
            import __main__

            spark = getattr(__main__, "spark", None)
            if spark:
                cluster_info = {
                    "app_name": spark.sparkContext.appName,
                    "spark_version": spark.version,
                    "master": spark.sparkContext.master,
                }

                # Try to get cluster ID from dbutils
                dbutils = getattr(__main__, "dbutils", None)
                if dbutils:
                    try:
                        context = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
                        cluster_info["cluster_id"] = context.clusterId().get()
                        cluster_info["cluster_name"] = context.clusterName().get()
                    except:
                        pass

                return cluster_info
        except Exception:
            pass
        return {}

    def deploy_spark_job(
        self, app_files: Dict[str, str], job_config: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Deploy application as Spark job on Databricks

        Creates a Databricks job, uploads files to DBFS, and configures the job.

        Args:
            app_files: Dictionary of {filename: content} to deploy
            job_config: Configuration including:
                - job_name: Display name for the job
                - cluster_id: Existing cluster ID (optional)
                - new_cluster: New cluster config (optional)
                - entry_point: Main Python file (default: kindling_bootstrap.py)
                - libraries: Additional libraries to install

        Returns:
            Dictionary with: {job_id, deployment_path, metadata}
        """
        self.logger.info(f"Deploying Spark job to Databricks: {job_config.get('job_name')}")

        job_name = job_config["job_name"]
        entry_point = job_config.get("entry_point", "kindling_bootstrap.py")

        # Step 1: Upload files to DBFS
        self.logger.debug(f"Uploading {len(app_files)} files to DBFS...")
        deployment_path = f"/dbfs/tmp/kindling_jobs/{job_name}"
        dbfs_paths = self._upload_files_to_dbfs(deployment_path, app_files)
        self.logger.debug(f"Uploaded files to: {deployment_path}")

        # Step 2: Create Databricks job
        self.logger.debug("Creating Databricks job...")
        job = self._create_databricks_job(job_name, entry_point, deployment_path, job_config)
        job_id = job["job_id"]
        self.logger.debug(f"Created job: {job_id}")

        return {
            "job_id": str(job_id),
            "deployment_path": deployment_path,
            "metadata": {
                "job_name": job_name,
                "entry_point": entry_point,
                "files": list(dbfs_paths.keys()),
            },
        }

    def run_spark_job(self, job_id: str, parameters: Dict[str, Any] = None) -> str:
        """Execute a Spark job on Databricks

        Args:
            job_id: Job ID
            parameters: Optional runtime parameters

        Returns:
            Run ID for monitoring
        """
        self.logger.info(f"Running Databricks job: {job_id}")

        url = f"{self._base_url}/api/2.1/jobs/run-now"

        payload = {"job_id": int(job_id)}
        if parameters:
            payload["python_params"] = [f"{k}={v}" for k, v in parameters.items()]

        response = requests.post(url, headers=self._get_headers(), json=payload)
        result = self._handle_response(response)

        run_id = str(result.get("run_id"))
        self.logger.info(f"Job started with run_id: {run_id}")
        return run_id

    def get_job_status(self, run_id: str) -> Dict[str, Any]:
        """Get status of a running Spark job

        Args:
            run_id: Run ID from run_spark_job()

        Returns:
            Dictionary with: {status, start_time, end_time, error, logs}
        """
        url = f"{self._base_url}/api/2.1/jobs/runs/get"

        params = {"run_id": int(run_id)}
        response = requests.get(url, headers=self._get_headers(), params=params)
        result = self._handle_response(response)

        state = result.get("state", {})
        return {
            "status": state.get("life_cycle_state") or "UNKNOWN",
            "result_state": state.get("result_state"),
            "start_time": result.get("start_time"),
            "end_time": result.get("end_time"),
            "error": state.get("state_message"),
            "logs": result.get("run_page_url"),
        }

    def cancel_job(self, run_id: str) -> bool:
        """Cancel a running Spark job

        Args:
            run_id: Run ID to cancel

        Returns:
            True if cancelled successfully
        """
        self.logger.info(f"Cancelling Databricks job: {run_id}")

        url = f"{self._base_url}/api/2.1/jobs/runs/cancel"

        payload = {"run_id": int(run_id)}
        response = requests.post(url, headers=self._get_headers(), json=payload)

        if response.status_code == 200:
            self.logger.info(f"Job cancelled: {run_id}")
            return True
        else:
            self.logger.error(f"Failed to cancel job: {response.status_code} - {response.text}")
            return False

    def _upload_files_to_dbfs(
        self, deployment_path: str, app_files: Dict[str, str]
    ) -> Dict[str, str]:
        """Upload files to DBFS

        Returns:
            Dictionary mapping filename to DBFS path
        """
        dbfs_paths = {}

        # Create directory
        import __main__

        dbutils = getattr(__main__, "dbutils", None)
        if not dbutils:
            raise Exception("dbutils not available")

        # Ensure directory exists
        try:
            dbutils.fs.mkdirs(deployment_path)
        except:
            pass

        # Write each file
        from kindling.bootstrap import get_temp_path

        temp_dir = get_temp_path()

        for filename, content in app_files.items():
            file_path = f"{deployment_path}/{filename}"

            # Write to temp file then copy to DBFS
            temp_path = f"{temp_dir}/{filename}"
            with open(temp_path, "w") as f:
                f.write(content)

            dbutils.fs.cp(f"file://{temp_path}", f"dbfs:{file_path}", True)
            dbfs_paths[filename] = f"dbfs:{file_path}"

        return dbfs_paths

    def _create_databricks_job(
        self, job_name: str, entry_point: str, deployment_path: str, job_config: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Create a Databricks job"""

        # Build job configuration
        task_config = {
            "task_key": "main",
            "spark_python_task": {
                "python_file": f"dbfs:{deployment_path}/{entry_point}",
                "parameters": job_config.get("parameters", []),
            },
            "libraries": job_config.get("libraries", []),
        }

        # Add cluster configuration
        if "cluster_id" in job_config:
            task_config["existing_cluster_id"] = job_config["cluster_id"]
        elif "new_cluster" in job_config:
            task_config["new_cluster"] = job_config["new_cluster"]
        else:
            # Default cluster config
            task_config["new_cluster"] = {
                "spark_version": "13.3.x-scala2.12",
                "node_type_id": "Standard_DS3_v2",
                "num_workers": 2,
            }

        payload = {"name": job_name, "tasks": [task_config], "format": "MULTI_TASK"}

        url = f"{self._base_url}/api/2.1/jobs/create"
        response = requests.post(url, headers=self._get_headers(), json=payload)

        return self._handle_response(response)


@PlatformServices.register(name="databricks", description="Databricks platform service")
def create_platform_service(config, logger):
    return DatabricksService(config, logger)


# ============================================================================
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
            azure_tenant_id=os.getenv("AZURE_TENANT_ID"),
            azure_client_id=os.getenv("AZURE_CLIENT_ID"),
            azure_client_secret=os.getenv("AZURE_CLIENT_SECRET"),
        )

    def get_platform_name(self) -> str:
        """Get platform name"""
        return "databricks"

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

        # Merge any additional config overrides (generic mechanism)
        # CONFIG__kindling__key -> passed as config:key=value
        if "config_overrides" in job_config:
            overrides = job_config["config_overrides"]

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
        existing_cluster_id = job_config.get("existing_cluster_id") or job_config.get("cluster_id")

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
