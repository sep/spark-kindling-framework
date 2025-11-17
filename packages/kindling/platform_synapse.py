import json
import time
import types
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Union
from urllib.parse import quote

# Azure Synapse imports - always available in Synapse wheel
from azure.core.credentials import AccessToken, TokenCredential
from azure.core.exceptions import *
from azure.synapse.artifacts import ArtifactsClient
from azure.synapse.artifacts.models import *
from kindling.data_apps import AppDeploymentService
from kindling.notebook_framework import *
from kindling.spark_session import *

from .platform_provider import PlatformAPI

mssparkutils = None


def _get_mssparkutils():
    """Get mssparkutils with fallback import chain for different environments"""
    import __main__

    # First try to get from __main__ (notebook environment)
    mssparkutils = getattr(__main__, "mssparkutils", None)
    if mssparkutils is not None:
        return mssparkutils

    # Fall back to explicit import (Spark job environment)
    try:
        from notebookutils import mssparkutils

        return mssparkutils
    except ImportError:
        pass

    # Final fallback - raise error
    raise ImportError("mssparkutils not available in this environment")


class SynapseService(PlatformService):
    def __init__(self, config, logger):
        self.config = config
        self.logger = logger
        self._base_url = self._build_base_url()

        # Initialize credential - use self as credential like Fabric does
        # The get_token method below handles mssparkutils
        self.credential = self

        # Initialize Synapse client
        from azure.synapse.artifacts import ArtifactsClient

        self.client = ArtifactsClient(endpoint=self._base_url, credential=self.credential)

    """Synapse-specific implementation of AppDeploymentService"""

    def __init__(self, synapse_service, logger):
        self.synapse_service = synapse_service
        self.logger = logger

    def submit_spark_job(self, job_config: Dict[str, Any]) -> Dict[str, Any]:
        """Submit a Spark job to Synapse

        Args:
            job_config: Contains 'script_path', 'app_name', 'environment_vars'

        Returns:
            Dict containing job_id and submission status
        """
        try:
            mssparkutils = _get_mssparkutils()

            if not mssparkutils:
                raise RuntimeError("mssparkutils not available in current environment")

            script_path = job_config.get("script_path")
            app_name = job_config.get("app_name", "unknown-app")
            env_vars = job_config.get("environment_vars", {})

            # In Synapse, we typically run scripts directly in the notebook context
            # For system testing, we'll simulate job submission by running the script

            self.logger.info(f"Executing Synapse job for app: {app_name}")
            self.logger.info(f"Script path: {script_path}")

            # Generate a simple job ID based on timestamp and app name
            import time

            job_id = f"synapse-{app_name}-{int(time.time())}"

            # Set environment variables if provided
            import os

            for key, value in env_vars.items():
                os.environ[key] = str(value)

            try:
                # Execute the script in the current context
                # For KDA packages, the script should be the main.py file
                if script_path and self.synapse_service.exists(script_path):
                    self.logger.info(f"Executing script: {script_path}")
                    exec(open(script_path).read())
                    status = "SUCCEEDED"
                    message = f"Job {job_id} completed successfully"
                else:
                    status = "FAILED"
                    message = f"Script not found: {script_path}"

            except Exception as e:
                status = "FAILED"
                message = f"Job execution failed: {str(e)}"
                self.logger.error(message)

            return {
                "job_id": job_id,
                "status": status,
                "message": message,
                "submission_time": time.time(),
            }

        except Exception as e:
            self.logger.error(f"Failed to submit Synapse job: {e}")
            return {
                "job_id": None,
                "status": "FAILED",
                "message": f"Job submission failed: {str(e)}",
            }

    def get_job_status(self, job_id: str) -> Dict[str, Any]:
        """Get the status of a submitted job

        Since Synapse notebooks run synchronously, this returns final status
        """
        # In a real implementation, you would track job states
        # For now, we'll return a basic status
        return {
            "job_id": job_id,
            "status": "COMPLETED",
            "message": "Job status not tracked in notebook execution mode",
        }

    def cancel_job(self, job_id: str) -> bool:
        """Cancel a running job

        Not applicable for synchronous notebook execution
        """
        self.logger.warning(f"Job cancellation not supported for notebook execution: {job_id}")
        return False

    def get_job_logs(self, job_id: str) -> str:
        """Get logs from a job

        In notebook mode, logs are displayed in real-time
        """
        return f"Logs for {job_id} are displayed in the notebook output"

    def create_job_config(
        self, app_name: str, app_path: str, environment_vars: Optional[Dict[str, str]] = None
    ) -> Dict[str, Any]:
        """Create Synapse-specific job configuration"""

        # For KDA packages, the main script is typically main.py
        main_script = f"{app_path}/main.py"

        return {
            "app_name": app_name,
            "script_path": main_script,
            "environment_vars": environment_vars or {},
            "platform": "synapse",
            "execution_mode": "notebook",
        }


class SynapseService(PlatformService):
    def __init__(self, config, logger):
        self.config = config
        self.logger = logger
        self._base_url = self._build_base_url()

        # Initialize credential and client
        # Use DefaultAzureCredential for both notebook and batch contexts
        from azure.identity import DefaultAzureCredential

        self.credential = DefaultAzureCredential()
        self.client = ArtifactsClient(endpoint=self._base_url, credential=self.credential)

        # Cache system
        self._token_cache = {}
        self._items_cache = []
        self._folders_cache = {}
        self._notebooks_cache = []

        self._initialize_cache()

    def _initialize_cache(self):
        """Initialize the cache with all workspace items and folders"""
        try:
            self.logger.debug("Initializing workspace cache...")

            # Get all notebooks using Synapse SDK
            notebooks = self.client.notebook.get_notebooks_by_workspace()
            self._notebooks_cache = []

            for notebook in notebooks:
                self._notebooks_cache.append(
                    {
                        "id": notebook.name,
                        "displayName": notebook.name,
                        "type": "notebook",
                        "etag": notebook.etag if hasattr(notebook, "etag") else None,
                        "properties": (
                            self._convert_synapse_properties_to_dict(notebook.properties)
                            if hasattr(notebook, "properties")
                            else {}
                        ),
                    }
                )

            # Get folders from notebook properties
            self._folders_cache = {}
            for notebook in self._notebooks_cache:
                if "folder" in notebook.get("properties", {}):
                    folder_info = notebook["properties"]["folder"]
                    if isinstance(folder_info, dict) and "name" in folder_info:
                        folder_name = folder_info["name"]
                        if folder_name:
                            # Use folder name as ID for simplicity
                            self._folders_cache[folder_name] = folder_name

            self._items_cache = self._notebooks_cache.copy()
            self._cache_initialized = True

            self.logger.debug(f"Cache initialized with {len(self._items_cache)} total items")
            self.logger.debug(f"Found {len(self._notebooks_cache)} notebooks")
            self.logger.debug(f"Found {len(self._folders_cache)} folders")

        except Exception as e:
            self.logger.warning(f"Failed to initialize cache: {e}")
            import traceback

            traceback.print_exc()
            self._cache_initialized = False

    def initialize(self):
        """Initialize the service"""
        # Already initialized in __init__, but this satisfies the interface
        if not self._cache_initialized:
            self._initialize_cache()
        return True

    def get_platform_name(self):
        return "synapse"

    def get_token(self, *scopes, **kwargs):
        """Get access token using mssparkutils - implements TokenCredential interface"""
        import time

        from azure.core.credentials import AccessToken

        mssparkutils = _get_mssparkutils()
        if mssparkutils:
            # Use mssparkutils to get token (works in both notebook and batch contexts)
            token_str = mssparkutils.credentials.getToken("https://dev.azuresynapse.net")
            # Return AccessToken with token and expiry (1 hour from now)
            return AccessToken(token_str, int(time.time()) + 3600)
        else:
            # Fallback for non-Synapse environments (shouldn't happen in production)
            raise RuntimeError("mssparkutils not available - cannot get authentication token")

    def exists(self, path: str) -> bool:
        """Check if a file exists"""
        try:
            # For Synapse, we'll use mssparkutils if available
            mssparkutils = _get_mssparkutils()
            if mssparkutils:
                return mssparkutils.fs.exists(path)

            # Fallback: check if it's a notebook by name
            return self.get_notebook_id_by_name(path) is not None
        except:
            return False

    def copy(self, source: str, destination: str, overwrite: bool = False) -> None:
        """Copy a file from source to destination"""
        mssparkutils = _get_mssparkutils()
        if mssparkutils:
            mssparkutils.fs.cp(source, destination, overwrite)
        else:
            raise NotImplementedError("File copy not available without mssparkutils")

    def delete(self, path: str, recurse=False) -> None:
        mssparkutils = _get_mssparkutils()
        if mssparkutils:
            mssparkutils.fs.rm(path, recurse)
        else:
            raise NotImplementedError("File delete not available without mssparkutils")

    def read(self, path: str, encoding: str = "utf-8") -> Union[str, bytes]:
        """Read file content"""
        try:
            # For ABFSS paths, use mssparkutils
            if path.startswith("abfss://") or path.startswith("wasbs://"):
                mssparkutils = _get_mssparkutils()
                if mssparkutils:
                    content = mssparkutils.fs.head(path, 10000000)  # Read up to ~10MB
                    if encoding:
                        return content
                    else:
                        return content.encode(encoding or "utf-8")

            # For local paths, use regular file I/O
            with open(path, "r" if encoding else "rb") as f:
                return f.read()
        except Exception as e:
            raise FileNotFoundError(f"Failed to read file {path}: {str(e)}")

    def move(self, path: str, dest: str) -> Union[str, bytes]:
        mssparkutils = _get_mssparkutils()
        if mssparkutils:
            mssparkutils.fs.mv(path, dest)
        else:
            raise NotImplementedError("File move not available without mssparkutils")

    def write(self, path: str, content: Union[str, bytes], overwrite: bool = False) -> None:
        """Write content to file"""
        mode = "w" if isinstance(content, str) else "wb"
        with open(path, mode) as f:
            f.write(content)

    def list(self, path: str) -> List[str]:
        """List files in directory"""
        import __main__

        mssparkutils = getattr(__main__, "mssparkutils", None)
        if mssparkutils:
            files = mssparkutils.fs.ls(path)
            return [f.name for f in files]
        else:
            raise NotImplementedError("Directory listing not available without mssparkutils")

    def _build_base_url(self) -> str:
        spark = get_or_create_spark_session()
        idkey = "spark.synapse.workspace.name"
        hostkey = "spark.synapse.gatewayHost"
        workspace_id = self.config.get(idkey, spark.conf.get(idkey))
        workspace_host = self.config.get(hostkey, spark.conf.get(hostkey))

        return f"https://{workspace_id}.{workspace_host}"

    def _get_token(self) -> str:
        """Get access token for Synapse API"""
        current_time = time.time()

        # Check if we have a valid cached token
        if (
            "token" in self._token_cache
            and "expires_at" in self._token_cache
            and current_time < self._token_cache["expires_at"]
        ):
            return self._token_cache["token"]

        # Get new token
        token_response = self.credential.get_token("https://dev.azuresynapse.net/.default")

        # Cache the token
        self._token_cache = {
            "token": token_response.token,
            "expires_at": token_response.expires_on,
        }

        return token_response.token

    def _get_headers(self) -> Dict[str, str]:
        """Get request headers with authorization"""
        return {
            "Authorization": f"Bearer {self._get_token()}",
            "Content-Type": "application/json",
            "Accept": "application/json",
            "User-Agent": "azure-synapse-artifacts/1.0.0",
        }

    def _handle_response(self, response) -> Any:
        """Handle response from Synapse SDK operations"""
        # For SDK responses, we typically don't need to handle HTTP status codes
        # The SDK handles exceptions internally
        return response

    def list_notebooks(self) -> List[Dict[str, Any]]:
        """List all notebooks in the workspace"""
        try:
            notebooks = self.client.notebook.get_notebooks_by_workspace()
            result = []

            for notebook in notebooks:
                result.append(
                    {
                        "id": notebook.name,
                        "displayName": notebook.name,
                        "type": "notebook",
                        "etag": notebook.etag if hasattr(notebook, "etag") else None,
                        "properties": (
                            self._convert_synapse_properties_to_dict(notebook.properties)
                            if hasattr(notebook, "properties")
                            else {}
                        ),
                    }
                )

            return result
        except Exception as e:
            self.logger.error(f"Failed to list notebooks: {e}")
            return []

    def get_notebook_id_by_name(self, notebook_name: str) -> Optional[str]:
        """Get notebook ID by display name"""
        try:
            # In Synapse, notebook name is typically the ID
            notebook = self.client.notebook.get_notebook(notebook_name)
            return notebook.name if notebook else None
        except ResourceNotFoundError:
            return None
        except Exception as e:
            self.logger.debug(f"Error getting notebook ID: {e}")
            return None

    def get_notebooks(self):
        """Get all notebooks as NotebookResource objects"""
        return [self._convert_to_notebook_resource(d) for d in self.list_notebooks()]

    def get_notebook(self, notebook_name: str, include_content: bool = True):
        """Get a specific notebook"""
        try:
            notebook = self.client.notebook.get_notebook(notebook_name)

            if not notebook:
                raise ResourceNotFoundError(f"Notebook '{notebook_name}' not found")

            # Convert to dictionary format with proper metadata handling
            notebook_data = {
                "id": notebook.name,
                "displayName": notebook.name,
                "type": "notebook",
                "etag": notebook.etag if hasattr(notebook, "etag") else None,
                "properties": (
                    self._convert_synapse_properties_to_dict(notebook.properties)
                    if hasattr(notebook, "properties")
                    else {}
                ),
            }

            if include_content:
                # Extract notebook content from properties
                if hasattr(notebook.properties, "cells"):
                    notebook_data["definition"] = {
                        "nbformat": getattr(notebook.properties, "nbformat", 4),
                        "nbformat_minor": getattr(notebook.properties, "nbformat_minor", 2),
                        "metadata": self._convert_metadata_to_dict(
                            getattr(notebook.properties, "metadata", {})
                        ),
                        "cells": self._convert_cells_to_dict(
                            getattr(notebook.properties, "cells", [])
                        ),
                    }

            return self._convert_to_notebook_resource(notebook_data, include_content)

        except Exception as e:
            self.logger.error(f"Failed to get notebook '{notebook_name}': {e}")
            raise

    def _convert_synapse_properties_to_dict(self, properties) -> Dict[str, Any]:
        """Convert Synapse properties object to dictionary"""
        if isinstance(properties, dict):
            return properties
        elif hasattr(properties, "__dict__"):
            result = {}
            for key, value in properties.__dict__.items():
                if hasattr(value, "__dict__") and not isinstance(
                    value, (str, int, float, bool, list)
                ):
                    result[key] = value.__dict__
                else:
                    result[key] = value
            return result
        else:
            return {}

    def _convert_metadata_to_dict(self, metadata) -> Dict[str, Any]:
        """Convert metadata object to dictionary, ensuring it's never None"""
        if metadata is None:
            return {}
        elif isinstance(metadata, dict):
            return metadata
        elif hasattr(metadata, "__dict__"):
            return metadata.__dict__
        else:
            return {}

    def _convert_cells_to_dict(self, cells) -> List[Dict[str, Any]]:
        """Convert cells to list of dictionaries with safe metadata handling"""
        if not cells:
            return []

        result = []
        for cell in cells:
            if isinstance(cell, dict):
                # Ensure metadata is not None
                cell_copy = cell.copy()
                if "metadata" not in cell_copy or cell_copy["metadata"] is None:
                    cell_copy["metadata"] = {}
                result.append(cell_copy)
            elif hasattr(cell, "__dict__"):
                cell_dict = cell.__dict__.copy()
                # Ensure metadata is a dictionary
                if "metadata" in cell_dict:
                    metadata = self._convert_metadata_to_dict(cell_dict["metadata"])
                    cell_dict["metadata"] = metadata
                else:
                    cell_dict["metadata"] = {}
                result.append(cell_dict)
            else:
                # Empty cell - ensure it has empty metadata
                result.append({"cell_type": "code", "source": [], "metadata": {}, "outputs": []})

        return result

    def create_or_update_notebook(
        self, notebook_name: str, notebook_data: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Create or update a notebook"""
        try:
            self.logger.debug(f"Creating/updating notebook: {notebook_name}")

            # Convert notebook_data to Synapse notebook format
            notebook_resource = self._convert_to_synapse_notebook(notebook_name, notebook_data)

            # Create or update the notebook
            operation = self.client.notebook.begin_create_or_update_notebook(
                notebook_name, notebook_resource
            )

            # Wait for completion
            result = operation.result()

            # Convert back to dictionary format
            return {
                "id": result.name,
                "displayName": result.name,
                "type": "notebook",
                "etag": result.etag if hasattr(result, "etag") else None,
                "properties": (result.properties.__dict__ if hasattr(result, "properties") else {}),
            }

        except Exception as e:
            self.logger.error(f"Failed to create/update notebook '{notebook_name}': {e}")
            raise

    def delete_notebook(self, notebook_name: str) -> None:
        """Delete a notebook"""
        try:
            operation = self.client.notebook.begin_delete_notebook(notebook_name)
            operation.result()  # Wait for completion
            self.logger.debug(f"Successfully deleted notebook: {notebook_name}")
        except Exception as e:
            self.logger.error(f"Failed to delete notebook '{notebook_name}': {e}")
            raise

    def create_notebook(self, name: str, notebook) -> Dict[str, Any]:
        """Create a notebook"""
        return self.create_or_update_notebook(name, notebook)

    def update_notebook(self, name: str, notebook) -> Dict[str, Any]:
        """Update a notebook"""
        return self.create_or_update_notebook(name, notebook)

    def get_spark_session(self):
        """Get the Spark session"""
        try:
            import __main__

            return getattr(__main__, "spark", None)
        except:
            return None

    def get_config(self, key: str, default: Any = None) -> Any:
        """Get configuration value"""
        return getattr(self.config, key, default)

    def set_config(self, key: str, value: Any) -> None:
        """Set configuration value"""
        setattr(self.config, key, value)

    def get_log_level(self) -> str:
        """Get current log level"""
        try:
            spark = self.get_spark_session()
            if spark:
                return str(
                    spark.sparkContext._jvm.org.apache.log4j.LogManager.getRootLogger().getLevel()
                )
            return "INFO"
        except:
            return "INFO"

    def get_folder_name(self, folder_id: str) -> str:
        """Get folder name from folder ID"""
        if not folder_id:
            return ""

        # In Synapse, folder ID is typically the folder name
        return self._folders_cache.get(folder_id, folder_id)

    def _convert_to_notebook_resource(
        self, synapse_data: Dict[str, Any], include_content: bool = True
    ):
        """Convert Synapse notebook to NotebookResource with safe metadata handling

        Note: Returns our custom NotebookResource from notebook_framework, not Azure SDK's.
        The Azure SDK's NotebookResource expects properties to be a Notebook object,
        but we need dict-based properties for compatibility with notebook_framework.
        """
        from kindling.notebook_framework import (
            NotebookResource as KindlingNotebookResource,
        )

        notebook_data = {
            "id": synapse_data.get("id"),
            "name": synapse_data.get("displayName", synapse_data.get("name")),
            "type": "Microsoft.Synapse/workspaces/notebooks",
            "etag": synapse_data.get("etag"),
            "properties": {},
        }

        # Extract folder information
        properties = synapse_data.get("properties", {})
        folder_info = properties.get("folder", {})

        if isinstance(folder_info, dict) and "name" in folder_info:
            folder_name = folder_info["name"]
        else:
            folder_name = ""

        folder_info = {"name": folder_name, "path": folder_name}

        if include_content and "definition" in synapse_data:
            # Use the definition as notebook content
            notebook_content = synapse_data["definition"]
            notebook_content = self._ensure_safe_notebook_content(notebook_content)
            notebook_content["folder"] = folder_info
            notebook_data["properties"] = notebook_content
        elif include_content and properties:
            # Extract content from properties
            notebook_content = self._extract_notebook_content_from_synapse(properties)
            notebook_content = self._ensure_safe_notebook_content(notebook_content)
            notebook_content["folder"] = folder_info
            notebook_data["properties"] = notebook_content
        else:
            notebook_data["properties"] = {"folder": folder_info}

        # Use our custom NotebookResource that converts dict properties to Notebook objects
        return KindlingNotebookResource(**notebook_data)

    def _ensure_safe_notebook_content(self, content: Dict[str, Any]) -> Dict[str, Any]:
        """Ensure notebook content has safe metadata values"""
        content = content.copy()

        if "metadata" not in content or content["metadata"] is None:
            content["metadata"] = {}

        if "cells" in content and isinstance(content["cells"], list):
            safe_cells = []
            for cell in content["cells"]:
                if isinstance(cell, dict):
                    cell_copy = cell.copy()
                    if "metadata" not in cell_copy or cell_copy["metadata"] is None:
                        cell_copy["metadata"] = {}
                    safe_cells.append(cell_copy)
                else:
                    safe_cells.append(
                        {"cell_type": "code", "source": [], "metadata": {}, "outputs": []}
                    )
            content["cells"] = safe_cells

        return content

    def _extract_notebook_content_from_synapse(self, properties: Dict[str, Any]) -> Dict[str, Any]:
        """Extract notebook content from Synapse properties with safe metadata handling"""
        # Extract metadata and ensure it's a dictionary
        metadata = self._convert_metadata_to_dict(properties.get("metadata", {}))

        content = {
            "nbformat": properties.get("nbformat", 4),
            "nbformat_minor": properties.get("nbformat_minor", 2),
            "metadata": metadata,
            "cells": [],
        }

        # Extract cells if available
        if "cells" in properties:
            cells = properties["cells"]
            if isinstance(cells, list):
                for cell in cells:
                    if isinstance(cell, dict):
                        # Ensure metadata exists and is not None
                        cell_copy = cell.copy()
                        if "metadata" not in cell_copy or cell_copy["metadata"] is None:
                            cell_copy["metadata"] = {}
                        content["cells"].append(cell_copy)
                    elif hasattr(cell, "__dict__"):
                        # Convert cell objects to dictionaries
                        cell_dict = cell.__dict__.copy()
                        # Ensure metadata is a dictionary
                        if "metadata" in cell_dict:
                            cell_dict["metadata"] = self._convert_metadata_to_dict(
                                cell_dict["metadata"]
                            )
                        else:
                            cell_dict["metadata"] = {}
                        content["cells"].append(cell_dict)
                    else:
                        # Fallback for unknown cell types
                        content["cells"].append(
                            {"cell_type": "code", "source": [], "metadata": {}, "outputs": []}
                        )

        return content

    def _convert_to_synapse_notebook(
        self, notebook_name: str, notebook_data: Dict[str, Any]
    ) -> NotebookResource:
        """Convert notebook data to Synapse NotebookResource

        Note: Returns Azure SDK's NotebookResource with proper Notebook object.
        This is used when creating/updating notebooks via the Synapse API.
        """
        # Extract properties from notebook_data
        properties = notebook_data.get("properties", {})

        # Create cells as NotebookCell objects
        cells = []
        if "cells" in properties:
            for cell_data in properties["cells"]:
                if isinstance(cell_data, dict):
                    # Create NotebookCell from dict
                    cell = NotebookCell(
                        cell_type=cell_data.get("cell_type", "code"),
                        source=cell_data.get("source", []),
                        metadata=cell_data.get("metadata", {}),
                        outputs=(
                            cell_data.get("outputs", [])
                            if cell_data.get("cell_type") == "code"
                            else None
                        ),
                        execution_count=(
                            cell_data.get("execution_count")
                            if cell_data.get("cell_type") == "code"
                            else None
                        ),
                    )
                    cells.append(cell)

        # Create metadata as NotebookMetadata object
        metadata_dict = self._convert_metadata_to_dict(properties.get("metadata", {}))
        metadata = NotebookMetadata(**metadata_dict) if metadata_dict else NotebookMetadata()

        # Create folder as NotebookFolder object if provided
        folder = None
        folder_info = properties.get("folder", {})
        if folder_info and isinstance(folder_info, dict):
            folder = NotebookFolder(
                name=folder_info.get("name", ""), path=folder_info.get("path", "")
            )

        # Create the Notebook object with proper Azure SDK models
        notebook = Notebook(
            metadata=metadata,
            nbformat=properties.get("nbformat", 4),
            nbformat_minor=properties.get("nbformat_minor", 2),
            cells=cells,
            folder=folder,
        )

        # Create the NotebookResource with the Notebook object
        return NotebookResource(name=notebook_name, properties=notebook)

    def _convert_from_notebook_resource(self, notebook: NotebookResource) -> Dict[str, Any]:
        """Convert NotebookResource to dictionary format"""
        display_name = notebook.name
        if "/" in display_name:
            display_name = display_name.split("/")[-1]

        return {
            "displayName": display_name,
            "type": "Notebook",
            "properties": {
                "cells": (
                    [
                        {
                            "cell_type": getattr(cell, "cell_type", "code"),
                            "source": getattr(cell, "source", []),
                        }
                        for cell in notebook.properties.cells
                    ]
                    if hasattr(notebook.properties, "cells")
                    else []
                )
            },
        }

    def is_interactive_session(self) -> bool:
        """Check if running in an interactive session"""
        if "get_ipython" not in globals():
            try:
                from IPython import get_ipython
            except ImportError:
                return False
        else:
            get_ipython = globals()["get_ipython"]

        ipython = get_ipython()
        if ipython is None:
            return False

        try:
            connection_file = ipython.config.get("IPKernelApp", {}).get("connection_file")
            return connection_file is not None
        except:
            return False

    def get_workspace_info(self) -> Dict[str, Any]:
        """Get workspace information"""
        try:
            mssparkutils = _get_mssparkutils()
            if mssparkutils:
                return {
                    "workspace_id": getattr(
                        mssparkutils.env, "getWorkspaceId", lambda: "unknown"
                    )(),
                    "workspace_name": getattr(
                        mssparkutils.env, "getWorkspaceName", lambda: "unknown"
                    )(),
                    "environment": "synapse",
                }
        except Exception:
            pass

        return {"workspace_url": self._base_url, "environment": "synapse"}

    def get_cluster_info(self) -> Dict[str, Any]:
        """Get cluster information"""
        try:
            import __main__

            spark = getattr(__main__, "spark", None)
            if spark:
                return {
                    "app_name": spark.sparkContext.appName,
                    "spark_version": spark.version,
                    "master": spark.sparkContext.master,
                    "environment": "synapse",
                }
        except Exception:
            pass

        return {"environment": "synapse"}

    def deploy_spark_job(
        self, app_files: Dict[str, str], job_config: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Deploy application as Spark batch job on Synapse

        Uploads files to workspace storage and creates a Spark batch job.

        Args:
            app_files: Dictionary of {filename: content} to deploy
            job_config: Configuration including:
                - job_name: Display name for the job
                - spark_pool_name: Spark pool to use
                - entry_point: Main Python file (default: kindling_bootstrap.py)
                - executor_size: Executor size (Small, Medium, Large)
                - executors: Number of executors

        Returns:
            Dictionary with: {job_id, deployment_path, metadata}
        """
        self.logger.info(f"Deploying Spark job to Synapse: {job_config.get('job_name')}")

        job_name = job_config["job_name"]
        spark_pool_name = job_config.get("spark_pool_name")
        entry_point = job_config.get("entry_point", "kindling_bootstrap.py")

        if not spark_pool_name:
            raise ValueError("spark_pool_name is required for Synapse job deployment")

        # Step 1: Upload files to workspace storage
        self.logger.debug(f"Uploading {len(app_files)} files to workspace storage...")
        deployment_path = f"abfss://{self.workspace_name}@{self.storage_account}.dfs.core.windows.net/kindling_jobs/{job_name}"
        storage_paths = self._upload_files_to_storage(deployment_path, app_files)
        self.logger.debug(f"Uploaded files to: {deployment_path}")

        # Step 2: Create Spark batch job definition
        self.logger.debug("Creating Spark batch job...")
        job = self._create_synapse_spark_batch(job_name, entry_point, deployment_path, job_config)
        job_id = job_name  # Synapse uses name as identifier
        self.logger.debug(f"Created job: {job_id}")

        return {
            "job_id": job_id,
            "deployment_path": deployment_path,
            "metadata": {
                "job_name": job_name,
                "spark_pool_name": spark_pool_name,
                "entry_point": entry_point,
                "files": list(storage_paths.keys()),
            },
        }

    def run_spark_job(self, job_id: str, parameters: Dict[str, Any] = None) -> str:
        """Execute a Spark batch job on Synapse

        Args:
            job_id: Job name (Synapse uses names as identifiers)
            parameters: Optional runtime parameters

        Returns:
            Batch ID for monitoring
        """
        self.logger.info(f"Running Synapse Spark batch: {job_id}")

        # Get the job definition
        spark_job_def = self.client.spark_job_definition.get_spark_job_definition(job_id)

        # Submit batch job
        batch_request = {
            "name": f"{job_id}_{int(time.time())}",
            "file": spark_job_def.properties.job_properties.file,
            "args": list(parameters.values()) if parameters else [],
            "className": spark_job_def.properties.job_properties.class_name or "",
            "conf": spark_job_def.properties.job_properties.conf or {},
        }

        batch = self.client.spark_batch.create_spark_batch_job(batch_request)
        batch_id = str(batch.id)

        self.logger.info(f"Batch job started with batch_id: {batch_id}")
        return batch_id

    def get_job_status(self, run_id: str) -> Dict[str, Any]:
        """Get status of a running Spark batch job

        Args:
            run_id: Batch ID from run_spark_job()

        Returns:
            Dictionary with: {status, start_time, end_time, error, logs}
        """
        batch = self.client.spark_batch.get_spark_batch_job(int(run_id))

        return {
            "status": batch.state,
            "start_time": (
                batch.scheduler_state.get("submittedAt") if batch.scheduler_state else None
            ),
            "end_time": batch.scheduler_state.get("endedAt") if batch.scheduler_state else None,
            "error": batch.errors[0] if batch.errors else None,
            "logs": batch.log_lines,
        }

    def cancel_job(self, run_id: str) -> bool:
        """Cancel a running Spark batch job

        Args:
            run_id: Batch ID to cancel

        Returns:
            True if cancelled successfully
        """
        self.logger.info(f"Cancelling Synapse batch: {run_id}")

        try:
            self.client.spark_batch.cancel_spark_batch_job(int(run_id))
            self.logger.info(f"Batch cancelled: {run_id}")
            return True
        except Exception as e:
            self.logger.error(f"Failed to cancel batch: {e}")
            return False

    def _upload_files_to_storage(
        self, deployment_path: str, app_files: Dict[str, str]
    ) -> Dict[str, str]:
        """Upload files to Synapse workspace storage

        Returns:
            Dictionary mapping filename to storage path
        """
        storage_paths = {}

        # Use mssparkutils to write files
        for filename, content in app_files.items():
            file_path = f"{deployment_path}/{filename}"
            mssparkutils.fs.put(file_path, content, True)
            storage_paths[filename] = file_path

        return storage_paths

    def _create_synapse_spark_batch(
        self, job_name: str, entry_point: str, deployment_path: str, job_config: Dict[str, Any]
    ) -> Any:
        """Create a Synapse Spark Job Definition"""

        spark_pool_name = job_config["spark_pool_name"]

        # Build job definition
        job_properties = SparkJobProperties(
            file=f"{deployment_path}/{entry_point}",
            class_name="",
            args=job_config.get("args", []),
            jars=[],
            py_files=[f"{deployment_path}/{f}" for f in job_config.get("additional_files", [])],
            files=[],
            archives=[],
            conf={
                "spark.dynamicAllocation.enabled": "false",
                "spark.dynamicAllocation.minExecutors": str(job_config.get("executors", 2)),
                "spark.dynamicAllocation.maxExecutors": str(job_config.get("executors", 2)),
            },
            num_executors=job_config.get("executors", 2),
            executor_cores=job_config.get("executor_cores", 4),
            executor_memory=job_config.get("executor_memory", "28g"),
            driver_cores=job_config.get("driver_cores", 4),
            driver_memory=job_config.get("driver_memory", "28g"),
        )

        job_definition = SparkJobDefinition(
            properties=SparkJobDefinitionResource(
                name=job_name,
                target_big_data_pool=BigDataPoolReference(
                    type="BigDataPoolReference", reference_name=spark_pool_name
                ),
                required_spark_version="3.3",
                language="Python",
                job_properties=job_properties,
            )
        )

        # Create or update the job definition
        result = self.client.spark_job_definition.create_or_update_spark_job_definition(
            job_name, job_definition
        )

        return result


@PlatformServices.register(name="synapse", description="Synapse platform service")
def create_platform_service(config, logger):
    return SynapseService(config, logger)


# ============================================================================
# Synapse REST API Client (for remote operations)
# ============================================================================


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
        >>> job = api.create_spark_job("my-job", config)
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
        self._min_request_interval = 0.6  # 600ms between requests = ~1.67 req/sec (safe margin)

    def get_platform_name(self) -> str:
        """Get platform name"""
        return "synapse"

    def deploy_spark_job(
        self, app_files: Dict[str, str], job_config: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Deploy application as Spark job (convenience method)

        Combines create_spark_job + upload_files + update_job_files into one call.

        Args:
            app_files: Dictionary of {filename: content} to deploy
            job_config: Platform-specific job configuration (must include spark_pool_name)

        Returns:
            Dictionary with job_id, deployment_path, and metadata
        """
        job_name = job_config["job_name"]

        # Step 1: Create job definition
        result = self.create_spark_job(job_name, job_config)
        job_id = result["job_id"]

        # Step 2: Upload files
        app_name = job_config.get("app_name", job_name)
        target_path = f"data-apps/{app_name}"
        deployment_path = self.upload_files(app_files, target_path)

        # Step 3: Update job with file paths
        self.update_job_files(job_id, deployment_path)

        return {
            "job_id": job_id,
            "deployment_path": deployment_path,
            "metadata": {
                "job_name": job_name,
                "app_name": app_name,
                "files_count": len(app_files),
            },
        }

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

    def create_spark_job(self, job_name: str, job_config: Dict[str, Any]) -> Dict[str, Any]:
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

    def upload_files(self, files: Dict[str, str], target_path: str) -> str:
        """Upload files to ADLS Gen2

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

    def update_job_files(self, job_id: str, files_path: str) -> None:
        """Update job definition with file paths

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
            job_id: Job ID to run (from create_spark_job)
            parameters: Optional runtime parameters

        Returns:
            Batch ID (run ID) for monitoring
        """
        if job_id not in self._job_mapping:
            raise ValueError(f"Job {job_id} not found. Call create_spark_job first.")

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
