import json
import os
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


class SynapseTokenCredential(TokenCredential):
    """Token credential for Synapse using mssparkutils"""

    def __init__(self, expires_on=None):
        mssparkutils = _get_mssparkutils()
        # Use simple "Synapse" audience - NOT a URL!
        self.token = mssparkutils.credentials.getToken("Synapse")
        self.expires_on = expires_on or (time.time() + 3600)

    def get_token(self, *scopes, **kwargs):
        return AccessToken(self.token, int(self.expires_on))


class SynapseService(PlatformService):
    def __init__(self, config, logger):
        self.config = config
        self.logger = logger
        self._base_url = self._build_base_url()

        # Initialize credential using SynapseTokenCredential
        self.credential = SynapseTokenCredential()

        # Initialize Synapse client
        from azure.synapse.artifacts import ArtifactsClient

        self.client = ArtifactsClient(endpoint=self._base_url, credential=self.credential)

        # Cache system
        self._token_cache = {}
        self._items_cache = []
        self._folders_cache = {}
        self._notebooks_cache = []
        self._cache_initialized = False

        try:
            self._initialize_cache()
        except Exception as e:
            self.logger.error(f"Cache initialization failed: {e}")
            import traceback

            traceback.print_exc()
            raise

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
                properties = notebook.get("properties", {})
                if "folder" in properties:
                    folder_info = properties["folder"]
                    if isinstance(folder_info, dict) and "name" in folder_info:
                        folder_name = folder_info["name"]
                        if folder_name:
                            self._folders_cache[folder_name] = folder_name

            self._items_cache = self._notebooks_cache.copy()
            self._cache_initialized = True

            self.logger.debug(
                f"Cache initialized: {len(self._notebooks_cache)} notebooks, {len(self._folders_cache)} folders"
            )

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

    def _config_get(self, key: str, default=None):
        try:
            if hasattr(self.config, "get"):
                return self.config.get(key, default)
        except Exception:
            pass
        if isinstance(self.config, dict):
            return self.config.get(key, default)
        return getattr(self.config, key, default)

    def get_token(self, audience: str) -> str:
        """Get access token for the specified audience"""
        token = self.credential.get_token(audience)
        return token.token

    def get_secret(self, secret_name: str, default: Optional[str] = None) -> str:
        """Resolve secret using Synapse notebook credentials API."""
        import os

        mssparkutils = _get_mssparkutils()
        linked_service = self._config_get("kindling.secrets.linked_service")
        key_vault_url = self._config_get("kindling.secrets.key_vault_url")

        last_error = None
        if linked_service:
            try:
                return mssparkutils.credentials.getSecret(linked_service, secret_name)
            except Exception as exc:
                last_error = exc
        if key_vault_url:
            try:
                return mssparkutils.credentials.getSecret(key_vault_url, secret_name)
            except Exception as exc:
                last_error = exc

        # Fallback for local/testing contexts where Key Vault integration isn't configured.
        env_key = secret_name.upper().replace("-", "_").replace(".", "_").replace(":", "_")
        for candidate in [secret_name, env_key, f"KINDLING_SECRET_{env_key}"]:
            value = os.getenv(candidate)
            if value is not None:
                return value

        if default is not None:
            return default
        if last_error is not None:
            raise KeyError(
                f"Failed to resolve Synapse secret '{secret_name}' "
                f"(linked_service={linked_service}, key_vault_url={key_vault_url}): {last_error}"
            ) from last_error

        raise KeyError(
            f"Synapse secret not found: {secret_name}. Configure kindling.secrets.linked_service "
            "or kindling.secrets.key_vault_url, or set environment fallback."
        )

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

        spark_pool_name = job_config.get("spark_pool_name", self.spark_pool_name)

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
