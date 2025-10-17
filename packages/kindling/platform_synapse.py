from kindling.spark_session import *
from kindling.notebook_framework import *
from kindling.data_apps import AppDeploymentService
import time
import json
import types
from typing import Dict, List, Optional, Any, Union
from datetime import datetime, timedelta
from urllib.parse import quote

add_to_registry = False
mssparkutils = None

try:
    from azure.core.exceptions import *
    from azure.synapse.artifacts import ArtifactsClient
    from azure.synapse.artifacts.models import *
    from azure.core.credentials import TokenCredential, AccessToken
    from notebookutils import mssparkutils

    add_to_registry = True
except Exception as e:
    print(f"Unable to import azure synapse libraries: {e}")
    print("Synapse will not be available as a platform for this session")
    add_to_registry = False

    class TokenCredential:
        def __init__():
            pass


# Always available mock/test version of SynapseAppDeploymentService
class MockSynapseAppDeploymentService(AppDeploymentService):
    """Mock implementation of SynapseAppDeploymentService for testing"""

    def __init__(self, synapse_service=None, logger=None):
        self.synapse_service = synapse_service
        self.logger = logger or self._create_mock_logger()

    def _create_mock_logger(self):
        class MockLogger:
            def info(self, msg):
                print(f"ℹ️  {msg}")

            def error(self, msg):
                print(f"❌ {msg}")

            def warning(self, msg):
                print(f"⚠️  {msg}")

        return MockLogger()

    def submit_spark_job(self, job_config: Dict[str, Any]) -> Dict[str, Any]:
        """Mock implementation that simulates job submission"""
        script_path = job_config.get("script_path")
        app_name = job_config.get("app_name", "unknown-app")

        self.logger.info(f"Mock: Executing Synapse job for app: {app_name}")

        # Generate job ID
        import time

        job_id = f"mock-synapse-{app_name}-{int(time.time())}"

        try:
            # Validate script exists and is syntactically correct
            if script_path and os.path.exists(script_path):
                with open(script_path, "r") as f:
                    script_content = f.read()
                compile(script_content, script_path, "exec")
                status = "SUCCEEDED"
                message = f"Mock job {job_id} completed successfully"
            else:
                status = "FAILED"
                message = f"Script not found: {script_path}"

        except Exception as e:
            status = "FAILED"
            message = f"Mock job execution failed: {str(e)}"

        return {
            "job_id": job_id,
            "status": status,
            "message": message,
            "submission_time": time.time(),
            "mock": True,
        }

    def get_job_status(self, job_id: str) -> Dict[str, Any]:
        return {"job_id": job_id, "status": "COMPLETED", "message": "Mock job status", "mock": True}

    def cancel_job(self, job_id: str) -> bool:
        self.logger.info(f"Mock: Cancelled job {job_id}")
        return True

    def get_job_logs(self, job_id: str) -> str:
        return f"Mock logs for {job_id}"

    def create_job_config(
        self, app_name: str, app_path: str, environment_vars: Optional[Dict[str, str]] = None
    ) -> Dict[str, Any]:
        main_script = f"{app_path}/main.py"
        return {
            "app_name": app_name,
            "script_path": main_script,
            "environment_vars": environment_vars or {},
            "platform": "synapse",
            "execution_mode": "mock",
            "mock": True,
        }


# Export the appropriate class based on environment
if not add_to_registry:
    # Testing/development environment - use mock
    SynapseAppDeploymentService = MockSynapseAppDeploymentService


if add_to_registry:

    class SynapseTokenCredential(TokenCredential):
        def __init__(self, expires_on=None):
            token = mssparkutils.credentials.getToken("Synapse")
            self.token = token
            self.expires_on = expires_on or (time.time() + 3600)

        def get_token(self, *scopes, **kwargs):
            return AccessToken(self.token, self.expires_on)

    class SynapseAppDeploymentService(AppDeploymentService):
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
                import __main__

                mssparkutils = getattr(__main__, "mssparkutils", None)

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
            self.credential = SynapseTokenCredential()
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

    def get_token(self, audience: str) -> str:
        """Get access token for the specified audience"""
        token = self.credential.get_token(audience)
        return token.token

    def exists(self, path: str) -> bool:
        """Check if a file exists"""
        try:
            # For Synapse, we'll use mssparkutils if available
            import __main__

            mssparkutils = getattr(__main__, "mssparkutils", None)
            if mssparkutils:
                return mssparkutils.fs.exists(path)

            # Fallback: check if it's a notebook by name
            return self.get_notebook_id_by_name(path) is not None
        except:
            return False

    def copy(self, source: str, destination: str, overwrite: bool = False) -> None:
        """Copy a file from source to destination"""
        import __main__

        mssparkutils = getattr(__main__, "mssparkutils", None)
        if mssparkutils:
            mssparkutils.fs.cp(source, destination, overwrite)
        else:
            raise NotImplementedError("File copy not available without mssparkutils")

    def delete(self, path: str, recurse=False) -> None:
        import __main__

        mssparkutils = getattr(__main__, "mssparkutils", None)
        if mssparkutils:
            mssparkutils.fs.rm(path, recurse)
        else:
            raise NotImplementedError("File delete not available without mssparkutils")

    def read(self, path: str, encoding: str = "utf-8") -> Union[str, bytes]:
        """Read file content"""
        with open(path, "r" if encoding else "rb") as f:
            return f.read()

    def move(self, path: str, dest: str) -> Union[str, bytes]:
        if mssparkutils:
            mssparkutils.fs.mv(path, dest)
        else:
            raise NotImplementedError("File delete not available without mssparkutils")

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
        self._token_cache = {"token": token_response.token, "expires_at": token_response.expires_on}

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
                "properties": result.properties.__dict__ if hasattr(result, "properties") else {},
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
    ) -> NotebookResource:
        """Convert Synapse notebook to NotebookResource with safe metadata handling"""

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

        return NotebookResource(**notebook_data)

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
        """Convert notebook data to Synapse NotebookResource"""

        # Extract properties from notebook_data
        properties = notebook_data.get("properties", {})

        # Create notebook properties
        notebook_props = {
            "cells": [],
            "metadata": self._convert_metadata_to_dict(properties.get("metadata", {})),
            "nbformat": properties.get("nbformat", 4),
            "nbformat_minor": properties.get("nbformat_minor", 2),
        }

        # Extract cells
        if "cells" in properties:
            cells = properties["cells"]
            if isinstance(cells, list):
                for cell in cells:
                    if isinstance(cell, dict):
                        cell_copy = cell.copy()
                        if "metadata" not in cell_copy or cell_copy["metadata"] is None:
                            cell_copy["metadata"] = {}
                        notebook_props["cells"].append(cell_copy)

        # Handle folder information
        folder_info = properties.get("folder", {})
        if folder_info and isinstance(folder_info, dict):
            notebook_props["folder"] = folder_info

        # Create the notebook resource
        return NotebookResource(name=notebook_name, properties=notebook_props)

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
            import __main__

            mssparkutils = getattr(__main__, "mssparkutils", None)
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


if add_to_registry:

    @PlatformServices.register(name="synapse", description="Synapse platform service")
    def create_platform_service(config, logger):
        return SynapseService(config, logger)
