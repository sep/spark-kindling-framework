import json
import os
import sys
import tempfile
import time
import types
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

# Import the base PlatformService
from kindling.notebook_framework import *


class StandaloneService(PlatformService):
    """
    Standalone platform implementation for vanilla/OSS Spark deployments.

    This service provides a generic Spark platform without vendor-specific APIs,
    working with any Spark deployment (local, standalone, YARN, Kubernetes, etc.)

    Key Features:
    - Hybrid filesystem support: Hadoop FS (HDFS/S3/ABFS) + local filesystem
    - Works with or without active Spark session
    - No dependencies on Databricks/Synapse/Fabric-specific APIs
    - Compatible with any Spark deployment model

    Use cases:
    - Local development and testing
    - Standalone Spark clusters
    - YARN/Kubernetes Spark deployments
    - CI/CD pipelines
    - Generic Spark applications
    """

    def __init__(self, config, logger):
        self.config = (
            config if isinstance(config, types.SimpleNamespace) else types.SimpleNamespace(**config)
        )
        self.logger = logger

        # Standalone platform attributes
        self.workspace_id = None
        self.workspace_url = "http://localhost"

        # Set up local paths - use temp directory by default to avoid polluting project directory
        default_workspace = Path(tempfile.gettempdir()) / "kindling_standalone_workspace"
        self.local_workspace_path = Path(
            getattr(self.config, "local_workspace_path", str(default_workspace))
        )
        self.local_workspace_path.mkdir(parents=True, exist_ok=True)

        # Hadoop FileSystem support (for HDFS/S3/ABFS when Spark is available)
        self._hadoop_fs = None
        self._spark_available = False
        self._try_initialize_hadoop_fs()

        # Cache system for consistency with other platforms
        self._notebooks_cache = []
        self._folders_cache = {}
        self._items_cache = []
        self._cache_initialized = False

        self._initialize_cache()

    def _try_initialize_hadoop_fs(self):
        """Try to initialize Hadoop FileSystem if Spark is available"""
        try:
            from pyspark.sql import SparkSession

            spark = SparkSession.getActiveSession()

            if spark:
                self._spark_available = True
                self.logger.debug("Spark session detected - Hadoop FileSystem support enabled")
                # Hadoop FS will be initialized per-operation as needed
            else:
                self.logger.debug("No active Spark session - using local filesystem only")
        except Exception as e:
            self.logger.debug(f"Hadoop FileSystem not available: {e}")
            self._spark_available = False

    def _get_hadoop_fs(self, path: str):
        """Get Hadoop FileSystem for the given path"""
        try:
            from py4j.java_gateway import java_import
            from pyspark.sql import SparkSession

            spark = SparkSession.getActiveSession()
            if not spark:
                return None

            # Import Hadoop classes
            java_import(spark._jvm, "org.apache.hadoop.fs.FileSystem")
            java_import(spark._jvm, "org.apache.hadoop.fs.Path")

            # Get FileSystem for the path
            hadoop_path = spark._jvm.Path(path)
            fs = spark._jvm.FileSystem.get(hadoop_path.toUri(), spark._jsc.hadoopConfiguration())
            return fs, spark._jvm
        except Exception as e:
            self.logger.debug(f"Could not get Hadoop FileSystem: {e}")
            return None

    def _is_distributed_path(self, path: str) -> bool:
        """Check if path is for distributed storage (HDFS/S3/ABFS/GCS)"""
        return path.startswith(
            (
                "hdfs://",
                "s3://",
                "s3a://",
                "s3n://",
                "abfs://",
                "abfss://",
                "gs://",
                "wasb://",
                "wasbs://",
            )
        )

    def _initialize_cache(self):
        """Initialize cache with local filesystem content"""
        try:
            self.logger.debug("Initializing local workspace cache...")

            # Scan local workspace for notebook files
            self._notebooks_cache = []
            if self.local_workspace_path.exists():
                for notebook_file in self.local_workspace_path.rglob("*.py"):
                    self._notebooks_cache.append(
                        {
                            "id": notebook_file.stem,
                            "displayName": notebook_file.stem,
                            "type": "notebook",
                            "path": str(notebook_file),
                            "etag": None,
                            "properties": {
                                "folder": {
                                    "name": (
                                        notebook_file.parent.name
                                        if notebook_file.parent != self.local_workspace_path
                                        else None
                                    )
                                }
                            },
                        }
                    )

            # Build folders cache
            self._folders_cache = {}
            for notebook in self._notebooks_cache:
                folder_info = notebook.get("properties", {}).get("folder", {})
                if folder_info and folder_info.get("name"):
                    folder_name = folder_info["name"]
                    self._folders_cache[folder_name] = folder_name

            self._items_cache = self._notebooks_cache.copy()
            self._cache_initialized = True

            self.logger.debug(f"Local cache initialized with {len(self._items_cache)} items")
            self.logger.debug(f"Found {len(self._notebooks_cache)} notebooks")
            self.logger.debug(f"Found {len(self._folders_cache)} folders")

        except Exception as e:
            self.logger.warning(f"Failed to initialize local cache: {e}")
            self._cache_initialized = False

    def initialize(self):
        """Initialize the service"""
        if not self._cache_initialized:
            self._initialize_cache()
        return True

    def get_platform_name(self):
        """Return platform name"""
        return "standalone"

    def get_workspace_id(self):
        """Get workspace ID (None for local)"""
        return None

    def get_workspace_url(self):
        """Get workspace URL (localhost for local)"""
        return "http://localhost"

    def get_environment(self):
        """Get environment from environment variables"""
        return os.environ.get("PROJECT_ENV", os.environ.get("ENVIRONMENT", "standalone"))

    def get_token(self, audience: str = None) -> str:
        """Get access token (not needed for standalone - returns dummy token)"""
        self.logger.debug(f"Standalone platform does not use real tokens. Audience: {audience}")
        return "standalone-dummy-token"

    def get_secret(self, secret_name: str, default: Optional[str] = None) -> str:
        """Resolve secret from environment variables in standalone mode."""
        env_key = secret_name.upper().replace("-", "_").replace(".", "_").replace(":", "_")
        candidates = [secret_name, env_key, f"KINDLING_SECRET_{env_key}"]

        for key in candidates:
            value = os.getenv(key)
            if value is not None:
                return value

        if default is not None:
            return default

        raise KeyError(f"Secret not found in environment: {secret_name}")

    def exists(self, path: str) -> bool:
        """Check if a file exists (supports local and distributed storage)"""
        try:
            # Use Hadoop FS for distributed paths if Spark is available
            if self._is_distributed_path(path) and self._spark_available:
                result = self._get_hadoop_fs(path)
                if result:
                    fs, jvm = result
                    hadoop_path = jvm.Path(path)
                    return fs.exists(hadoop_path)

            # Fall back to local filesystem
            if Path(path).is_absolute():
                return Path(path).exists()
            else:
                local_path = self.local_workspace_path / path
                return local_path.exists()
        except Exception as e:
            self.logger.debug(f"Error checking if path exists: {e}")
            return False

    def copy(self, source: str, destination: str, overwrite: bool = False) -> None:
        """Copy a file (supports local and distributed storage)"""
        import shutil

        # Use Hadoop FS for distributed paths
        if (
            self._is_distributed_path(source) or self._is_distributed_path(destination)
        ) and self._spark_available:
            result = self._get_hadoop_fs(
                source if self._is_distributed_path(source) else destination
            )
            if result:
                fs, jvm = result
                src_path = jvm.Path(source)
                dst_path = jvm.Path(destination)

                if not overwrite and fs.exists(dst_path):
                    raise FileExistsError(f"Destination {destination} already exists")

                fs.copyFromLocalFile(False, overwrite, src_path, dst_path)
                return

        # Fall back to local filesystem
        source_path = (
            Path(source) if Path(source).is_absolute() else self.local_workspace_path / source
        )
        dest_path = (
            Path(destination)
            if Path(destination).is_absolute()
            else self.local_workspace_path / destination
        )

        if not overwrite and dest_path.exists():
            raise FileExistsError(f"Destination {destination} already exists")

        # Ensure destination directory exists
        dest_path.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source_path, dest_path)

    def delete(self, path: str, recurse: bool = False) -> None:
        """Delete a file or directory (supports local and distributed storage)"""
        import shutil

        # Use Hadoop FS for distributed paths
        if self._is_distributed_path(path) and self._spark_available:
            result = self._get_hadoop_fs(path)
            if result:
                fs, jvm = result
                hadoop_path = jvm.Path(path)
                if fs.exists(hadoop_path):
                    fs.delete(hadoop_path, recurse)
                return

        # Fall back to local filesystem
        target_path = Path(path) if Path(path).is_absolute() else self.local_workspace_path / path

        if not target_path.exists():
            self.logger.debug(f"Path does not exist: {target_path}")
            return

        if target_path.is_file():
            target_path.unlink()
        elif target_path.is_dir() and recurse:
            shutil.rmtree(target_path)
        elif target_path.is_dir():
            target_path.rmdir()

    def read(self, path: str, encoding: str = "utf-8") -> Union[str, bytes]:
        """Read file content locally"""
        file_path = Path(path) if Path(path).is_absolute() else self.local_workspace_path / path
        mode = "r" if encoding else "rb"

        with open(file_path, mode, encoding=encoding if encoding else None) as f:
            return f.read()

    def write(self, path: str, content: Union[str, bytes], overwrite: bool = False) -> None:
        """Write content to local file"""
        file_path = Path(path) if Path(path).is_absolute() else self.local_workspace_path / path

        if not overwrite and file_path.exists():
            raise FileExistsError(f"File {path} already exists")

        # Ensure parent directory exists
        file_path.parent.mkdir(parents=True, exist_ok=True)
        mode = "w" if isinstance(content, str) else "wb"

        with open(file_path, mode) as f:
            f.write(content)

    def move(self, source: str, destination: str) -> None:
        """Move a file locally"""
        import shutil

        source_path = (
            Path(source) if Path(source).is_absolute() else self.local_workspace_path / source
        )
        dest_path = (
            Path(destination)
            if Path(destination).is_absolute()
            else self.local_workspace_path / destination
        )

        # Ensure destination directory exists
        dest_path.parent.mkdir(parents=True, exist_ok=True)
        shutil.move(str(source_path), str(dest_path))

    def list(self, path: str) -> List[str]:
        """List files in directory (supports local and distributed storage)"""
        # Use Hadoop FS for distributed paths
        if self._is_distributed_path(path) and self._spark_available:
            result = self._get_hadoop_fs(path)
            if result:
                fs, jvm = result
                hadoop_path = jvm.Path(path)
                if fs.exists(hadoop_path) and fs.isDirectory(hadoop_path):
                    status_list = fs.listStatus(hadoop_path)
                    return [status.getPath().getName() for status in status_list]
                return []

        # Fall back to local filesystem
        dir_path = Path(path) if Path(path).is_absolute() else self.local_workspace_path / path

        if dir_path.is_dir():
            return [item.name for item in dir_path.iterdir()]
        else:
            self.logger.debug(f"Path is not a directory: {dir_path}")
            return []

    def list_notebooks(self) -> List[Dict[str, Any]]:
        """List all notebooks in local workspace"""
        return self._notebooks_cache.copy()

    def get_notebook_id_by_name(self, notebook_name: str) -> Optional[str]:
        """Get notebook ID by name"""
        for notebook in self._notebooks_cache:
            if notebook["displayName"] == notebook_name:
                return notebook["id"]
        return None

    def get_notebooks(self):
        """Get all notebooks as NotebookResource objects"""
        return [self._convert_to_notebook_resource(notebook) for notebook in self._notebooks_cache]

    def get_notebook(self, notebook_name: str, include_content: bool = True):
        """Get a specific notebook"""
        try:
            for notebook in self._notebooks_cache:
                if notebook["displayName"] == notebook_name:
                    return self._convert_to_notebook_resource(notebook, include_content)
            return None
        except Exception as e:
            self.logger.error(f"Error getting notebook {notebook_name}: {e}")
            return None

    def _convert_to_notebook_resource(
        self, local_data: Dict[str, Any], include_content: bool = True
    ):
        """Convert local notebook data to NotebookResource format"""
        notebook_data = {
            "id": local_data["id"],
            # NotebookResource uses 'name' not 'displayName'
            "name": local_data["displayName"],
            "type": "notebook",
            "etag": local_data.get("etag"),
            "properties": {
                "folder": local_data.get("properties", {}).get("folder"),
                "metadata": {},
                "nbformat": 4,
                "nbformat_minor": 2,
            },
        }

        if include_content:
            # Read content from local file
            try:
                file_path = local_data.get("path")
                if file_path and Path(file_path).exists():
                    with open(file_path, "r", encoding="utf-8") as f:
                        content = f.read()

                    # Convert Python file to notebook format
                    notebook_data["properties"]["cells"] = self._convert_python_to_cells(content)
                else:
                    notebook_data["properties"]["cells"] = []
            except Exception as e:
                self.logger.warning(f"Could not read notebook content: {e}")
                notebook_data["properties"]["cells"] = []

        return NotebookResource(**notebook_data)

    def _convert_python_to_cells(self, python_content: str) -> List[Dict[str, Any]]:
        """Convert Python file content to notebook cells"""
        cells = []
        lines = python_content.split("\n")
        current_cell = []
        cell_type = "code"

        for line in lines:
            if line.startswith("# MAGIC %md"):
                # Save current cell if it has content
                if current_cell:
                    cells.append(
                        {
                            "cell_type": cell_type,
                            "source": "\n".join(current_cell),
                            "metadata": {},
                            "outputs": [] if cell_type == "code" else None,
                            "execution_count": None if cell_type == "code" else None,
                        }
                    )
                    current_cell = []
                cell_type = "markdown"
            elif line.startswith("# COMMAND ----------"):
                # Save current cell and start new code cell
                if current_cell:
                    cells.append(
                        {
                            "cell_type": cell_type,
                            "source": "\n".join(current_cell),
                            "metadata": {},
                            "outputs": [] if cell_type == "code" else None,
                            "execution_count": None if cell_type == "code" else None,
                        }
                    )
                    current_cell = []
                cell_type = "code"
            else:
                # Remove MAGIC prefix for markdown
                if cell_type == "markdown" and line.startswith("# MAGIC "):
                    line = line[8:]  # Remove '# MAGIC '
                current_cell.append(line)

        # Save final cell
        if current_cell:
            cells.append(
                {
                    "cell_type": cell_type,
                    "source": "\n".join(current_cell),
                    "metadata": {},
                    "outputs": [] if cell_type == "code" else None,
                    "execution_count": None if cell_type == "code" else None,
                }
            )

        return cells

    def create_notebook(self, notebook_name: str, content: str = None) -> bool:
        """Create a new notebook locally"""
        try:
            notebook_path = self.local_workspace_path / f"{notebook_name}.py"

            if notebook_path.exists():
                return False  # Already exists
        except:
            pass

    def update_notebook(self, notebook_name: str, content: str) -> bool:
        """Update an existing notebook locally"""
        try:
            notebook_path = self.local_workspace_path / f"{notebook_name}.py"

            with open(notebook_path, "w", encoding="utf-8") as f:
                f.write(content)

            return True

        except Exception as e:
            self.logger.error(f"Failed to update notebook {notebook_name}: {e}")
            return False

    def delete_notebook(self, notebook_name: str) -> bool:
        """Delete a notebook locally"""
        try:
            notebook_path = self.local_workspace_path / f"{notebook_name}.py"

            if notebook_path.exists():
                notebook_path.unlink()

                # Update cache
                self._initialize_cache()

                return True
            return False

        except Exception as e:
            self.logger.error(f"Failed to delete notebook {notebook_name}: {e}")
            return False

    def create_or_update_notebook(
        self, notebook_name: str, notebook_data: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Create or update a notebook (compatibility method)"""
        try:
            notebook_path = self.local_workspace_path / f"{notebook_name}.py"

            # Extract content from notebook_data
            content = ""
            if isinstance(notebook_data, dict):
                if "content" in notebook_data:
                    content = notebook_data["content"]
                elif "properties" in notebook_data:
                    props = notebook_data["properties"]
                    if "cells" in props:
                        # Convert cells to Python content
                        content = self._convert_cells_to_python(props["cells"])

            # Write the content
            with open(notebook_path, "w", encoding="utf-8") as f:
                f.write(content)

            # Update cache
            self._initialize_cache()

            return {
                "id": notebook_name,
                "displayName": notebook_name,
                "type": "notebook",
                "path": str(notebook_path),
            }

        except Exception as e:
            self.logger.error(f"Failed to create/update notebook {notebook_name}: {e}")
            raise

    def _convert_cells_to_python(self, cells: List[Dict[str, Any]]) -> str:
        """Convert notebook cells to Python file format"""
        lines = []

        for cell in cells:
            cell_type = cell.get("cell_type", "code")
            source = cell.get("source", [])

            if isinstance(source, list):
                source = "\n".join(source)

            if cell_type == "markdown":
                # Convert markdown to commented lines
                for line in source.split("\n"):
                    lines.append(f"# MAGIC %md {line}")
            else:
                # Code cell
                if lines:  # Add separator if not first cell
                    lines.append("# COMMAND ----------")
                lines.append(source)

        return "\n".join(lines)

    def get_folder_name(self, folder_id: str) -> str:
        """Get folder name from folder ID (in local, folder_id is the folder name)"""
        return folder_id if folder_id else ""

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
        """Get local workspace information"""
        return {
            "workspace_id": None,
            "workspace_url": self.workspace_url,
            "workspace_path": str(self.local_workspace_path),
            "environment": self.get_environment(),
            "platform": "standalone",
        }

    def get_cluster_info(self) -> Dict[str, Any]:
        """Get local Spark cluster information"""
        try:
            # Try to get Spark session from global scope
            import __main__

            spark = getattr(__main__, "spark", None)

            if spark:
                return {
                    "app_name": spark.sparkContext.appName,
                    "spark_version": spark.version,
                    "master": spark.sparkContext.master,
                    "environment": self.get_environment(),
                    "executor_memory": spark.conf.get("spark.executor.memory", "unknown"),
                    "driver_memory": spark.conf.get("spark.driver.memory", "unknown"),
                }
        except Exception as e:
            self.logger.debug(f"Could not retrieve Spark info: {e}")

        return {"environment": self.get_environment(), "spark_available": False}

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


# Register the standalone platform service
@PlatformServices.register(
    name="standalone", description="Standalone/OSS Spark platform service for any Spark deployment"
)
def create_platform_service(config, logger):
    return StandaloneService(config, logger)
