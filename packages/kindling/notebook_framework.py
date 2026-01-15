import enum
import json
import os
import re
import shutil
import subprocess
import sys
import types
import uuid
from abc import ABC, abstractmethod
from dataclasses import dataclass, field, fields
from importlib.machinery import ModuleSpec
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Set, Tuple, Union

from kindling.injection import *
from kindling.platform_provider import *
from kindling.spark_config import *
from kindling.spark_log_provider import *
from kindling.spark_session import get_or_create_spark_session


def is_interactive_session(force_interactive: Optional[bool] = None) -> bool:
    """Check if running in interactive session"""
    if force_interactive is not None:
        return force_interactive

    try:
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
            if connection_file:
                return True
        except Exception:
            pass

        return False
    except Exception:
        return False


def notebook_import(notebook_ref: str) -> Optional[types.ModuleType]:
    is_interactive = is_interactive_session()

    if is_interactive:
        return None

    if "." in notebook_ref:
        parts = notebook_ref.split(".")
        notebook_name = parts[1]
        package_name = parts[0]
        module_name = notebook_ref
    else:
        notebook_name = notebook_ref
        package_name = None
        module_name = notebook_ref

    if module_name in sys.modules:
        module = sys.modules[module_name]

        import __main__

        imported_count = 0

        for name in dir(module):
            if not name.startswith("_"):
                setattr(__main__, name, getattr(module, name))
                imported_count += 1

        return module
    else:
        return None


# ═══════════════════════════════════════════════════════════════════════════
# Notebook Utilities
# ═══════════════════════════════════════════════════════════════════════════


class NotebookUtilities:
    """
    Utility class for notebook operations using platform APIs.

    Provides core notebook functionality without service-level dependency injection:
    - Extract code from notebooks via platform service
    - Scan notebook folders and packages
    - Process notebook cells and imports
    - Execute notebook code

    Used by both NotebookWheelBuilder and NotebookAppBuilder.
    """

    def __init__(self, platform_service, logger=None):
        """Initialize with platform service for notebook access

        Args:
            platform_service: Platform service for get_notebook(), list_notebooks(), etc.
            logger: Optional logger for debug output
        """
        self.platform_service = platform_service
        self.logger = logger

    def extract_code_from_notebook(
        self, notebook_name: str, suppress_nbimports: bool = False
    ) -> tuple[str, list[str]]:
        """Extract Python code from a notebook using platform APIs

        Args:
            notebook_name: Name/path of notebook
            suppress_nbimports: If True, don't process notebook_import() calls

        Returns:
            Tuple of (combined_code, list_of_imported_modules)
        """
        if self.logger:
            self.logger.debug(f"Extracting code from notebook: {notebook_name}")

        # Fetch notebook via platform API
        notebook = self.platform_service.get_notebook(notebook_name)

        # Extract code from cells
        code_blocks = []
        for cell in notebook.properties.cells:
            if cell.cell_type == "code":
                cell_code = self._extract_cell_code(cell)
                if cell_code.strip():
                    code_blocks.append(cell_code)

        # Process code blocks
        processed_blocks, imported_modules = self._process_code_blocks(
            code_blocks, suppress_nbimports
        )

        # Combine with cell separators
        final_code = "\n\n# ---- Next Cell ----\n\n".join(processed_blocks)

        return final_code, imported_modules

    def get_notebooks_in_folder(self, folder_path: str) -> list:
        """Get all notebooks in a folder using platform APIs

        Args:
            folder_path: Folder path to scan

        Returns:
            List of notebook objects in the folder
        """
        if self.logger:
            self.logger.debug(f"Scanning folder for notebooks: {folder_path}")

        # Use platform API to list notebooks
        all_notebooks = self.platform_service.get_notebooks()

        # Filter to notebooks in the target folder
        folder_notebooks = []
        for notebook in all_notebooks:
            if notebook.name and notebook.name.startswith(folder_path + "/"):
                # Check it's directly in this folder, not a subfolder
                relative_path = notebook.name[len(folder_path) + 1 :]
                if "/" not in relative_path:
                    folder_notebooks.append(notebook)

        return folder_notebooks

    def scan_for_notebook_packages(self) -> list[str]:
        """Scan workspace for notebook packages (folders with _init notebooks)

        Returns:
            List of package folder names
        """
        if self.logger:
            self.logger.debug("Scanning for notebook packages")

        notebooks = self.platform_service.get_notebooks()

        # Find notebooks ending with _init
        package_folders = set()
        for notebook in notebooks:
            if notebook.name and "_init" in notebook.name:
                # Extract folder name (everything before _init)
                folder = notebook.name.rsplit("_init", 1)[0].rstrip("/")
                if folder:
                    package_folders.add(folder)

        return sorted(package_folders)

    def _extract_cell_code(self, cell) -> str:
        """Extract code from a notebook cell"""
        if isinstance(cell.source, list):
            # Some platforms (Synapse) provide source as list with \n endings
            return "".join(str(line) for line in cell.source)
        else:
            return str(cell.source)

    def _process_code_blocks(
        self, code_blocks: list[str], suppress_nbimports: bool = False
    ) -> tuple[list[str], list[str]]:
        """Process code blocks, converting notebook_import() to Python imports

        Args:
            code_blocks: List of code strings from cells
            suppress_nbimports: If True, don't convert notebook_import() calls

        Returns:
            Tuple of (processed_blocks, imported_module_names)
        """
        processed_blocks = []
        all_imported_modules = []

        notebook_import_pattern = r"notebook_import\(['\"]([\w_]*)\.([\w_]+)['\"]\)"

        for block in code_blocks:
            # Find all notebook_import calls
            matches = re.findall(notebook_import_pattern, block)

            for package_name, module_name in matches:
                all_imported_modules.append(module_name)

            # Replace notebook_import with standard Python imports
            if not suppress_nbimports:
                modified_block = re.sub(
                    notebook_import_pattern,
                    lambda m: f"from {m.group(1)}.{m.group(2)} import *",
                    block,
                )
            else:
                modified_block = block

            processed_blocks.append(modified_block)

        return processed_blocks, all_imported_modules


# ═══════════════════════════════════════════════════════════════════════════
# Notebook Wheel Builder
# ═══════════════════════════════════════════════════════════════════════════


class NotebookWheelBuilder:
    """
    Build Python wheel packages (.whl) from notebook folders.

    Converts notebook-based packages to pip-installable wheels:
    1. Uses NotebookUtilities to extract Python code from notebooks
    2. Generates pyproject.toml with package metadata
    3. Builds wheel using pip/setuptools

    Used for: Creating distributable Python packages from notebook code
    """

    def __init__(self, platform_service, logger=None):
        """Initialize builder with platform service

        Args:
            platform_service: Platform service for notebook access
            logger: Optional logger
        """
        self.utilities = NotebookUtilities(platform_service, logger)
        self.platform_service = platform_service
        self.logger = logger

    def build_wheel(
        self,
        folder_name: str,
        package_name: str,
        output_location: str,
        version: str = "0.1.0",
        dependencies: list[str] = None,
        description: str = None,
        author: str = "Author",
        license_text: str = None,
    ) -> str:
        """Build a wheel package from a notebook folder

        Args:
            folder_name: Source notebook folder
            package_name: Name for the Python package
            output_location: Where to save the .whl file
            version: Package version
            dependencies: List of package dependencies
            description: Package description
            author: Package author
            license_text: License text content

        Returns:
            Path to the generated wheel file
        """
        import os
        import shutil
        import subprocess
        import uuid

        if self.logger:
            self.logger.info(f"Building wheel for notebook package: {folder_name}")

        dependencies = dependencies or []
        description = description or f"Package created from notebooks in {folder_name}"

        # Create temporary build directory
        temp_folder_path = f"/tmp/dist_{uuid.uuid4().hex}"
        os.makedirs(temp_folder_path, exist_ok=True)

        try:
            # Get notebooks in folder
            notebooks = self.utilities.get_notebooks_in_folder(folder_name)

            # Filter out _init notebooks
            module_names = [nb.name.split("/")[-1] for nb in notebooks if "_init" not in nb.name]

            # Generate __init__.py content
            import_lines = [f"from . import {name}" for name in module_names]
            init_content = "\n".join(import_lines)
            if init_content:
                init_content += "\n"

            # Generate pyproject.toml
            dependency_block = self._format_dependencies(dependencies)
            pyproject_content = self._generate_pyproject_toml(
                package_name, version, description, author, dependency_block
            )

            # Create package directory structure
            package_dir = os.path.join(temp_folder_path, package_name)
            os.makedirs(package_dir, exist_ok=True)

            # Write __init__.py
            with open(f"{package_dir}/__init__.py", "w") as f:
                f.write(init_content)

            # Write LICENSE if provided
            if license_text:
                with open(f"{package_dir}/LICENSE", "w") as f:
                    f.write(license_text)

            # Extract and write notebook code as .py modules
            for nb_name in module_names:
                full_nb_name = f"{folder_name}/{nb_name}"
                code, _ = self.utilities.extract_code_from_notebook(full_nb_name)
                with open(f"{package_dir}/{nb_name}.py", "w") as f:
                    f.write(code)

            # Write pyproject.toml
            with open(os.path.join(temp_folder_path, "pyproject.toml"), "w") as f:
                f.write(pyproject_content)

            # Write README.md
            readme_content = f"# {package_name}\n\n{description}\n"
            with open(os.path.join(temp_folder_path, "README.md"), "w") as f:
                f.write(readme_content)

            # Build wheel
            current_dir = os.getcwd()
            try:
                os.chdir(temp_folder_path)
                subprocess.check_call(["pip", "wheel", ".", "--no-deps", "-w", "dist/"])

                # Find generated wheel
                wheel_files = os.listdir("dist/")
                if not wheel_files:
                    raise RuntimeError("No wheel file generated")

                wheel_file = wheel_files[0]
                wheel_path = os.path.join(temp_folder_path, "dist", wheel_file)

                # Copy to output location
                if output_location.startswith("/"):
                    # Local filesystem
                    os.makedirs(os.path.dirname(output_location), exist_ok=True)
                    shutil.copy2(wheel_path, output_location)
                    final_path = output_location
                else:
                    # Distributed filesystem via platform service
                    local_uri = f"file:///{wheel_path}"
                    target_path = f"{output_location}/{wheel_file}"
                    self.platform_service.copy(local_uri, target_path, True)
                    final_path = target_path

                if self.logger:
                    self.logger.info(f"✅ Wheel built successfully: {final_path}")

                return final_path

            finally:
                os.chdir(current_dir)
        finally:
            # Cleanup temp directory
            if os.path.exists(temp_folder_path):
                shutil.rmtree(temp_folder_path)

    def _format_dependencies(self, dependencies: list[str]) -> str:
        """Format dependencies for pyproject.toml"""
        if not dependencies:
            return ""
        return "\n".join(f'        "{dep}",' for dep in dependencies)

    def _generate_pyproject_toml(
        self, name: str, version: str, description: str, author: str, deps: str
    ) -> str:
        """Generate pyproject.toml content (tightly coupled to wheel building)"""
        return f"""[build-system]
requires = ["setuptools>=42", "wheel"]
build-backend = "setuptools.build_meta"

[project]
name = "{name}"
version = "{version}"
description = "{description}"
readme = "README.md"
authors = [{{"name" = "{author}"}}]
license = {{"text" = "MIT"}}
classifiers = [
    "Programming Language :: Python :: 3",
    "License :: OSI Approved :: MIT License",
    "Operating System :: OS Independent",
]
dependencies = [
{deps}
]
requires-python = ">=3.7"
"""


# ═══════════════════════════════════════════════════════════════════════════
# Notebook App Builder
# ═══════════════════════════════════════════════════════════════════════════


class NotebookAppBuilder:
    """
    Build data app packages (.kda) from notebook folders.

    Converts notebook-based apps to deployable .kda archives:
    1. Uses NotebookUtilities to extract Python code from notebooks
    2. Generates app manifest (app.yaml)
    3. Creates ZIP archive with .kda extension

    Used for: Creating deployable data applications from notebook code
    """

    def __init__(self, platform_service, logger=None):
        """Initialize builder with platform service

        Args:
            platform_service: Platform service for notebook access
            logger: Optional logger
        """
        self.utilities = NotebookUtilities(platform_service, logger)
        self.logger = logger

    def build_app(
        self,
        folder_name: str,
        app_name: str,
        output_path: str,
        version: str = "1.0.0",
        entry_point: str = "main.py",
        dependencies: list[str] = None,
        environment: str = "production",
        description: str = None,
        metadata: dict = None,
    ) -> str:
        """Build a .kda app package from a notebook folder

        Args:
            folder_name: Source notebook folder
            app_name: Name for the app
            output_path: Where to save the .kda file
            version: App version
            entry_point: Entry point Python file
            dependencies: List of package dependencies
            environment: Target environment
            description: App description
            metadata: Additional metadata dict

        Returns:
            Path to the generated .kda file
        """
        import os
        import tempfile
        import zipfile
        from datetime import datetime

        import yaml

        if self.logger:
            self.logger.info(f"Building data app from notebook folder: {folder_name}")

        dependencies = dependencies or []
        description = description or f"Data app created from notebooks in {folder_name}"
        metadata = metadata or {}
        metadata.setdefault("source", "notebooks")
        metadata.setdefault("folder", folder_name)

        # Create temporary directory for app files
        with tempfile.TemporaryDirectory() as temp_dir:
            app_dir = Path(temp_dir) / app_name
            app_dir.mkdir()

            # Get notebooks and convert to Python files
            notebooks = self.utilities.get_notebooks_in_folder(folder_name)

            for notebook in notebooks:
                nb_name = notebook.name.split("/")[-1]
                if "_init" in nb_name:
                    continue

                full_nb_name = notebook.name
                code, _ = self.utilities.extract_code_from_notebook(full_nb_name)

                # Write as .py file
                py_filename = f"{nb_name}.py"
                (app_dir / py_filename).write_text(code)

            # Generate app.yaml manifest
            app_config = {
                "name": app_name,
                "version": version,
                "description": description,
                "entry_point": entry_point,
                "environment": environment,
                "dependencies": dependencies,
                "metadata": metadata,
            }

            (app_dir / "app.yaml").write_text(yaml.safe_dump(app_config, indent=2))

            # Create .kda package (ZIP archive)
            with zipfile.ZipFile(output_path, "w", zipfile.ZIP_DEFLATED) as kda_file:
                for file_path in app_dir.rglob("*"):
                    if file_path.is_file():
                        relative_path = file_path.relative_to(app_dir)
                        kda_file.write(file_path, relative_path)

                # Add manifest
                manifest = {
                    "name": app_name,
                    "version": version,
                    "description": description,
                    "entry_point": entry_point,
                    "dependencies": dependencies,
                    "lake_requirements": [],
                    "environment": environment,
                    "metadata": metadata,
                    "created_at": datetime.utcnow().isoformat(),
                    "kda_version": "1.0",
                }
                kda_file.writestr("kda-manifest.json", json.dumps(manifest, indent=2))

            if self.logger:
                self.logger.info(f"✅ Data app built successfully: {output_path}")

            return output_path


level_hierarchy = {
    "ALL": 0,
    "DEBUG": 1,
    "INFO": 2,
    "WARN": 3,
    "WARNING": 3,
    "ERROR": 4,
    "FATAL": 5,
    "OFF": 6,
}

currentLevel = "INFO"

# Import get_or_create_spark_session from spark_session module
# (removed duplicate implementation that was here)


def get_spark_log_level():
    return (
        get_or_create_spark_session()
        .sparkContext._jvm.org.apache.log4j.LogManager.getRootLogger()
        .getLevel()
    )


def create_console_logger(config):
    from pyspark.sql import SparkSession

    currentLevel = config.get("log_level", None) or get_spark_log_level()

    def should_log(level):
        level_log_rank = level_hierarchy.get(level.upper(), 2)
        current_log_rank = level_hierarchy.get(currentLevel, 2)
        return level_log_rank >= current_log_rank

    return type(
        "ConsoleLogger",
        (),
        {
            "debug": lambda self, *args, **kwargs: (
                print("DEBUG:", *args) if should_log("DEBUG") else None
            ),
            "info": lambda self, *args, **kwargs: (
                print("INFO:", *args) if should_log("INFO") else None
            ),
            "error": lambda self, *args, **kwargs: (
                print("ERROR:", *args) if should_log("ERROR") else None
            ),
            "warning": lambda self, *args, **kwargs: (
                print("WARNING:", *args) if should_log("WARNING") else None
            ),
        },
    )()


class PlatformService:
    @abstractmethod
    def get_platform_name(self):
        pass

    @abstractmethod
    def initialize(self):
        pass

    @abstractmethod
    def get_token(self, audience: str) -> str:
        pass

    @abstractmethod
    def exists(self, path: str) -> bool:
        pass

    @abstractmethod
    def copy(self, source: str, destination: str, overwrite: bool = False) -> None:
        pass

    @abstractmethod
    def read(self, path: str, encoding: str = "utf-8") -> Union[str, bytes]:
        pass

    @abstractmethod
    def write(self, path: str, content: Union[str, bytes], overwrite: bool = False) -> None:
        pass

    @abstractmethod
    def move(self, path: str, dest: str) -> None:
        pass

    @abstractmethod
    def delete(self, path: str, recurse=False) -> None:
        pass

    @abstractmethod
    def list(self, path: str) -> List[str]:
        pass

    @abstractmethod
    def get_notebooks(self) -> List:
        pass

    @abstractmethod
    def get_notebook(self, name: str):
        pass

    @abstractmethod
    def create_notebook(self, name: str, notebook):
        pass

    @abstractmethod
    def update_notebook(self, name: str, notebook):
        pass

    @abstractmethod
    def delete_notebook(self, name: str) -> None:
        pass

    @abstractmethod
    def get_spark_session(self):
        pass

    @abstractmethod
    def get_config(self, key: str, default: Any = None) -> Any:
        pass

    @abstractmethod
    def set_config(self, key: str, value: Any) -> None:
        pass

    @abstractmethod
    def get_log_level(self) -> str:
        pass

    @abstractmethod
    def is_interactive_session(self) -> bool:
        pass

    @abstractmethod
    def get_workspace_info(self) -> Dict[str, Any]:
        pass

    @abstractmethod
    def get_cluster_info(self) -> Dict[str, Any]:
        pass

    @abstractmethod
    def deploy_spark_job(
        self, app_files: Dict[str, str], job_config: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Deploy application as a Spark job on this platform

        Args:
            app_files: Dictionary of {filename: content or path} to deploy
            job_config: Job configuration including name, parameters, compute settings

        Returns:
            Dictionary with deployment info: {job_id, deployment_path, metadata}
        """
        pass

    @abstractmethod
    def run_spark_job(self, job_id: str, parameters: Dict[str, Any] = None) -> str:
        """Execute a deployed Spark job

        Args:
            job_id: ID of the job to run
            parameters: Optional runtime parameters

        Returns:
            Run ID for monitoring the job execution
        """
        pass

    @abstractmethod
    def get_job_status(self, run_id: str) -> Dict[str, Any]:
        """Get the status of a running job

        Args:
            run_id: Run ID from run_spark_job()

        Returns:
            Dictionary with: {status, start_time, end_time, error, logs}
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


def is_interactive_session(force_interactive: Optional[bool] = None) -> bool:
    if force_interactive is not None:
        return force_interactive

    try:
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
            if connection_file:
                return True
        except Exception:
            pass

        return False
    except Exception:
        return False


def safe_get_global(var_name: str, default: Any = None) -> Any:
    try:
        import __main__

        return getattr(__main__, var_name, default)
    except Exception:
        return default


class NotebookMetadata:
    def __init__(self, **kwargs):
        self.kernelspec = kwargs.get("kernelspec")
        self.language_info = kwargs.get("language_info")
        self.additional_properties = kwargs.get("additional_properties", {})


class NotebookCellMetadata:
    def __init__(self, **kwargs):
        self.additional_properties = kwargs.get("additional_properties", {})


class NotebookCell:
    def __init__(self, **kwargs):
        self.cell_type = kwargs.get("cell_type", "code")
        self.source = kwargs.get("source", [])
        self.metadata = NotebookCellMetadata(**kwargs.get("metadata", {}))
        self.outputs = kwargs.get("outputs", [])
        self.execution_count = kwargs.get("execution_count")
        self.additional_properties = kwargs.get("additional_properties", {})

    def get_source_as_string(self) -> str:
        if isinstance(self.source, list):
            return "".join(str(line) for line in self.source)
        return str(self.source)


class BigDataPoolReference:
    def __init__(self, **kwargs):
        self.type = kwargs.get("type", "BigDataPoolReference")
        self.reference_name = kwargs.get("reference_name")


class NotebookSessionProperties:
    def __init__(self, **kwargs):
        self.driver_memory = kwargs.get("driver_memory")
        self.driver_cores = kwargs.get("driver_cores")
        self.executor_memory = kwargs.get("executor_memory")
        self.executor_cores = kwargs.get("executor_cores")
        self.num_executors = kwargs.get("num_executors")


class NotebookFolder:
    def __init__(self, **kwargs):
        self.name = kwargs.get("name", "")
        self.path = kwargs.get("path", "")


class Notebook:
    def __init__(self, **kwargs):
        self.description = kwargs.get("description")
        self.big_data_pool = kwargs.get("big_data_pool")
        if self.big_data_pool and isinstance(self.big_data_pool, dict):
            self.big_data_pool = BigDataPoolReference(**self.big_data_pool)

        self.session_properties = kwargs.get("session_properties")
        if self.session_properties and isinstance(self.session_properties, dict):
            self.session_properties = NotebookSessionProperties(**self.session_properties)

        self.metadata = NotebookMetadata(**kwargs.get("metadata", {}))
        self.nbformat = kwargs.get("nbformat", 4)
        self.nbformat_minor = kwargs.get("nbformat_minor", 2)

        folder_data = kwargs.get("folder", {})
        if isinstance(folder_data, dict):
            self.folder = NotebookFolder(**folder_data)
        else:
            self.folder = NotebookFolder(name="", path="")

        cells_data = kwargs.get("cells", [])
        self.cells = []
        for cell_data in cells_data:
            if isinstance(cell_data, dict):
                self.cells.append(NotebookCell(**cell_data))
            else:
                self.cells.append(cell_data)

        self.additional_properties = kwargs.get("additional_properties", {})

    def get_code_cells(self) -> List[NotebookCell]:
        return [cell for cell in self.cells if cell.cell_type == "code"]


class NotebookResource:
    def __init__(self, **kwargs):
        self.id = kwargs.get("id")
        self.name = kwargs.get("name")
        self.type = kwargs.get("type", "Microsoft.Synapse/workspaces/notebooks")
        self.etag = kwargs.get("etag")

        properties_data = kwargs.get("properties", {})
        if isinstance(properties_data, dict):
            self.properties = Notebook(**properties_data)
        else:
            self.properties = properties_data

        self.additional_properties = kwargs.get("additional_properties", {})


class ArtifactRenameRequest:
    def __init__(self, new_name: str):
        self.new_name = new_name


class NotebookManager(ABC):
    """Abstract base class defining the interface for notebook management operations."""

    @abstractmethod
    def get_all_notebooks(self, force_refresh: bool = False) -> List:
        """Get all available notebooks, optionally forcing a cache refresh."""
        pass

    @abstractmethod
    def get_all_folders(self, force_refresh: bool = False) -> Set[str]:
        """Get all available folders, optionally forcing a cache refresh."""
        pass

    @abstractmethod
    def get_all_packages(self, ignored_folders: Optional[List[str]] = None) -> List[str]:
        """Get all packages, optionally ignoring specified folders."""
        pass

    @abstractmethod
    def get_notebooks_for_folder(self, folder_path: str) -> List:
        """Get all notebooks within a specific folder path."""
        pass

    @abstractmethod
    def get_notebooks_for_package(self, package_name: str, include_subfolders: bool = True) -> List:
        """Get all notebooks for a package, optionally including subfolders."""
        pass

    @abstractmethod
    def get_package_structure(self, package_name: str) -> Dict[str, Any]:
        """Get the structure of a package including notebooks and subfolders."""
        pass

    @abstractmethod
    def find_notebook_by_name(
        self, notebook_name: str, package_name: Optional[str] = None
    ) -> Optional:
        """Find a notebook by name, optionally within a specific package."""
        pass

    @abstractmethod
    def get_package_dependencies(self, package_name: str) -> List[str]:
        """Get the dependencies for a specific package."""
        pass

    @abstractmethod
    def refresh_cache(self) -> None:
        """Refresh the internal cache of notebooks and folders."""
        pass

    @abstractmethod
    def load_notebook_code(
        self, notebook_name: str, suppress_nbimports: bool = False
    ) -> Tuple[str, List[str]]:
        """Load the code from a notebook, returning the code and imported modules."""
        pass

    @abstractmethod
    def import_notebook_as_module(
        self,
        notebook_name: str,
        code: str,
        pkg_name: Optional[str] = None,
        module_name: Optional[str] = None,
        include_globals: Optional[Dict[str, Any]] = None,
    ) -> types.ModuleType:
        """Import a notebook as a Python module."""
        pass

    @abstractmethod
    def import_notebooks_into_module(
        self, package_name: str, notebook_names: List[str]
    ) -> Dict[str, types.ModuleType]:
        """Import multiple notebooks into modules within a package."""
        pass

    @abstractmethod
    def notebook_import(self, notebook_ref: str) -> Optional[types.ModuleType]:
        """Import a notebook by reference string."""
        pass

    @abstractmethod
    def publish_all_notebook_folders_as_packages(
        self, location: str, version: str = "0.1.0"
    ) -> None:
        """Publish all notebook folders as Python packages to a location."""
        pass

    @abstractmethod
    def publish_notebook_folder_as_package(
        self,
        folder_name: str,
        package_name: str,
        location: str,
        version: str,
        extra_dependencies: Optional[List[str]] = None,
    ) -> None:
        """Publish a specific notebook folder as a Python package."""
        pass


try:
    from kindling.injection import *
except (ImportError, ModuleNotFoundError):
    # injection module is not available, create a no-op version
    class _NoOpInjector:
        @staticmethod
        def singleton_autobind():
            """No-op decorator that returns the original function/class unchanged."""

            def decorator(func_or_class):
                return func_or_class

            return decorator

    GlobalInjector = _NoOpInjector()

    def inject(func):
        return func


try:
    from kindling.platform_provider import PlatformServiceProvider
except (ImportError, ModuleNotFoundError):
    # kindling.platform_provider module is not available, define interface locally
    class PlatformServiceProvider:
        def set_service(self):
            pass

        def get_service(self):
            pass


try:
    from kindling.spark_log_provider import PythonLoggerProvider
except (ImportError, ModuleNotFoundError):
    # kindling.platform_provider module is not available, define interface locally
    class PythonLoggerProvider:
        def get_logger(self, name):
            pass


def create_notebook_loader(es, config):
    """Create a dynamic object that implements PlatformEnvironmentProvider interface."""

    class DynamicPlatformProvider:
        def get_service(self):
            return es

    class DynamicLoggerProvider:
        def get_logger(self, name):
            return create_console_logger(config)

    return NotebookLoader(DynamicPlatformProvider(), DynamicLoggerProvider())


@GlobalInjector.singleton_autobind()
class NotebookLoader(NotebookManager):
    @inject
    def __init__(self, psp: PlatformServiceProvider, plp: PythonLoggerProvider):
        self.es = None
        self.psp = psp
        self.logger = plp.get_logger("NotebookLoader")
        self._loaded_modules: Dict[str, types.ModuleType] = {}
        self._notebook_cache = None
        self._folder_cache = None

    def get_platform_service(self):
        if not self.es:
            self.es = self.psp.get_service()
        return self.es

    def get_all_notebooks(self, force_refresh: bool = False) -> List[NotebookResource]:
        if self._notebook_cache is None or force_refresh:
            try:
                self._notebook_cache = self.get_platform_service().get_notebooks()
                self.logger.debug(f"Discovered {len(self._notebook_cache)} notebooks")
            except Exception as e:
                self.logger.error(f"Failed to discover notebooks: {str(e)}")
                raise e

        return self._notebook_cache

    def get_all_folders(self, force_refresh: bool = False) -> Set[str]:
        if self._folder_cache is None or force_refresh:
            notebooks = self.get_all_notebooks(force_refresh)
            folders = set()

            for notebook in notebooks:
                if notebook.name:
                    folder_path = self._extract_folder_path(notebook)
                    if folder_path:
                        folders.add(folder_path)
                        folders.update(self._get_parent_folders(folder_path))

            self._folder_cache = folders
            self.logger.debug(f"Discovered {len(folders)} folders")

        return self._folder_cache

    def get_all_packages(self, ignored_folders: Optional[List[str]] = None) -> List[str]:
        self.logger.debug(f"Getting all packages ...")

        if ignored_folders is None:
            ignored_folders = []

        folders = self.get_all_folders()
        packages = {}

        self.logger.debug(f"Searching {len(folders)} folders for packages...")

        for folder in folders:
            if folder not in ignored_folders:
                if self._folder_has_notebooks(folder):
                    folder_name = folder.split("/")[-1]
                    nbs_in_folder = self.get_notebooks_for_folder(folder)
                    nbs = [nb.name for nb in nbs_in_folder]
                    self.logger.debug(f"Checking folder '{folder}': {len(nbs)} notebooks")

                    init_nb_name = f"{folder_name}_init"

                    if init_nb_name in nbs:
                        self.logger.debug(f"Found package: {folder_name}")
                        packages[folder] = self.get_package_dependencies(folder_name)
                    else:
                        self.logger.debug(f"No package in {folder_name} (missing {init_nb_name})")
                else:
                    self.logger.debug(f"Folder {folder} has no notebooks")
            else:
                self.logger.debug(f"Ignoring folder: {folder}")

        package_list = self._resolve_dependency_order(packages)

        filtered_package_list = [pkg for pkg in package_list if pkg in packages.keys()]

        self.logger.debug(
            f"Package discovery complete: {len(filtered_package_list)} packages found"
        )
        return filtered_package_list

    def get_notebooks_for_folder(self, folder_path: str) -> List[NotebookResource]:
        notebooks = self.get_all_notebooks()
        folder_notebooks = []

        for notebook in notebooks:
            if notebook.name:
                notebook_folder = self._extract_folder_path(notebook)
                if notebook_folder and (
                    notebook_folder == folder_path or notebook_folder.startswith(folder_path + "/")
                ):
                    folder_notebooks.append(notebook)

        self.logger.debug(f"Found {len(folder_notebooks)} notebooks in folder '{folder_path}'")
        return folder_notebooks

    def get_notebooks_for_package(
        self, package_name: str, include_subfolders: bool = True
    ) -> List[NotebookResource]:
        return (
            self.get_notebooks_for_folder(package_name)
            if include_subfolders
            else self._get_direct_notebooks_for_folder(package_name)
        )

    def get_package_structure(self, package_name: str) -> Dict[str, Any]:
        notebooks = self.get_notebooks_for_package(package_name)
        structure = {"name": package_name, "notebooks": [], "subfolders": {}}

        for notebook in notebooks:
            if notebook.name:
                relative_path = self._get_relative_path(notebook.name, package_name)
                if "/" in relative_path:
                    subfolder = relative_path.split("/")[0]
                    if subfolder not in structure["subfolders"]:
                        structure["subfolders"][subfolder] = []
                    structure["subfolders"][subfolder].append(notebook)
                else:
                    structure["notebooks"].append(notebook)

        return structure

    def find_notebook_by_name(
        self, notebook_name: str, package_name: Optional[str] = None
    ) -> Optional[NotebookResource]:
        notebooks = (
            self.get_notebooks_for_package(package_name)
            if package_name
            else self.get_all_notebooks()
        )

        for notebook in notebooks:
            if notebook.name == notebook_name:
                return notebook

        base_name = notebook_name.replace(".ipynb", "").replace(".py", "")
        for notebook in notebooks:
            if notebook.name:
                notebook_base = notebook.name.replace(".ipynb", "").replace(".py", "")
                if notebook_base == base_name or notebook_base.endswith("/" + base_name):
                    return notebook

        return None

    def get_package_dependencies(self, package_name: str) -> List[str]:
        self.logger.debug(f"Getting package dependencies for {package_name}")

        result = self.load_notebook_code(f"{package_name}_init")
        if result is None:
            self.logger.debug(f"No _init notebook found for {package_name}, skipping dependencies")
            return []

        code, _ = result
        if not code:
            self.logger.error(f"No code loaded from notebook {package_name}")
            return []

        import __main__

        # Create execution context with NotebookPackages available
        exec_context = __main__.__dict__.copy()
        exec_context["NotebookPackages"] = NotebookPackages

        exec(compile(code, f"{package_name}_init", "exec"), exec_context)

        return NotebookPackages.get_package_definition(package_name).dependencies

    def _extract_folder_path(self, notebook) -> Optional[str]:
        """Extract folder path from notebook, handling both dict and object properties"""
        try:
            # Try object access first (Notebook object)
            if hasattr(notebook.properties, "folder"):
                folder = notebook.properties.folder
                if hasattr(folder, "path"):
                    return folder.path
                elif isinstance(folder, dict):
                    return folder.get("path", "")
            # Fallback to dict access
            elif isinstance(notebook.properties, dict):
                folder = notebook.properties.get("folder", {})
                if isinstance(folder, dict):
                    return folder.get("path", "")
                elif hasattr(folder, "path"):
                    return folder.path
        except (AttributeError, TypeError):
            pass
        return None

    def _get_parent_folders(self, folder_path: str) -> List[str]:
        parents = []
        parts = folder_path.split("/")
        for i in range(1, len(parts)):
            parent = "/".join(parts[:i])
            if parent:
                parents.append(parent)
        return parents

    def _get_top_level_folder(self, folder_path: str) -> Optional[str]:
        if "/" in folder_path:
            return folder_path.split("/")[0]
        return folder_path if folder_path else None

    def _folder_has_notebooks(self, folder_path: str) -> bool:
        notebooks = self.get_all_notebooks()
        for notebook in notebooks:
            if notebook.name:
                notebook_folder = self._extract_folder_path(notebook)
                if notebook_folder and (
                    notebook_folder == folder_path or notebook_folder.startswith(folder_path + "/")
                ):
                    return True
        return False

    def _get_direct_notebooks_for_folder(self, folder_path: str) -> List[NotebookResource]:
        notebooks = self.get_all_notebooks()
        direct_notebooks = []

        for notebook in notebooks:
            if notebook.name:
                notebook_folder = self._extract_folder_path(notebook)
                if notebook_folder == folder_path:
                    direct_notebooks.append(notebook)

        return direct_notebooks

    def _get_relative_path(self, notebook_name: str, package_name: str) -> str:
        if notebook_name.startswith(package_name + "/"):
            return notebook_name[len(package_name) + 1 :]
        return notebook_name

    def refresh_cache(self):
        self._notebook_cache = None
        self._folder_cache = None
        self.logger.info("Discovery cache refreshed")

    def _execute_notebook_code(self, notebook_name):
        self.logger.debug(f"Attempting to execute notebook: {notebook_name}")
        try:
            code, _ = self.load_notebook_code(notebook_name)
            if not code:
                self.logger.error(f"No code loaded from notebook {notebook_name}")
                return

            self.logger.debug(f"Loaded code length: {len(code)} characters")
            self.logger.debug(f"Code preview: {code[:200]}...")

            self.import_notebook_as_module(notebook_name, code)
            self.logger.debug(f"Successfully executed notebook {notebook_name}")
        except Exception as e:
            self.logger.error(f"Failed to execute notebook {notebook_name}: {str(e)}")
            raise

    def load_notebook_code(
        self, notebook_name: str, suppress_nbimports=False
    ) -> Tuple[str, List[str]]:
        try:
            self.logger.debug(f"Loading notebook code for: {notebook_name}")
            notebook = self.get_platform_service().get_notebook(notebook_name)

            code_blocks = []
            for cell in notebook.properties.cells:
                if cell.cell_type == "code":
                    cell_code = self._extract_cell_code(cell)
                    if cell_code.strip():
                        code_blocks.append(cell_code)

            if len(code_blocks) > 0:
                self.logger.debug(f"Found code for: {notebook_name}")

            processed_blocks, imported_modules = self._process_code_blocks(
                code_blocks, suppress_nbimports
            )
            final_code = "\n\n# ---- Next Cell ----\n\n".join(processed_blocks)

            return final_code, imported_modules
        except Exception as e:
            self.logger.error(f"Failed to load notebook {notebook_name}: {str(e)}")
            return None  # Return None on error so caller can handle it

    def _extract_cell_code(self, cell) -> str:
        if isinstance(cell.source, list):
            # Synapse cells already have \n at the end of each line
            return "\n".join(str(line.rstrip("\n")) for line in cell.source)
        else:
            return str(cell.source)

    def _process_code_blocks(
        self, code_blocks: List[str], suppress_nbimports=False
    ) -> Tuple[List[str], List[str]]:
        processed_blocks = []
        all_imported_modules = []

        notebook_import_pattern = r"notebook_import\(['\"]([\w_]*)\.([\w_]+)['\"]\)"

        for block in code_blocks:
            matches = re.findall(notebook_import_pattern, block)

            for package_name, module_name in matches:
                all_imported_modules.append(module_name)

            modified_block = (
                re.sub(notebook_import_pattern, self._create_import_replacement(), block)
                if not suppress_nbimports
                else block
            )

            processed_blocks.append(modified_block)

        return processed_blocks, all_imported_modules

    def _create_import_replacement(self):
        def replacement(match):
            package_name = match.group(1)
            module_name = match.group(2)
            importrepl = f"from {package_name}.{module_name} import *"
            self.logger.debug(f"import replacement = {importrepl}")
            return importrepl

        return replacement

    def import_notebook_as_module(
        self,
        notebook_name: str,
        code: str,
        pkg_name: Optional[str] = None,
        module_name: Optional[str] = None,
        include_globals: Optional[Dict[str, Any]] = None,
    ) -> types.ModuleType:
        if include_globals is None:
            import __main__

            include_globals = __main__.__dict__

        effective_module_name = (
            module_name or f"{pkg_name}.{notebook_name}" if pkg_name else notebook_name
        )

        if effective_module_name in sys.modules:
            del sys.modules[effective_module_name]

        module = types.ModuleType(effective_module_name)
        module.__spec__ = ModuleSpec(
            name=effective_module_name, loader=None, origin="dynamically created from notebook"
        )

        sys.modules[effective_module_name] = module

        if pkg_name:
            package = self._setup_dynamic_package(pkg_name)
            module.__package__ = pkg_name
            setattr(package, notebook_name, module)

        module_dict = module.__dict__

        if include_globals:
            for key, value in include_globals.items():
                if key not in module_dict:
                    module_dict[key] = value

        try:
            compiled_code = compile(code, effective_module_name, "exec")
            exec(compiled_code, module_dict)

            self._loaded_modules[effective_module_name] = module
            return module
        except Exception as e:
            if effective_module_name in sys.modules:
                del sys.modules[effective_module_name]
            self.logger.error(f"Failed to create module {effective_module_name}: {str(e)}")

    def _setup_dynamic_package(self, package_name: str) -> types.ModuleType:
        if package_name not in sys.modules:
            package_path = f"./{package_name}"
            package = types.ModuleType(package_name)
            package.__path__ = []
            package.__package__ = package_name
            package.__file__ = os.path.join(package_path, "__init__.py")

            package.__spec__ = ModuleSpec(
                name=package_name, loader=None, origin=package.__file__, is_package=True
            )

            sys.modules[package_name] = package
        else:
            package = sys.modules[package_name]

        return package

    def import_notebooks_into_module(
        self, package_name: str, notebook_names: List[str]
    ) -> Dict[str, types.ModuleType]:
        # Check if package already exists and prepare for reload if so
        if package_name in sys.modules:
            self.logger.info(f"Package {package_name} already loaded - preparing reload")
            self._prepare_package_reload(package_name)

        # First, check if this package has dependencies that need to be installed
        try:
            init_notebook_name = f"{package_name}_init"
            if init_notebook_name not in notebook_names:
                # Try to find and load the init notebook to get dependencies
                init_notebooks = [
                    nb.name
                    for nb in self.get_notebooks_for_folder(package_name)
                    if nb.name == init_notebook_name
                ]
                if init_notebooks:
                    notebook_names = [init_notebook_name] + notebook_names
        except Exception as e:
            self.logger.debug(f"Could not find init notebook for {package_name}: {e}")

        notebook_code = {}
        notebook_dependencies = {}
        package_pip_dependencies = []

        for nb_name in notebook_names:
            try:
                code, dependencies = self.load_notebook_code(nb_name)
                notebook_code[nb_name] = code
                notebook_dependencies[nb_name] = dependencies

                # If this is the init notebook, extract pip dependencies
                if nb_name == f"{package_name}_init":
                    try:
                        # Execute the init code to register the package
                        import __main__

                        exec_context = __main__.__dict__.copy()
                        exec_context["NotebookPackages"] = NotebookPackages
                        exec(compile(code, nb_name, "exec"), exec_context)

                        # Get the package definition
                        package_def = NotebookPackages.get_package_definition(package_name)
                        if package_def:
                            package_pip_dependencies = package_def.dependencies
                            self.logger.info(
                                f"Found pip dependencies for {package_name}: {package_pip_dependencies}"
                            )
                    except Exception as e:
                        self.logger.warning(f"Failed to extract dependencies from {nb_name}: {e}")

            except Exception as e:
                self.logger.error(f"Exception loading notebook code for {nb_name}: {e}")
                continue

        # Install pip dependencies if found
        if package_pip_dependencies:
            self._install_package_dependencies(package_name, package_pip_dependencies)

        load_order = self._resolve_dependency_order(notebook_dependencies)

        self.logger.debug(f"Load order = {load_order}")

        imported_modules = {}
        for nb_name in load_order:
            if nb_name in notebook_code:
                try:
                    module = self.import_notebook_as_module(
                        nb_name, notebook_code[nb_name], package_name
                    )
                    imported_modules[nb_name] = module
                except Exception as e:
                    self.logger.error(f"Exception importing notebook as module for {nb_name}: {e}")
                    pass

        return imported_modules

    def _prepare_package_reload(self, package_name: str) -> None:
        self.logger.debug(f"Clearing bindings for package: {package_name}")

        modules_to_remove = []
        if package_name in sys.modules:
            modules_to_remove.append(package_name)

        for module_name in list(sys.modules.keys()):
            if module_name.startswith(f"{package_name}."):
                modules_to_remove.append(module_name)

        classes_to_unbind = set()
        for module_name in modules_to_remove:
            module = sys.modules.get(module_name)
            if module:
                for name in dir(module):
                    obj = getattr(module, name, None)
                    if obj is not None and isinstance(obj, type):
                        if self._is_class_from_package(obj, package_name):
                            classes_to_unbind.add(obj)

        injector = GlobalInjector.get_injector()
        for cls in classes_to_unbind:
            self._unbind_class_and_interfaces(cls)

        for module_name in modules_to_remove:
            del sys.modules[module_name]
            self.logger.debug(f"Removed module from sys.modules: {module_name}")

    def _unbind_classes_from_module(self, module, package_name: str):
        """Unbind only classes that are defined in this module, not imported ones"""
        for name in dir(module):
            obj = getattr(module, name, None)
            if obj is not None and isinstance(obj, type):
                if self._is_class_from_package(obj, package_name):
                    self._unbind_class_and_interfaces(obj)

    def _is_class_from_package(self, cls, package_name: str) -> bool:
        """Check if a class is actually defined in the given package"""
        try:
            class_module = cls.__module__
            return class_module == package_name or class_module.startswith(f"{package_name}.")
        except AttributeError:
            return False

    def _unbind_class_and_interfaces(self, cls):
        """Unbind a class and all its abstract base classes from the injector"""
        import inspect

        injector = GlobalInjector.get_injector()

        for base in cls.__bases__:
            if inspect.isabstract(base):
                self._clear_binding(injector, base)

        self._clear_binding(injector, cls)

    def _clear_binding(self, injector, interface):
        """Clear both binding and singleton cache for an interface"""
        if hasattr(injector.binder, "_bindings") and interface in injector.binder._bindings:
            del injector.binder._bindings[interface]
            self.logger.debug(f"Unbound: {interface.__name__}")

        if hasattr(injector, "_stack") and len(injector._stack) > 0:
            scope = injector._stack[0]
            if hasattr(scope, "_cache") and interface in scope._cache:
                del scope._cache[interface]
                self.logger.debug(f"Cleared singleton cache: {interface.__name__}")

    def _install_package_dependencies(self, package_name: str, dependencies: List[str]) -> bool:
        """Install pip dependencies for a notebook package"""
        if not dependencies:
            return True

        try:
            import subprocess
            import sys

            self.logger.info(f"Installing {len(dependencies)} pip dependencies for {package_name}")

            pip_args = (
                [sys.executable, "-m", "pip", "install"]
                + dependencies
                + ["--disable-pip-version-check", "--no-warn-conflicts"]
            )

            result = subprocess.run(pip_args, capture_output=True, text=True)

            if result.returncode != 0:
                self.logger.error(
                    f"Failed to install dependencies for {package_name}: {result.stderr}"
                )
                return False

            self.logger.info(f"Successfully installed dependencies for {package_name}")
            return True

        except Exception as e:
            self.logger.error(f"Exception installing dependencies for {package_name}: {str(e)}")
            return False

    def _resolve_dependency_order(self, dependencies: Dict[str, List[str]]) -> List[str]:
        try:
            from graphlib import TopologicalSorter

            return list(TopologicalSorter(dependencies).static_order())
        except Exception as e:
            self.logger.error(f"Dependency resolution error: {e}")
            return list(dependencies.keys())

    def notebook_import(self, notebook_ref: str) -> Optional[types.ModuleType]:

        is_interactive = is_interactive_session()

        if is_interactive:
            return None

        if "." in notebook_ref:
            parts = notebook_ref.split(".")
            package_name = parts[0]
            notebook_name = parts[1]
        else:
            package_name = None
            notebook_name = notebook_ref

        if notebook_ref in sys.modules:
            module = sys.modules[notebook_ref]

            import __main__

            for name in dir(module):
                if not name.startswith("_"):
                    setattr(__main__, name, getattr(module, name))

            return module
        else:
            return None

    def _generate_python_dependency_block(self, dependencies):
        output = ""
        for dep in dependencies:
            output += f'        "{dep}",\n'
        return output

    def publish_all_notebook_folders_as_packages(self, location, version="0.1.0"):
        pkgs = self.get_all_packages()
        for pkg in pkgs:
            publish_notebook_folder_as_package(pkg, pkg, location)

    def publish_notebook_folder_as_package(
        self, folder_name, package_name, location, version, extra_dependencies=None
    ):
        import os
        import shutil
        import uuid

        from setuptools import find_packages, setup

        temp_folder_path = f"/tmp/dist_{uuid.uuid4().hex}"
        os.makedirs(temp_folder_path, exist_ok=True)

        nbs = self.get_notebooks_for_folder(folder_name)

        filtered_modules = [nb.name for nb in nbs if "_init" not in nb.name]
        import_lines = [f"from . import {nb_name}" for nb_name in filtered_modules]
        init_content = "\n".join(import_lines)
        if init_content:
            init_content += "\n"

        self._execute_notebook_code(f"{folder_name}_init")
        pkgdef = NotebookPackages.get_package_definition(folder_name)
        deps = pkgdef.dependencies
        if extra_dependencies:
            deps.extend(extra_dependencies)
        dependency_block = self._generate_python_dependency_block(deps)

        pyproject_content = f"""[build-system]
    requires = ["setuptools>=42", "wheel"]
    build-backend = "setuptools.build_meta"

    [project]
    name = "{package_name}"
    version = "{version}"
    description = "Package created from notebooks in {folder_name}"
    readme = "README.md"
    authors = [{{"name" = "Author"}}]
    license = {{"text" = "MIT"}}
    classifiers = [
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ]
    dependencies = [
{dependency_block}    ]
    requires-python = ">=3.7"
    """

        package_dir = os.path.join(temp_folder_path, package_name)
        os.makedirs(package_dir, exist_ok=True)

        with open(f"{package_dir}/__init__.py", "w") as f:
            f.write(init_content)

        if pkgdef.license_text:
            with open(f"{package_dir}/LICENSE", "w") as f:
                f.write(pkgdef.license_text)

        for nb_name in filtered_modules:
            code, _ = self.load_notebook_code(nb_name)
            with open(f"{package_dir}/{nb_name}.py", "w") as f:
                f.write(code)

        readme_content = f"""# {package_name}

    A package created from notebooks in {folder_name}.
    """

        with open(os.path.join(temp_folder_path, "pyproject.toml"), "w") as f:
            f.write(pyproject_content)

        with open(os.path.join(temp_folder_path, "README.md"), "w") as f:
            f.write(readme_content)

        current_dir = os.getcwd()
        try:
            os.chdir(temp_folder_path)

            subprocess.check_call(["pip", "wheel", ".", "--no-deps", "-w", "dist/"])

            wheel_files = os.listdir("dist/")
            wheel_file = wheel_files[0]
            wheel_path = os.path.join(temp_folder_path, "dist", wheel_file)

            if location.startswith("/"):
                os.makedirs(os.path.dirname(location), exist_ok=True)
                shutil.copy2(wheel_path, location)
            else:
                local_path = f"file:///{wheel_path}"
                self.get_platform_service().copy(local_path, f"{location}/{wheel_file}", True)

            print(f"Package {package_name} successfully built and saved to {location}")

        finally:
            os.chdir(current_dir)
            shutil.rmtree(temp_folder_path)


@dataclass
class NotebookPackage:
    name: str
    dependencies: List[str]
    tags: Dict[str, str]
    description: Optional[str] = None
    version: Optional[str] = "0.1.0"
    author: Optional[str] = None
    license_text: str = None


class NotebookPackages:
    registry: Dict[str, NotebookPackage] = {}

    @classmethod
    def register(cls, **decorator_params):
        required_fields = {field.name for field in fields(NotebookPackage)}
        provided_fields = set(decorator_params.keys())
        missing_fields = {"name", "dependencies", "tags"} - provided_fields

        if missing_fields:
            raise ValueError(f"Missing required fields in package decorator: {missing_fields}")

        package_name = decorator_params["name"]
        package_params = {k: v for k, v in decorator_params.items() if k != "name"}
        cls.register_package(package_name, **package_params)
        return None

    @classmethod
    def register_package(cls, name: str, **package_params):
        package_params.setdefault("description", f"Notebook package: {name}")
        package_params.setdefault("version", "0.1.0")
        package_params.setdefault("author", "Unknown")

        package = NotebookPackage(name=name, **package_params)
        cls.registry[name] = package
        cls._validate_dependencies(name, package.dependencies)

    @classmethod
    def _validate_dependencies(cls, package_name: str, dependencies: List[str]):
        if package_name in dependencies:
            raise DependencyError(f"Package {package_name} cannot depend on itself")

        for dep in dependencies:
            if dep in cls.registry:
                dep_package = cls.registry[dep]
                if package_name in dep_package.dependencies:
                    raise DependencyError(f"Circular dependency detected: {package_name} <-> {dep}")

    @classmethod
    def get_package_names(cls) -> List[str]:
        return list(cls.registry.keys())

    @classmethod
    def get_package_definition(cls, name: str) -> Optional[NotebookPackage]:
        return cls.registry.get(name)

    @classmethod
    def get_packages_by_tag(cls, tag_key: str, tag_value: str = None) -> List[NotebookPackage]:
        matching_packages = []

        for package in cls.registry.values():
            if tag_key in package.tags:
                if tag_value is None or package.tags[tag_key] == tag_value:
                    matching_packages.append(package)

        return matching_packages

    @classmethod
    def get_dependency_graph(cls) -> Dict[str, List[str]]:
        return {name: package.dependencies for name, package in cls.registry.items()}

    @classmethod
    def get_dependents(cls, package_name: str) -> List[str]:
        dependents = []

        for name, package in cls.registry.items():
            if package_name in package.dependencies:
                dependents.append(name)

        return dependents

    @classmethod
    def get_all_dependencies(cls, package_name: str, visited: Set[str] = None) -> Set[str]:
        if visited is None:
            visited = set()

        if package_name in visited:
            raise DependencyError(f"Circular dependency detected involving {package_name}")

        if package_name not in cls.registry:
            return set()

        visited.add(package_name)
        all_deps = set()

        package = cls.registry[package_name]
        for dep in package.dependencies:
            all_deps.add(dep)
            all_deps.update(cls.get_all_dependencies(dep, visited.copy()))

        return all_deps

    @classmethod
    def clear_registry(cls):
        cls.registry.clear()


class DependencyError(Exception):
    pass


@dataclass
class PlatformServiceDefinition:
    name: str
    factory: Callable[[], Any]
    description: Optional[str] = None


class PlatformServices:
    registry: Dict[str, PlatformServiceDefinition] = {}

    @classmethod
    def register(cls, name: str, description: Optional[str] = None):
        """
        Decorator to register a platform service factory function.

        Usage:
            @PlatformServices.register(name="spark_session")
            def create_spark():
                return SparkSession.builder.getOrCreate()
        """

        def decorator(factory_func: Callable[[], Any]):
            cls.register_service(name=name, factory=factory_func, description=description)
            return factory_func

        return decorator

    @classmethod
    def register_service(
        cls, name: str, factory: Callable[[], Any], description: Optional[str] = None
    ):
        """
        Register a platform service with its factory function.

        Args:
            name: Unique service name
            factory: Callable that returns the service instance
            description: Optional description
        """
        if name in cls.registry:
            # Skip if already registered (e.g., from cached Python environment)
            # This prevents errors when Fabric reuses Python interpreters between job runs
            return

        service_def = PlatformServiceDefinition(
            name=name, factory=factory, description=description or f"Platform service: {name}"
        )
        cls.registry[name] = service_def
        print(f"✓ Registered platform service: {name}")

    @classmethod
    def has_service(cls, name: str) -> bool:
        """Check if a service is registered"""
        return name in cls.registry

    @classmethod
    def get_service_names(cls) -> list[str]:
        """Get all registered service names"""
        return list(cls.registry.keys())

    @classmethod
    def get_service_definition(cls, name: str) -> Optional[PlatformServiceDefinition]:
        """Get the service definition without instantiating"""
        return cls.registry.get(name)

    @classmethod
    def clear_registry(cls):
        """Clear all registered services (useful for testing)"""
        cls.registry.clear()
        print("✓ Platform services registry cleared")
