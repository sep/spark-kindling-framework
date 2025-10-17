import sys
import subprocess
import os
import uuid
import shutil
import tempfile
import zipfile
import json
import yaml
from typing import Dict, List, Any, Optional, Union, Tuple
from dataclasses import dataclass, asdict
from pathlib import Path
from abc import ABC, abstractmethod
from datetime import datetime

from kindling.injection import *
from kindling.spark_config import *
from kindling.spark_log_provider import *
from kindling.spark_trace import *
from .notebook_framework import *
from kindling.platform_provider import *


class DataAppConstants:
    """Configuration constants for data app framework"""
    REQUIREMENTS_FILE = "requirements.txt"
    LAKE_REQUIREMENTS_FILE = "lake-reqs.txt"
    BASE_CONFIG_FILE = "app.yaml"
    ENV_CONFIG_TEMPLATE = "app.{environment}.yaml"
    DEFAULT_ENTRY_POINT = "main.py"

    # KDA package constants
    KDA_EXTENSION = ".kda"
    KDA_MANIFEST_FILE = "kda-manifest.json"
    KDA_VERSION = "1.0"

    # Wheel priorities (lower is better)
    WHEEL_PRIORITY_PLATFORM_SPECIFIC = 1
    WHEEL_PRIORITY_GENERIC = 2
    WHEEL_PRIORITY_FALLBACK = 3

    # Pip common arguments
    PIP_COMMON_ARGS = [
        "--disable-pip-version-check",
        "--no-warn-conflicts"
    ]


class DataAppRunner(ABC):
    @abstractmethod
    def run_app(self, app_name: str) -> Any:
        pass


class AppDeploymentService(ABC):
    """Platform-specific service for deploying and running data apps"""

    @abstractmethod
    def submit_spark_job(self, job_config: Dict[str, Any]) -> Dict[str, Any]:
        """Submit a Spark job to the platform

        Args:
            job_config: Platform-specific job configuration

        Returns:
            Dict containing job_id and submission status
        """
        pass

    @abstractmethod
    def get_job_status(self, job_id: str) -> Dict[str, Any]:
        """Get the status of a submitted job

        Args:
            job_id: Platform job identifier

        Returns:
            Dict containing job status, logs, and results
        """
        pass

    @abstractmethod
    def cancel_job(self, job_id: str) -> bool:
        """Cancel a running job

        Args:
            job_id: Platform job identifier

        Returns:
            True if cancellation succeeded
        """
        pass

    @abstractmethod
    def get_job_logs(self, job_id: str) -> str:
        """Get logs from a job

        Args:
            job_id: Platform job identifier

        Returns:
            Job logs as string
        """
        pass

    @abstractmethod
    def create_job_config(self, app_name: str, app_path: str,
                          environment_vars: Optional[Dict[str, str]] = None) -> Dict[str, Any]:
        """Create platform-specific job configuration

        Args:
            app_name: Name of the data app
            app_path: Path to deployed app in storage
            environment_vars: Environment variables for the job

        Returns:
            Platform-specific job configuration
        """
        pass


@dataclass
class KDAManifest:
    """Manifest file for KDA packages"""
    name: str
    version: str
    description: str
    entry_point: str
    dependencies: List[str]
    lake_requirements: List[str]
    environment: str
    metadata: Dict[str, Any]
    created_at: str
    kda_version: str = DataAppConstants.KDA_VERSION


@dataclass
class WheelCandidate:
    """Represents a wheel file candidate for dependency resolution"""
    file_path: str
    file_name: str
    priority: int  # 1=platform-specific, 2=any, 3=fallback
    version: Optional[str]

    @property
    def sort_key(self) -> Tuple[int, str]:
        """Sort key for selecting best wheel (prefer lower priority, higher version)"""
        return (self.priority, self.version or '0')


@dataclass
class DataAppContext:
    """Container for data app execution context"""
    config: 'DataAppConfig'
    pypi_dependencies: List[str]
    lake_requirements: List[str]


@dataclass
class DataAppConfig:
    name: str
    description: str
    entry_point: str
    dependencies: List[str]
    environment: str
    metadata: Dict[str, Any]


@GlobalInjector.singleton_autobind()
class DataAppManager(DataAppRunner):
    @inject
    def __init__(self, nm: NotebookManager, pp: PlatformServiceProvider,
                 config: ConfigService, tp: SparkTraceProvider, lp: PythonLoggerProvider):
        self.config = config
        self.psp = pp
        self.es = None
        self.tp = tp
        self.framework = nm
        self.logger = lp.get_logger("AppManager")
        self.artifacts_path = config.get("artifacts_storage_path")

    def get_platform_service(self):
        if not self.es:
            self.es = self.psp.get_service()
        return self.es

    def discover_apps(self) -> List[str]:
        try:
            apps_dir = f"{self.artifacts_path}/data-apps/"  # Updated path
            apps = self.get_platform_service().list(apps_dir)
            app_names = []
            for app_path in apps:
                app_name = app_path.rstrip('/').split('/')[-1]
                if app_name and not app_name.startswith('.'):
                    app_names.append(app_name)
            self.logger.info(
                f"Discovered {len(app_names)} data apps: {app_names}")
            return app_names
        except Exception as e:
            self.logger.error(f"Failed to discover data apps: {str(e)}")
            return []

    def run_app(self, app_name: str) -> Any:
        """Run an app with full lifecycle management"""
        with self.tp.span(component=f"kindling-app-{app_name}", operation="running",
                          details={}, reraise=True):
            temp_dir = None
            try:
                # Prepare app context
                app_context = self._prepare_app_context(app_name)

                # Install dependencies
                with self.tp.span(component=f"kindling-app-{app_name}",
                                  operation="loading_dependencies", details={}, reraise=True):
                    temp_dir = self._install_app_dependencies(
                        app_name,
                        app_context.pypi_dependencies,
                        app_context.lake_requirements
                    )

                # Load and execute app code
                with self.tp.span(component=f"kindling-app-{app_name}",
                                  operation="loading_code", details={}, reraise=True):
                    code = self._load_app_code(
                        app_name, app_context.config.entry_point)

                with self.tp.span(component=f"kindling-app-{app_name}",
                                  operation="executing_code", details={}, reraise=True):
                    result = self._execute_app(app_name, code)

                return result

            finally:
                if temp_dir:
                    self._cleanup_temp_files(temp_dir)

    def _prepare_app_context(self, app_name: str) -> DataAppContext:
        """Prepare all context needed for app execution"""
        environment = self.config.get('environment')

        # Load config
        app_config = self._load_app_config(app_name, environment)

        # Resolve dependencies
        pypi_deps, lake_reqs = self._resolve_app_dependencies(
            app_name, self.config)
        pypi_deps = self._override_with_workspace_packages(
            pypi_deps, self.config)

        return DataAppContext(
            config=app_config,
            pypi_dependencies=pypi_deps,
            lake_requirements=lake_reqs
        )

    def _get_env_name(self) -> str:
        """Get current platform environment name"""
        return self.get_platform_service().get_platform_name()

    def _get_app_dir(self, app_name: str) -> str:
        """Get app directory path"""
        return f"{self.artifacts_path}/data-apps/{app_name}/"

    def _get_packages_dir(self) -> str:
        """Get packages directory path"""
        return f"{self.artifacts_path}/packages/"

    def package_app(self, app_directory: str, output_path: Optional[str] = None,
                    version: Optional[str] = None, target_platform: Optional[str] = None,
                    merge_platform_config: bool = True) -> str:
        """Package a data app directory into a KDA file for a specific platform

        Args:
            app_directory: Path to app source directory
            output_path: Output KDA file path (auto-generated if None)
            version: App version (from metadata if None)
            target_platform: Target platform (synapse, databricks, fabric)
            merge_platform_config: If True, merge platform config into base config.
                                  If False, include all platform configs separately.
        """
        try:
            app_path = Path(app_directory)
            if not app_path.exists():
                raise FileNotFoundError(
                    f"App directory not found: {app_directory}")

            # Load base app config from source directory
            config = self._load_app_config_from_directory(app_path)

            # If merging platform config, merge it at package time
            if merge_platform_config and target_platform:
                config = self._merge_platform_config_at_deploy_time(
                    app_path, config, target_platform)
            app_name = config.name
            app_version = version or config.metadata.get('version', '1.0.0')

            # Generate output path if not provided
            if not output_path:
                platform_suffix = f"-{target_platform}" if target_platform and merge_platform_config else ""
                output_path = f"{app_name}{platform_suffix}-v{app_version}{DataAppConstants.KDA_EXTENSION}"

            platform_info = f" for {target_platform}" if target_platform else ""
            merge_info = " (merged)" if merge_platform_config and target_platform else " (multi-platform)"
            self.logger.info(
                f"Packaging data app '{app_name}' v{app_version}{platform_info}{merge_info} -> {output_path}")

            # Create KDA package
            with zipfile.ZipFile(output_path, 'w', zipfile.ZIP_DEFLATED) as kda_file:
                if merge_platform_config:
                    # Single platform: exclude platform-specific configs and base app.yaml, use merged version
                    for file_path in app_path.rglob('*'):
                        if (file_path.is_file() and
                            not self._is_platform_config_file(file_path) and
                                file_path.name != 'app.yaml'):
                            relative_path = file_path.relative_to(app_path)
                            kda_file.write(file_path, relative_path)
                            self.logger.debug(f"Added to KDA: {relative_path}")

                    # Add the merged config as app.yaml
                    merged_config = self._create_merged_config(config)
                    kda_file.writestr("app.yaml", merged_config)
                    self.logger.debug("Added merged app.yaml to KDA")
                else:
                    # Multi-platform: include all files including platform configs
                    for file_path in app_path.rglob('*'):
                        if file_path.is_file():
                            relative_path = file_path.relative_to(app_path)
                            kda_file.write(file_path, relative_path)
                            self.logger.debug(f"Added to KDA: {relative_path}")

                # Generate and add manifest
                manifest = self._create_kda_manifest(
                    config, app_version, target_platform, merge_platform_config)
                manifest_json = json.dumps(asdict(manifest), indent=2)
                kda_file.writestr(
                    DataAppConstants.KDA_MANIFEST_FILE, manifest_json)

            self.logger.info(
                f"✅ Successfully created KDA package: {output_path}")
            return output_path

        except Exception as e:
            self.logger.error(f"Failed to package data app: {str(e)}")
            raise

    def _is_platform_config_file(self, file_path: Path) -> bool:
        """Check if file is a platform-specific config file"""
        filename = file_path.name
        return (filename.startswith('app.') and
                filename != 'app.yaml' and
                filename.endswith('.yaml'))

    def _create_merged_config(self, config: 'DataAppConfig') -> str:
        """Create merged YAML config from DataAppConfig"""
        try:
            import yaml

            config_dict = {
                'name': config.name,
                'description': config.description,
                'entry_point': config.entry_point,
                'environment': config.environment,
                'dependencies': config.dependencies,
                'metadata': config.metadata
            }

            return yaml.safe_dump(config_dict, indent=2)
        except ImportError:
            # Fallback to JSON if yaml not available
            return json.dumps(asdict(config), indent=2)

    def deploy_kda(self, kda_file_path: str, target_environment: Optional[str] = None) -> bool:
        """Deploy a KDA file to the artifacts storage"""
        try:
            kda_path = Path(kda_file_path)
            if not kda_path.exists():
                raise FileNotFoundError(f"KDA file not found: {kda_file_path}")

            self.logger.info(f"Deploying KDA: {kda_file_path}")

            # Extract and validate manifest
            with zipfile.ZipFile(kda_path, 'r') as kda_file:
                if DataAppConstants.KDA_MANIFEST_FILE not in kda_file.namelist():
                    raise ValueError("Invalid KDA file: missing manifest")

                manifest_content = kda_file.read(
                    DataAppConstants.KDA_MANIFEST_FILE)
                manifest_data = json.loads(manifest_content)
                manifest = KDAManifest(**manifest_data)

                app_name = manifest.name
                self.logger.info(
                    f"Deploying data app: {app_name} v{manifest.version}")

                # Extract to target directory
                target_dir = f"{self.artifacts_path}/data-apps/{app_name}/"
                platform_service = self.get_platform_service()

                # Clear existing deployment
                try:
                    platform_service.delete(target_dir)
                    self.logger.debug(
                        f"Cleared existing deployment at: {target_dir}")
                except:
                    pass  # Directory might not exist

                # Extract all files except manifest to target directory
                for file_info in kda_file.filelist:
                    if file_info.filename == DataAppConstants.KDA_MANIFEST_FILE:
                        continue  # Skip manifest in deployed version

                    file_content = kda_file.read(file_info.filename)
                    target_path = f"{target_dir}{file_info.filename}"
                    platform_service.write(target_path, file_content)
                    self.logger.debug(f"Deployed: {target_path}")

                # Store deployment metadata
                deployment_info = {
                    "app_name": app_name,
                    "version": manifest.version,
                    "deployed_at": datetime.now().isoformat(),
                    "environment": target_environment or "default",
                    "kda_version": manifest.kda_version
                }

                deployment_path = f"{target_dir}.deployment-info.json"
                platform_service.write(
                    deployment_path, json.dumps(deployment_info, indent=2))

            self.logger.info(
                f"✅ Successfully deployed data app '{app_name}' to {target_dir}")
            return True

        except Exception as e:
            self.logger.error(f"Failed to deploy KDA: {str(e)}")
            return False

    def _create_kda_manifest(self, config: DataAppConfig, version: str, target_platform: Optional[str] = None, merged: bool = False) -> KDAManifest:
        """Create KDA manifest from app config"""
        # For local packaging, resolve what we can
        pypi_deps = config.dependencies
        lake_reqs = []  # Could enhance to read lake-reqs.txt from directory

        # Add platform info to metadata
        metadata = config.metadata.copy()
        if target_platform:
            metadata['target_platform'] = target_platform

        return KDAManifest(
            name=config.name,
            version=version,
            description=config.description,
            entry_point=config.entry_point,
            dependencies=pypi_deps,
            lake_requirements=lake_reqs,
            environment=config.environment,
            metadata=metadata,
            created_at=datetime.now().isoformat()
        )

    def _load_app_config_from_directory(self, app_path: Path) -> DataAppConfig:
        """Load app config from local directory"""
        try:
            # Try base config first
            config_file = app_path / DataAppConstants.BASE_CONFIG_FILE
            if not config_file.exists():
                raise FileNotFoundError(f"App config not found: {config_file}")

            with open(config_file, 'r') as f:
                config_content = f.read()

            try:
                config_data = yaml.safe_load(config_content)
            except:
                config_data = json.loads(config_content)

            return DataAppConfig(
                name=config_data.get('name', app_path.name),
                description=config_data.get('description', ''),
                entry_point=config_data.get(
                    'entry_point', DataAppConstants.DEFAULT_ENTRY_POINT),
                dependencies=config_data.get('dependencies', []),
                environment=config_data.get('environment', 'default'),
                metadata=config_data.get('metadata', {})
            )

        except Exception as e:
            self.logger.error(
                f"Failed to load app config from {app_path}: {str(e)}")
            raise

    def _merge_platform_config_at_deploy_time(self, app_path: Path, base_config: DataAppConfig, target_platform: str) -> DataAppConfig:
        """Merge platform-specific config into base config at deploy time"""
        try:
            # Look for platform-specific config file
            platform_config_file = app_path / f"app.{target_platform}.yaml"

            if not platform_config_file.exists():
                self.logger.debug(
                    f"No platform config found for {target_platform}, using base config")
                return base_config

            # Load platform-specific overrides
            with open(platform_config_file, 'r') as f:
                platform_content = f.read()

            try:
                platform_data = yaml.safe_load(platform_content)
            except:
                platform_data = json.loads(platform_content)

            # Create merged config (platform overrides base)
            merged_metadata = base_config.metadata.copy()
            if platform_data.get('metadata'):
                merged_metadata.update(platform_data['metadata'])

            merged_dependencies = base_config.dependencies.copy()
            if platform_data.get('dependencies'):
                # Platform dependencies extend base dependencies
                for dep in platform_data['dependencies']:
                    if dep not in merged_dependencies:
                        merged_dependencies.append(dep)

            merged_config = DataAppConfig(
                name=platform_data.get('name', base_config.name),
                description=platform_data.get(
                    'description', base_config.description),
                entry_point=platform_data.get(
                    'entry_point', base_config.entry_point),
                dependencies=merged_dependencies,
                environment=platform_data.get(
                    'environment', base_config.environment),
                metadata=merged_metadata
            )

            self.logger.debug(
                f"Merged {target_platform} config into base config")
            return merged_config

        except Exception as e:
            self.logger.warning(
                f"Failed to merge platform config for {target_platform}: {str(e)}")
            return base_config

    def _load_app_config(self, app_name: str, environment: str = None) -> DataAppConfig:
        """Load app configuration with environment override support"""
        self.logger.debug(f"Loading config for {app_name}")

        config_content = self._load_config_content(app_name, environment)
        config_data = self._parse_config_content(config_content)

        return self._create_app_config(config_data, app_name, environment)

    def _load_config_content(self, app_name: str, environment: str = None) -> str:
        """Load config file content (with environment override)"""
        app_dir = self._get_app_dir(app_name)

        # Try environment-specific config first
        if environment:
            try:
                env_config_file = f"app.{environment}.yaml"
                env_config_path = f"{app_dir}{env_config_file}"
                self.logger.debug(f"Loading config: {env_config_path}")
                content = self.get_platform_service().read(env_config_path)
                self.logger.info(
                    f"Loaded environment config: {env_config_file}")
                return content
            except Exception:
                self.logger.debug(
                    f"Environment config {env_config_file} not found, trying base config")

        # Fall back to base config
        try:
            config_path = f"{app_dir}{DataAppConstants.BASE_CONFIG_FILE}"
            self.logger.debug(f"Loading config: {config_path}")
            return self.get_platform_service().read(config_path)
        except Exception as e:
            raise Exception(
                f"Failed to load app config for {app_name}: {str(e)}")

    def _parse_config_content(self, content: str) -> Dict[str, Any]:
        """Parse config content (YAML or JSON fallback)"""
        try:
            import yaml
            return yaml.safe_load(content)
        except ImportError:
            import json
            return json.loads(content)

    def _create_app_config(self, config_data: Dict[str, Any],
                           app_name: str, environment: str = None) -> DataAppConfig:
        """Create DataAppConfig from parsed data"""
        return DataAppConfig(
            name=config_data.get('name', app_name),
            description=config_data.get('description', ''),
            entry_point=config_data.get(
                'entry_point', DataAppConstants.DEFAULT_ENTRY_POINT),
            dependencies=config_data.get('dependencies', []),
            environment=environment or config_data.get(
                'environment', 'default'),
            metadata=config_data.get('metadata', {})
        )

    def _resolve_app_dependencies(self, app_name: str, config: Dict[str, Any]) -> Tuple[List[str], List[str]]:
        """Resolve app dependencies from requirements files"""
        pypi_dependencies = []
        lake_requirements = []

        try:
            app_dir = self._get_app_dir(app_name)

            # Load PyPI dependencies
            try:
                requirements_path = f"{app_dir}{DataAppConstants.REQUIREMENTS_FILE}"
                content = self.get_platform_service().read(requirements_path)

                pypi_dependencies = [line.strip() for line in content.split('\n')
                                     if line.strip() and not line.startswith('#') and not line.startswith('-')]

                self.logger.debug(
                    f"Loaded {len(pypi_dependencies)} PyPI requirements for {app_name}")

            except Exception as e:
                self.logger.debug(
                    f"No requirements.txt found for {app_name}: {str(e)}")

            # Load lake dependencies
            lake_requirements = self._parse_lake_requirements(app_name)

            return pypi_dependencies, lake_requirements

        except Exception as e:
            self.logger.error(
                f"Failed to resolve dependencies for {app_name}: {str(e)}")
            return [], []

    def _parse_lake_requirements(self, app_name: str) -> List[str]:
        """Parse lake-reqs.txt file for custom wheel dependencies"""
        try:
            app_dir = self._get_app_dir(app_name)
            lake_reqs_path = f"{app_dir}{DataAppConstants.LAKE_REQUIREMENTS_FILE}"

            content = self.get_platform_service().read(lake_reqs_path)

            lake_requirements = []
            for line in content.split('\n'):
                line = line.strip()
                if line and not line.startswith('#'):
                    lake_requirements.append(line)

            self.logger.debug(
                f"Parsed {len(lake_requirements)} lake requirements for {app_name}")
            return lake_requirements

        except Exception as e:
            self.logger.debug(
                f"No lake-reqs.txt found for {app_name}: {str(e)}")
            return []

    def _extract_package_name(self, package_spec: str) -> str:
        """Extract package name from spec (e.g., 'pandas==1.0.0' -> 'pandas')"""
        return package_spec.split('==')[0].split('>=')[0].split('<=')[0].split('<')[0].split('>')[0].strip()

    def _override_with_workspace_packages(self, dependencies: List[str], config: Dict[str, Any]) -> List[str]:
        """Filter out dependencies available in workspace"""
        if not config.get('load_local_packages', False):
            return dependencies

        try:
            workspace_packages = []
            if hasattr(self.framework, 'get_all_packages'):
                workspace_packages = self.framework.get_all_packages()

            overridden = []
            for dep in dependencies:
                package_name = self._extract_package_name(dep)

                if package_name in workspace_packages:
                    self.logger.info(
                        f"Using workspace version of {package_name}")
                    continue
                else:
                    overridden.append(dep)

            self.logger.debug(
                f"Workspace override: {len(dependencies) - len(overridden)} packages from workspace")
            return overridden

        except Exception as e:
            self.logger.warning(
                f"Failed to override with workspace packages: {str(e)}")
            return dependencies

    def _install_app_dependencies(self, app_name: str, pypi_dependencies: List[str],
                                  lake_requirements: List[str]) -> Optional[str]:
        """Install app dependencies (orchestrator method)"""
        temp_dir = None
        wheels_cache_dir = ""

        # Step 1: Download lake wheels if needed
        if lake_requirements:
            temp_dir = tempfile.mkdtemp(prefix=f"app_{app_name}_")
            wheels_cache_dir = self._download_lake_wheels(
                app_name, lake_requirements, temp_dir)

        # Step 2: Install lake wheels
        if wheels_cache_dir and lake_requirements:
            self._install_lake_wheels(wheels_cache_dir, lake_requirements)

        # Step 3: Install PyPI dependencies
        if pypi_dependencies:
            self._install_pypi_dependencies(
                pypi_dependencies, wheels_cache_dir)

        if not pypi_dependencies and not lake_requirements:
            self.logger.debug("No dependencies to install")

        return temp_dir

    def _install_lake_wheels(self, wheels_cache_dir: str, lake_requirements: List[str]) -> None:
        """Install lake wheel packages"""
        self.logger.info(
            f"Installing {len(lake_requirements)} datalake packages")

        wheel_files = list(Path(wheels_cache_dir).glob("*.whl"))
        if not wheel_files:
            return

        pip_args = [
            sys.executable, "-m", "pip", "install",
            *[str(wf) for wf in wheel_files],
            *DataAppConstants.PIP_COMMON_ARGS
        ]

        result = subprocess.run(pip_args, capture_output=True, text=True)

        if result.returncode != 0:
            self.logger.error(
                f"Datalake wheel installation failed: {result.stderr}")
            raise Exception("Datalake wheel installation failed")

        self.logger.info("Datalake wheels installed successfully")

        # Import packages to execute decorators
        self._import_installed_packages(lake_requirements)

    def _import_installed_packages(self, package_specs: List[str]) -> None:
        """Import packages to trigger decorator execution"""
        for package_spec in package_specs:
            package_name = self._extract_package_name(package_spec)
            try:
                __import__(package_name)
                self.logger.info(
                    f"Imported {package_name} - decorators executed")
            except ImportError as e:
                self.logger.error(f"Failed to import {package_name}: {e}")
                raise

    def _install_pypi_dependencies(self, pypi_dependencies: List[str],
                                   wheels_cache_dir: str = "") -> None:
        """Install PyPI packages"""
        self.logger.info(f"Installing {len(pypi_dependencies)} PyPI packages")

        pip_args = [
            sys.executable, "-m", "pip", "install",
            *pypi_dependencies,
            *DataAppConstants.PIP_COMMON_ARGS
        ]

        if wheels_cache_dir:
            pip_args.extend(["--find-links", wheels_cache_dir])

        result = subprocess.run(pip_args, capture_output=True, text=True)

        if result.returncode != 0:
            self.logger.error(
                f"PyPI dependency installation failed: {result.stderr}")
            raise Exception("PyPI dependency installation failed")

        self.logger.info("PyPI dependencies installed successfully")

    def _download_lake_wheels(self, app_name: str, lake_requirements: List[str], temp_dir: str) -> str:
        if not lake_requirements:
            return ""

        wheels_dir = Path(temp_dir) / "wheels"
        wheels_dir.mkdir(parents=True, exist_ok=True)

        downloaded_wheels = []
        total_size = 0

        for package_spec in lake_requirements:
            wheel_path = self._find_best_wheel(package_spec)

            if not wheel_path:
                self.logger.warning(f"Skipping missing wheel: {package_spec}")
                continue

            wheel_name = wheel_path.split('/')[-1]
            local_wheel_path = wheels_dir / wheel_name
            remote_wheel_path = f"{self.artifacts_path}/packages/{wheel_name}"

            if local_wheel_path.exists():
                self.logger.debug(f"Wheel already cached: {wheel_name}")
                continue

            try:
                self.logger.debug(
                    f"Downloading wheel: {wheel_name} from {remote_wheel_path}")

                self.get_platform_service().copy(
                    remote_wheel_path, f"file://{local_wheel_path}", overwrite=True)

                file_size = local_wheel_path.stat().st_size
                total_size += file_size

                downloaded_wheels.append(wheel_name)

                self.logger.debug(
                    f"Downloaded {wheel_name} ({file_size/1024/1024:.1f}MB)")

            except Exception as e:
                self.logger.error(f"Failed to download {wheel_name}: {str(e)}")

        self.logger.info(
            f"Downloaded {len(downloaded_wheels)} wheels, total size: {total_size/1024/1024:.1f}MB")
        return str(wheels_dir)

    def _find_best_wheel(self, package_spec: str) -> Optional[str]:
        """Find the best matching wheel for a package spec"""
        package_name, version = self._parse_package_spec(package_spec)

        all_wheels = self._list_available_wheels()
        matching_wheels = self._filter_matching_wheels(
            all_wheels, package_name, version
        )

        if not matching_wheels:
            self.logger.warning(f"No wheel found for package: {package_spec}")
            return None

        best_wheel = self._select_best_wheel(matching_wheels)
        self.logger.info(
            f"Selected wheel for {package_spec}: {best_wheel.file_name}")

        return best_wheel.file_path

    def _parse_package_spec(self, package_spec: str) -> Tuple[str, Optional[str]]:
        """Parse package specification into name and version"""
        if '==' in package_spec:
            package_name, version = package_spec.split('==', 1)
            return package_name.strip(), version.strip()
        return package_spec.strip(), None

    def _list_available_wheels(self) -> List[str]:
        """List all available wheel files in packages directory"""
        packages_dir = self._get_packages_dir()
        return self.get_platform_service().list(packages_dir)

    def _filter_matching_wheels(self, all_files: List[str],
                                package_name: str,
                                version: Optional[str]) -> List[WheelCandidate]:
        """Filter wheels matching package name and version"""
        current_env = self._get_env_name()
        matching = []

        for file_path in all_files:
            file_name = file_path.split('/')[-1]

            if not file_name.endswith('.whl'):
                continue

            candidate = self._parse_wheel_filename(
                file_name, file_path, current_env)
            if not candidate:
                continue

            # Check name match
            wheel_pkg_name = candidate.file_name.split(
                '-')[0].replace('_', '-')
            if wheel_pkg_name.lower() != package_name.lower().replace('_', '-'):
                continue

            # Check version match
            if version and candidate.version != version:
                continue

            matching.append(candidate)

        return matching

    def _parse_wheel_filename(self, file_name: str, file_path: str,
                              current_env: str) -> Optional[WheelCandidate]:
        """Parse wheel filename into WheelCandidate"""
        wheel_parts = file_name.split('-')
        if len(wheel_parts) < 2:
            return None

        package_name = wheel_parts[0].replace('_', '-')
        version = wheel_parts[1] if len(wheel_parts) > 1 else None

        # Determine priority based on platform specificity
        # Highest priority: platform-specific package names
        # Note: wheel filenames convert dashes to underscores (kindling-synapse -> kindling_synapse)
        if package_name == f"kindling-{current_env}" or package_name == f"kindling_{current_env}":
            priority = DataAppConstants.WHEEL_PRIORITY_PLATFORM_SPECIFIC
        # Legacy support: platform tags in filename (kindling-0.1.0-py3-none-synapse.whl)
        elif f"-{current_env}.whl" in file_name:
            priority = DataAppConstants.WHEEL_PRIORITY_PLATFORM_SPECIFIC
        # Generic wheels (kindling-0.1.0-py3-none-any.whl)
        elif "-any.whl" in file_name:
            priority = DataAppConstants.WHEEL_PRIORITY_GENERIC
        else:
            priority = DataAppConstants.WHEEL_PRIORITY_FALLBACK

        return WheelCandidate(
            file_path=file_path,
            file_name=file_name,
            priority=priority,
            version=version
        )

    def _select_best_wheel(self, candidates: List[WheelCandidate]) -> WheelCandidate:
        """Select the best wheel from candidates (prefer platform-specific, higher version)"""
        candidates.sort(key=lambda w: w.sort_key)
        return candidates[0]

    def _load_app_code(self, app_name: str, entry_point: str) -> str:
        try:
            app_dir = f"{self.artifacts_path}/apps/{app_name}/"
            code_path = f"{app_dir}{entry_point}"

            code_content = self.get_platform_service().read(code_path)
            self.logger.debug(
                f"Loaded app code from {entry_point} ({len(code_content)} chars)")

            return code_content

        except Exception as e:
            raise Exception(
                f"Failed to load app code for {app_name}: {str(e)}")

    def _execute_app(self, app_name: str, code: str) -> Any:
        try:
            self.logger.info(f"Executing app: {app_name}")

            exec_globals = {
                '__name__': f'app_{app_name}',
                '__file__': f'{app_name}/main.py',
                'framework': self.framework,
                'logger': self.logger
            }

            import __main__
            exec_globals.update(__main__.__dict__)

            compiled_code = compile(code, f'{app_name}/main.py', 'exec')
            exec(compiled_code, exec_globals)

            self.logger.debug(f"App {app_name} executed successfully")
            return exec_globals.get('result')

        except Exception as e:
            self.logger.error(f"Failed to execute app {app_name}: {str(e)}")
            raise

    def _cleanup_temp_files(self, temp_dir: str):
        if temp_dir and os.path.exists(temp_dir):
            try:
                shutil.rmtree(temp_dir)
                self.logger.debug(f"Cleaned up temp directory: {temp_dir}")
            except Exception as e:
                self.logger.warning(
                    f"Failed to cleanup temp directory: {str(e)}")
