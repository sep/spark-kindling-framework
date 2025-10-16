import sys
import subprocess
import os
import uuid
import shutil
import tempfile
from typing import Dict, List, Any, Optional, Union, Tuple
from dataclasses import dataclass
from pathlib import Path
from abc import ABC, abstractmethod

from kindling.injection import *
from kindling.spark_config import *
from kindling.spark_log_provider import *
from kindling.spark_trace import *
from .notebook_framework import *
from kindling.platform_provider import *


class AppConstants:
    """Configuration constants for app framework"""
    REQUIREMENTS_FILE = "requirements.txt"
    LAKE_REQUIREMENTS_FILE = "lake-reqs.txt"
    BASE_CONFIG_FILE = "app.yaml"
    ENV_CONFIG_TEMPLATE = "app.{environment}.yaml"
    DEFAULT_ENTRY_POINT = "main.py"

    # Wheel priorities (lower is better)
    WHEEL_PRIORITY_PLATFORM_SPECIFIC = 1
    WHEEL_PRIORITY_GENERIC = 2
    WHEEL_PRIORITY_FALLBACK = 3

    # Pip common arguments
    PIP_COMMON_ARGS = [
        "--disable-pip-version-check",
        "--no-warn-conflicts"
    ]


class AppRunner(ABC):
    @abstractmethod
    def run_app(self, app_name: str) -> Any:
        pass


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
class AppContext:
    """Container for app execution context"""
    config: 'AppConfig'
    pypi_dependencies: List[str]
    lake_requirements: List[str]


@dataclass
class AppConfig:
    name: str
    description: str
    entry_point: str
    dependencies: List[str]
    environment: str
    metadata: Dict[str, Any]


@GlobalInjector.singleton_autobind()
class AppManager(AppRunner):
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
            apps_dir = f"{self.artifacts_path}/apps/"
            apps = self.get_platform_service().list(apps_dir)
            app_names = []
            for app_path in apps:
                app_name = app_path.rstrip('/').split('/')[-1]
                if app_name and not app_name.startswith('.'):
                    app_names.append(app_name)
            self.logger.info(f"Discovered {len(app_names)} apps: {app_names}")
            return app_names
        except Exception as e:
            self.logger.error(f"Failed to discover apps: {str(e)}")
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

    def _prepare_app_context(self, app_name: str) -> AppContext:
        """Prepare all context needed for app execution"""
        environment = self.config.get('environment')

        # Load config
        app_config = self._load_app_config(app_name, environment)

        # Resolve dependencies
        pypi_deps, lake_reqs = self._resolve_app_dependencies(
            app_name, self.config)
        pypi_deps = self._override_with_workspace_packages(
            pypi_deps, self.config)

        return AppContext(
            config=app_config,
            pypi_dependencies=pypi_deps,
            lake_requirements=lake_reqs
        )

    def _get_env_name(self) -> str:
        """Get current platform environment name"""
        return self.get_platform_service().get_platform_name()

    def _get_app_dir(self, app_name: str) -> str:
        """Get app directory path"""
        return f"{self.artifacts_path}/apps/{app_name}/"

    def _get_packages_dir(self) -> str:
        """Get packages directory path"""
        return f"{self.artifacts_path}/packages/"

    def _load_app_config(self, app_name: str, environment: str = None) -> AppConfig:
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
            config_path = f"{app_dir}{AppConstants.BASE_CONFIG_FILE}"
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
                           app_name: str, environment: str = None) -> AppConfig:
        """Create AppConfig from parsed data"""
        return AppConfig(
            name=config_data.get('name', app_name),
            description=config_data.get('description', ''),
            entry_point=config_data.get(
                'entry_point', AppConstants.DEFAULT_ENTRY_POINT),
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
                requirements_path = f"{app_dir}{AppConstants.REQUIREMENTS_FILE}"
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
            lake_reqs_path = f"{app_dir}{AppConstants.LAKE_REQUIREMENTS_FILE}"

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
            *AppConstants.PIP_COMMON_ARGS
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
            *AppConstants.PIP_COMMON_ARGS
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
        if f"-{current_env}.whl" in file_name:
            priority = AppConstants.WHEEL_PRIORITY_PLATFORM_SPECIFIC
        elif "-any.whl" in file_name:
            priority = AppConstants.WHEEL_PRIORITY_GENERIC
        else:
            priority = AppConstants.WHEEL_PRIORITY_FALLBACK

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
