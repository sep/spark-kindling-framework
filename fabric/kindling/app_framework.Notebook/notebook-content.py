# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

import sys
import subprocess
import os
import uuid
import shutil
import tempfile
from typing import Dict, List, Any, Optional, Union, Tuple
from dataclasses import dataclass
from pathlib import Path

notebook_import(".injection")
notebook_import(".spark_config")
notebook_import(".spark_log_provider")
notebook_import(".spark_trace")
notebook_import(".notebook_framework")
notebook_import(".platform_provider")

class AppRunner(ABC):
    @abstractmethod
    def run_app(self, app_name: str) -> Any:
        pass

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
    def __init__(self, nm: NotebookManager, pp: PlatformEnvironmentProvider, config: ConfigService, tp: SparkTraceProvider, lp: PythonLoggerProvider):
        self.config = config
        self.pp = pp
        self.es = pp.get_service()
        self.tp = tp
        self.framework = nm
        self.logger = lp.get_logger("AppManager")
        self.storage_utils = self._get_storage_utils()
        self.temp_dir = None  # For downloading files locally
        self.artifacts_path = config.get("artifacts_storage_path")
    
    def _get_storage_utils(self):
        """Get platform storage utilities"""
        import __main__
        return getattr(__main__, 'mssparkutils', None) or getattr(__main__, 'dbutils', None)
    
    def _get_env_name(self) -> str:
        return self.es.get_platform_name()
    
    def discover_apps(self) -> List[str]:
        """Discover all available apps in the datalake"""
        try:
            apps_dir = f"{self.artifacts_path}/apps/"
            apps = self.storage_utils.fs.ls(apps_dir)
            app_names = [app.name for app in apps if app.isDir]
            self.logger.info(f"Discovered {len(app_names)} apps: {app_names}")
            return app_names
        except Exception as e:
            self.logger.error(f"Failed to discover apps: {str(e)}")
            return []
    
    def load_app_config(self, app_name: str, environment: str = None) -> AppConfig:
        self.logger.debug(f"Loading config for {app_name}")        
        app_dir = f"{self.artifacts_path}/apps/{app_name}/"
        
        config_content = None
        config_file = None
        
        if environment:
            env_config_file = f"app.{environment}.yaml"
            try:     
                self.logger.debug(f"Loading config: {app_dir}{env_config_file}")
                raw_content = self.storage_utils.fs.head(f"{app_dir}{env_config_file}", max_bytes=1024*1024)
                config_content = raw_content.decode('utf-8') if isinstance(raw_content, bytes) else raw_content
                config_file = env_config_file
                self.logger.info(f"Loaded environment config: {env_config_file}")
            except Exception:
                self.logger.debug(f"Environment config {env_config_file} not found, trying base config")
        
        if not config_content:
            try:
                config_file = "app.yaml"
                config_file_path = f"{app_dir}app.yaml"
                self.logger.debug(f"Loading config: {config_file_path}")
                raw_content = self.storage_utils.fs.head(config_file_path, max_bytes=1024*1024)
                config_content = raw_content.decode('utf-8') if isinstance(raw_content, bytes) else raw_content
                config_file = "app.yaml"
            except Exception as e:
                raise Exception(f"Failed to load app config for {app_name}: {str(e)}")
        
        try:
            import yaml
            config_data = yaml.safe_load(config_content)
        except ImportError:
            import json
            config_data = json.loads(config_content)
        
        return AppConfig(
            name=config_data.get('name', app_name),
            description=config_data.get('description', ''),
            entry_point=config_data.get('entry_point', 'main.py'),
            dependencies=config_data.get('dependencies', []),
            environment=environment or config_data.get('environment', 'default'),
            metadata=config_data.get('metadata', {})
        )
    
    def parse_lake_requirements(self, app_name: str) -> List[str]:
        """Parse lake-reqs.txt file to get package specifications"""
        try:
            app_dir = f"{self.artifacts_path}/apps/{app_name}/"
            lake_reqs_path = f"{app_dir}lake-reqs.txt"
            
            raw_content = self.storage_utils.fs.head(lake_reqs_path, max_bytes=1024*1024)
            content = raw_content.decode('utf-8') if isinstance(raw_content, bytes) else raw_content
            
            # Parse package specifications (package name with optional version)
            lake_requirements = []
            for line in content.split('\n'):
                line = line.strip()
                if line and not line.startswith('#'):
                    lake_requirements.append(line)
            
            self.logger.debug(f"Parsed {len(lake_requirements)} lake requirements for {app_name}")
            return lake_requirements
            
        except Exception as e:
            self.logger.debug(f"No lake-reqs.txt found for {app_name}: {str(e)}")
            return []
    
    def resolve_app_dependencies(self, app_name: str, config: Dict[str, Any]) -> Tuple[List[str], List[str]]:
        """Load requirements.txt (PyPI) and lake-reqs.txt (datalake) separately"""
        pypi_dependencies = []
        lake_requirements = []
        
        try:
            app_dir = f"{self.artifacts_path}/apps/{app_name}/"
            
            # Load requirements.txt (PyPI packages)
            try:
                requirements_path = f"{app_dir}requirements.txt"
                raw_content = self.storage_utils.fs.head(requirements_path, max_bytes=1024*1024)
                content = raw_content.decode('utf-8') if isinstance(raw_content, bytes) else raw_content
                
                pypi_dependencies = [line.strip() for line in content.split('\n') 
                                   if line.strip() and not line.startswith('#') and not line.startswith('-')]
                
                self.logger.debug(f"Loaded {len(pypi_dependencies)} PyPI requirements for {app_name}")
                
            except Exception as e:
                self.logger.debug(f"No requirements.txt found for {app_name}: {str(e)}")
            
            # Load lake-reqs.txt (datalake packages)
            lake_requirements = self.parse_lake_requirements(app_name)
            
            return pypi_dependencies, lake_requirements
            
        except Exception as e:
            self.logger.error(f"Failed to resolve dependencies for {app_name}: {str(e)}")
            return [], []
    
    def find_best_wheel(self, package_spec: str) -> Optional[str]:
        """Find the best matching wheel for a package specification considering environment"""
        current_env = self._get_env_name()
        
        # Parse package spec (e.g., "my-framework==1.0.0" or "my-framework")
        if '==' in package_spec:
            package_name, version = package_spec.split('==', 1)
            package_name = package_name.strip()
            version = version.strip()
        else:
            package_name = package_spec.strip()
            version = None
        
        try:
            packages_dir = f"{self.artifacts_path}/packages/"
            all_files = self.storage_utils.fs.ls(packages_dir)
            
            # Find all wheels that match the package name
            matching_wheels = []
            for file_info in all_files:
                if not file_info.name.endswith('.whl'):
                    continue
                
                # Extract package name from wheel filename
                wheel_parts = file_info.name.split('-')
                if len(wheel_parts) < 2:
                    continue
                
                wheel_package_name = wheel_parts[0].replace('_', '-')
                wheel_version = wheel_parts[1] if len(wheel_parts) > 1 else None
                
                # Check if package name matches
                if wheel_package_name.lower() != package_name.lower().replace('_', '-'):
                    continue
                
                # Check version if specified
                if version and wheel_version != version:
                    continue
                
                # Determine priority based on environment match
                priority = 3  # Default priority
                if f"-{current_env}.whl" in file_info.name:
                    priority = 1  # Environment-specific wheel (highest priority)
                elif "-any.whl" in file_info.name:
                    priority = 2  # Generic wheel (medium priority)
                
                matching_wheels.append((file_info.path, file_info.name, priority, wheel_version))
            
            if not matching_wheels:
                self.logger.warning(f"No wheel found for package: {package_spec}")
                return None
            
            # Sort by priority (1=best), then by version (newest first)
            matching_wheels.sort(key=lambda x: (x[2], x[3] or '0'), reverse=False)
            
            best_wheel = matching_wheels[0]
            self.logger.info(f"Selected wheel for {package_spec}: {best_wheel[1]}")
            
            return best_wheel[0]  # Return path
            
        except Exception as e:
            self.logger.error(f"Failed to find wheel for {package_spec}: {str(e)}")
            return None
    
    def download_lake_wheels(self, app_name: str, lake_requirements: List[str]) -> str:
        """Download required wheels from datalake to local cache"""
        if not lake_requirements:
            return ""
        
        if not self.temp_dir:
            self.temp_dir = tempfile.mkdtemp(prefix=f"app_{app_name}_")
        
        wheels_dir = Path(self.temp_dir) / "wheels"
        wheels_dir.mkdir(parents=True, exist_ok=True)
        
        downloaded_wheels = []
        total_size = 0
        
        for package_spec in lake_requirements:
            wheel_path = self.find_best_wheel(package_spec)
            
            if not wheel_path:
                self.logger.warning(f"Skipping missing wheel: {package_spec}")
                continue
            
            wheel_name = wheel_path.split('/')[-1]
            local_wheel_path = wheels_dir / wheel_name
            
            # Skip if already downloaded
            if local_wheel_path.exists():
                self.logger.debug(f"Wheel already cached: {wheel_name}")
                continue
            
            try:
                self.logger.debug(f"Downloading wheel: {wheel_name}")
                
                self.storage_utils.fs.cp(wheel_path, f"file://{local_wheel_path}")

                file_size = local_wheel_path.stat().st_size
                total_size += file_size

                downloaded_wheels.append(wheel_name)
                
                self.logger.debug(f"Downloaded {wheel_name} ({file_size/1024/1024:.1f}MB)")
                
            except Exception as e:
                self.logger.error(f"Failed to download {wheel_name}: {str(e)}")
        
        self.logger.info(f"Downloaded {len(downloaded_wheels)} wheels, total size: {total_size/1024/1024:.1f}MB")
        return str(wheels_dir)
    
    def override_with_workspace_packages(self, dependencies: List[str], config: Dict[str, Any]) -> List[str]:
        """Override dependencies with workspace versions if available and configured"""
        if not config.get('load_local_packages', False):
            return dependencies
        
        try:
            workspace_packages = []
            if hasattr(self.framework, 'get_all_packages'):
                workspace_packages = self.framework.get_all_packages()
            
            overridden = []
            for dep in dependencies:
                package_name = dep.split('==')[0].split('>=')[0].split('<=')[0].split('<')[0].split('>')[0]
                
                if package_name in workspace_packages:
                    self.logger.info(f"Using workspace version of {package_name}")
                    continue
                else:
                    overridden.append(dep)
            
            self.logger.debug(f"Workspace override: {len(dependencies) - len(overridden)} packages from workspace")
            return overridden
            
        except Exception as e:
            self.logger.warning(f"Failed to override with workspace packages: {str(e)}")
            return dependencies
    
    def install_app_dependencies(self, app_name: str, pypi_dependencies: List[str], lake_requirements: List[str]) -> bool:

        # Download wheels from datalake
        wheels_cache_dir = ""
        if lake_requirements:
            wheels_cache_dir = self.download_lake_wheels(app_name, lake_requirements)
        
        # Install datalake wheels first (they may be dependencies for PyPI packages)
        if wheels_cache_dir and lake_requirements:
            try:
                self.logger.info(f"Installing {len(lake_requirements)} datalake packages")
                
                # Install wheels directly
                wheel_files = list(Path(wheels_cache_dir).glob("*.whl"))
                if wheel_files:
                    pip_args = [sys.executable, "-m", "pip", "install"] + [str(wf) for wf in wheel_files] + [
                        "--disable-pip-version-check",
                        "--no-warn-conflicts"
                    ]
                    
                    result = subprocess.run(pip_args, capture_output=True, text=True)
                    
                    if result.returncode == 0:
                        self.logger.info("Datalake wheels installed successfully")
                        
                        # Import packages to execute @singleton_autobind decorators
                        for package_spec in lake_requirements:
                            package_name = package_spec.split('==')[0].strip()
                            try:
                                # This import will execute the class definitions and register bindings
                                __import__(package_name)
                                self.logger.info(f"Imported {package_name} - decorators executed")
                            except ImportError as e:
                                self.logger.error(f"Failed to import {package_name}: {e}")
                                return False
                    else:
                        self.logger.error(f"Datalake wheel installation failed: {result.stderr}")
                        return False
                    
                    self.logger.info("Datalake wheels installed successfully")
                
            except Exception as e:
                self.logger.error(f"Failed to install datalake wheels: {str(e)}")
                return False
        
        # Install PyPI dependencies
        if pypi_dependencies:
            try:
                self.logger.info(f"Installing {len(pypi_dependencies)} PyPI packages")
                
                pip_args = [sys.executable, "-m", "pip", "install"] + pypi_dependencies + [
                    "--disable-pip-version-check",
                    "--no-warn-conflicts"
                ]
                
                # Add find-links if we have local wheels (in case PyPI packages depend on our wheels)
                if wheels_cache_dir:
                    pip_args.extend(["--find-links", wheels_cache_dir])
                
                result = subprocess.run(pip_args, capture_output=True, text=True)
                
                if result.returncode != 0:
                    self.logger.error(f"PyPI dependency installation failed: {result.stderr}")
                    return False
                
                self.logger.info("PyPI dependencies installed successfully")
                
            except Exception as e:
                self.logger.error(f"Failed to install PyPI dependencies: {str(e)}")
                return False
        
        if not pypi_dependencies and not lake_requirements:
            self.logger.debug("No dependencies to install")
        
        return True
    
    def load_app_code(self, app_name: str, entry_point: str) -> str:
        """Load app code from datalake"""
        try:
            app_dir = f"{self.artifacts_path}/apps/{app_name}/"
            code_path = f"{app_dir}{entry_point}"
            
            raw_content = self.storage_utils.fs.head(code_path, max_bytes=10*1024*1024)
            code_content = raw_content.decode('utf-8') if isinstance(raw_content, bytes) else raw_content
            self.logger.debug(f"Loaded app code from {entry_point} ({len(code_content)} chars)")
            
            return code_content
            
        except Exception as e:
            raise Exception(f"Failed to load app code for {app_name}: {str(e)}")
    
    def execute_app(self, app_name: str, code: str) -> Any:
        """Execute app code in the current environment"""
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
    
    def cleanup_temp_files(self):
        """Clean up temporary wheel cache"""
        if self.temp_dir and os.path.exists(self.temp_dir):
            try:
                shutil.rmtree(self.temp_dir)
                self.logger.debug(f"Cleaned up temp directory: {self.temp_dir}")
            except Exception as e:
                self.logger.warning(f"Failed to cleanup temp directory: {str(e)}")
    
    def __del__(self):
        """Cleanup on destruction"""
        self.cleanup_temp_files()


    def run_app(self, app_name: str) -> Any:
        artifacts_path = self.config.get('artifacts_storage_path')
        with self.tp.span(component=f"kindling-app-{app_name}",operation="running",details={},reraise=True ):   
            try:
                environment = self.config.get('environment')
                app_config = self.load_app_config(app_name, environment)
                
                pypi_dependencies, lake_requirements = self.resolve_app_dependencies(app_name, self.config)
                
                pypi_dependencies = self.override_with_workspace_packages(pypi_dependencies, self.config)
                
                with self.tp.span(component=f"kindling-app-{app_name}",operation="loading_dependencies",details={},reraise=True ):  
                    if not self.install_app_dependencies(app_name, pypi_dependencies, lake_requirements):
                        raise Exception("Failed to install app dependencies")
                
                with self.tp.span(component=f"kindling-app-{app_name}",operation="loading_code",details={},reraise=True ):  
                    code = self.load_app_code(app_name, app_config.entry_point)

                with self.tp.span(component=f"kindling-app-{app_name}",operation="executing_code",details={},reraise=True ):                   
                    result = self.execute_app(app_name, code)
                
                return result
            
            finally:
                # Cleanup temp files
                self.cleanup_temp_files()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
