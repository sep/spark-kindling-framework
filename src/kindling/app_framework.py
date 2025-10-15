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

from .injection import *
from .spark_config import *
from .spark_log_provider import *
from .spark_trace import *
from .notebook_framework import *
from .platform_provider import *

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
        with self.tp.span(component=f"kindling-app-{app_name}", operation="running", 
                         details={}, reraise=True):   
            temp_dir = None
            try:
                environment = self.config.get('environment')
                app_config = self._load_app_config(app_name, environment)
                
                pypi_dependencies, lake_requirements = self._resolve_app_dependencies(
                    app_name, self.config
                )
                
                pypi_dependencies = self._override_with_workspace_packages(
                    pypi_dependencies, self.config
                )
                
                with self.tp.span(component=f"kindling-app-{app_name}", 
                                operation="loading_dependencies", details={}, reraise=True):
                    temp_dir = self._install_app_dependencies(
                        app_name, pypi_dependencies, lake_requirements
                    )
                
                with self.tp.span(component=f"kindling-app-{app_name}", 
                                operation="loading_code", details={}, reraise=True):
                    code = self._load_app_code(app_name, app_config.entry_point)
                
                with self.tp.span(component=f"kindling-app-{app_name}", 
                                operation="executing_code", details={}, reraise=True):
                    result = self._execute_app(app_name, code)
                
                return result
            
            finally:
                if temp_dir:
                    self._cleanup_temp_files(temp_dir)
    
    def _get_env_name(self) -> str:
        return self.get_platform_service().get_platform_name()
    
    def _load_app_config(self, app_name: str, environment: str = None) -> AppConfig:
        self.logger.debug(f"Loading config for {app_name}")        
        app_dir = f"{self.artifacts_path}/apps/{app_name}/"
        
        config_content = None
        config_file = None
        
        if environment:
            env_config_file = f"app.{environment}.yaml"
            env_config_path = f"{app_dir}{env_config_file}"
            try:
                self.logger.debug(f"Loading config: {env_config_path}")
                config_content = self.get_platform_service().read(env_config_path)
                config_file = env_config_file
                self.logger.info(f"Loaded environment config: {env_config_file}")
            except Exception:
                self.logger.debug(f"Environment config {env_config_file} not found, trying base config")
        
        if not config_content:
            try:
                config_file = "app.yaml"
                config_file_path = f"{app_dir}app.yaml"
                self.logger.debug(f"Loading config: {config_file_path}")
                config_content = self.get_platform_service().read(config_file_path)
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
    
    def _resolve_app_dependencies(self, app_name: str, config: Dict[str, Any]) -> Tuple[List[str], List[str]]:
        pypi_dependencies = []
        lake_requirements = []
        
        try:
            app_dir = f"{self.artifacts_path}/apps/{app_name}/"
            
            try:
                requirements_path = f"{app_dir}requirements.txt"
                content = self.get_platform_service().read(requirements_path)
                
                pypi_dependencies = [line.strip() for line in content.split('\n') 
                                   if line.strip() and not line.startswith('#') and not line.startswith('-')]
                
                self.logger.debug(f"Loaded {len(pypi_dependencies)} PyPI requirements for {app_name}")
                
            except Exception as e:
                self.logger.debug(f"No requirements.txt found for {app_name}: {str(e)}")
            
            lake_requirements = self._parse_lake_requirements(app_name)
            
            return pypi_dependencies, lake_requirements
            
        except Exception as e:
            self.logger.error(f"Failed to resolve dependencies for {app_name}: {str(e)}")
            return [], []
    
    def _parse_lake_requirements(self, app_name: str) -> List[str]:
        try:
            app_dir = f"{self.artifacts_path}/apps/{app_name}/"
            lake_reqs_path = f"{app_dir}lake-reqs.txt"
            
            content = self.get_platform_service().read(lake_reqs_path)
            
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
    
    def _override_with_workspace_packages(self, dependencies: List[str], config: Dict[str, Any]) -> List[str]:
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
    
    def _install_app_dependencies(self, app_name: str, pypi_dependencies: List[str], 
                                  lake_requirements: List[str]) -> Optional[str]:
        temp_dir = None
        wheels_cache_dir = ""
        
        if lake_requirements:
            temp_dir = tempfile.mkdtemp(prefix=f"app_{app_name}_")
            wheels_cache_dir = self._download_lake_wheels(app_name, lake_requirements, temp_dir)
        
        if wheels_cache_dir and lake_requirements:
            try:
                self.logger.info(f"Installing {len(lake_requirements)} datalake packages")
                
                wheel_files = list(Path(wheels_cache_dir).glob("*.whl"))
                if wheel_files:
                    pip_args = [sys.executable, "-m", "pip", "install"] + [str(wf) for wf in wheel_files] + [
                        "--disable-pip-version-check",
                        "--no-warn-conflicts"
                    ]
                    
                    result = subprocess.run(pip_args, capture_output=True, text=True)
                    
                    if result.returncode == 0:
                        self.logger.info("Datalake wheels installed successfully")
                        
                        for package_spec in lake_requirements:
                            package_name = package_spec.split('==')[0].strip()
                            try:
                                __import__(package_name)
                                self.logger.info(f"Imported {package_name} - decorators executed")
                            except ImportError as e:
                                self.logger.error(f"Failed to import {package_name}: {e}")
                                raise
                    else:
                        self.logger.error(f"Datalake wheel installation failed: {result.stderr}")
                        raise Exception("Datalake wheel installation failed")
                    
            except Exception as e:
                self.logger.error(f"Failed to install datalake wheels: {str(e)}")
                raise
        
        if pypi_dependencies:
            try:
                self.logger.info(f"Installing {len(pypi_dependencies)} PyPI packages")
                
                pip_args = [sys.executable, "-m", "pip", "install"] + pypi_dependencies + [
                    "--disable-pip-version-check",
                    "--no-warn-conflicts"
                ]
                
                if wheels_cache_dir:
                    pip_args.extend(["--find-links", wheels_cache_dir])
                
                result = subprocess.run(pip_args, capture_output=True, text=True)
                
                if result.returncode != 0:
                    self.logger.error(f"PyPI dependency installation failed: {result.stderr}")
                    raise Exception("PyPI dependency installation failed")
                
                self.logger.info("PyPI dependencies installed successfully")
                
            except Exception as e:
                self.logger.error(f"Failed to install PyPI dependencies: {str(e)}")
                raise
        
        if not pypi_dependencies and not lake_requirements:
            self.logger.debug("No dependencies to install")
        
        return temp_dir
    
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
                self.logger.debug(f"Downloading wheel: {wheel_name} from {remote_wheel_path}")

                self.get_platform_service().copy( remote_wheel_path, f"file://{local_wheel_path}", overwrite=True )

                file_size = local_wheel_path.stat().st_size
                total_size += file_size
                
                downloaded_wheels.append(wheel_name)
                
                self.logger.debug(f"Downloaded {wheel_name} ({file_size/1024/1024:.1f}MB)")
                
            except Exception as e:
                self.logger.error(f"Failed to download {wheel_name}: {str(e)}")
        
        self.logger.info(f"Downloaded {len(downloaded_wheels)} wheels, total size: {total_size/1024/1024:.1f}MB")
        return str(wheels_dir)
    
    def _find_best_wheel(self, package_spec: str) -> Optional[str]:
        current_env = self._get_env_name()
        
        if '==' in package_spec:
            package_name, version = package_spec.split('==', 1)
            package_name = package_name.strip()
            version = version.strip()
        else:
            package_name = package_spec.strip()
            version = None
        
        try:
            packages_dir = f"{self.artifacts_path}/packages/"
            all_files = self.get_platform_service().list(packages_dir)
            
            matching_wheels = []
            for file_path in all_files:
                file_name = file_path.split('/')[-1]
                
                if not file_name.endswith('.whl'):
                    continue
                
                wheel_parts = file_name.split('-')
                if len(wheel_parts) < 2:
                    continue
                
                wheel_package_name = wheel_parts[0].replace('_', '-')
                wheel_version = wheel_parts[1] if len(wheel_parts) > 1 else None
                
                if wheel_package_name.lower() != package_name.lower().replace('_', '-'):
                    continue
                
                if version and wheel_version != version:
                    continue
                
                priority = 3
                if f"-{current_env}.whl" in file_name:
                    priority = 1
                elif "-any.whl" in file_name:
                    priority = 2
                
                matching_wheels.append((file_path, file_name, priority, wheel_version))
            
            if not matching_wheels:
                self.logger.warning(f"No wheel found for package: {package_spec}")
                return None
            
            matching_wheels.sort(key=lambda x: (x[2], x[3] or '0'), reverse=False)
            
            best_wheel = matching_wheels[0]
            self.logger.info(f"Selected wheel for {package_spec}: {best_wheel[1]}")
            
            return best_wheel[0]
            
        except Exception as e:
            self.logger.error(f"Failed to find wheel for {package_spec}: {str(e)}")
            return None
    
    def _load_app_code(self, app_name: str, entry_point: str) -> str:
        try:
            app_dir = f"{self.artifacts_path}/apps/{app_name}/"
            code_path = f"{app_dir}{entry_point}"
            
            code_content = self.get_platform_service().read(code_path)
            self.logger.debug(f"Loaded app code from {entry_point} ({len(code_content)} chars)")
            
            return code_content
            
        except Exception as e:
            raise Exception(f"Failed to load app code for {app_name}: {str(e)}")
    
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
                self.logger.warning(f"Failed to cleanup temp directory: {str(e)}")