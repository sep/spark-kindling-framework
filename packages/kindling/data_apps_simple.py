"""
Updated Data Apps module using simplified platform provider system.

This version eliminates conditional imports and uses the new platform-specific
wheel architecture for cleaner, more maintainable code.
"""

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

# Import the simplified platform provider system
from .platform_provider_simple import (
    get_platform_provider,
    get_available_platforms,
    PlatformProvider
)


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
        """Run a data application"""
        pass


@dataclass
class DataAppConfig:
    """Configuration for a data application"""
    name: str
    version: str = "1.0.0"
    description: str = ""
    entry_point: str = DataAppConstants.DEFAULT_ENTRY_POINT
    requirements: List[str] = None
    lake_requirements: List[str] = None
    environment_configs: Dict[str, Any] = None
    platform_specific: Dict[str, Any] = None

    def __post_init__(self):
        if self.requirements is None:
            self.requirements = []
        if self.lake_requirements is None:
            self.lake_requirements = []
        if self.environment_configs is None:
            self.environment_configs = {}
        if self.platform_specific is None:
            self.platform_specific = {}


@dataclass
class KDAManifest:
    """KDA package manifest"""
    version: str
    created_at: str
    app_config: DataAppConfig
    platform: str
    package_files: List[str]
    wheel_files: List[str] = None

    def __post_init__(self):
        if self.wheel_files is None:
            self.wheel_files = []


class DataAppManager:
    """
    Simplified Data Application Manager using platform-specific wheels.

    This version uses the new platform provider system which eliminates
    conditional imports and provides clean platform separation.
    """

    def __init__(self, platform_name: str = None):
        """
        Initialize DataAppManager with optional platform specification.

        Args:
            platform_name: Explicit platform name or None for auto-detection
        """
        self.platform_provider = get_platform_provider(platform_name)
        self.platform_name = self.platform_provider.get_platform_name()

        print(
            f"🎯 DataAppManager initialized for platform: {self.platform_name}")
        print(f"📋 Available platforms: {get_available_platforms()}")

    def load_app_config(self, app_path: str, environment: str = None) -> DataAppConfig:
        """Load and merge application configuration"""
        app_path = Path(app_path)

        # Load base configuration
        base_config_path = app_path / DataAppConstants.BASE_CONFIG_FILE
        if not base_config_path.exists():
            raise FileNotFoundError(
                f"Base configuration file not found: {base_config_path}")

        with open(base_config_path, 'r') as f:
            config_data = yaml.safe_load(f)

        # Load environment-specific configuration if specified
        if environment:
            env_config_path = app_path / \
                DataAppConstants.ENV_CONFIG_TEMPLATE.format(
                    environment=environment)
            if env_config_path.exists():
                with open(env_config_path, 'r') as f:
                    env_config = yaml.safe_load(f)
                    # Merge environment config into base config
                    config_data = self._merge_config(config_data, env_config)

        # Load requirements files
        requirements = self._load_requirements(
            app_path / DataAppConstants.REQUIREMENTS_FILE)
        lake_requirements = self._load_requirements(
            app_path / DataAppConstants.LAKE_REQUIREMENTS_FILE)

        # Create DataAppConfig
        app_config = DataAppConfig(
            name=config_data.get('name', app_path.name),
            version=config_data.get('version', '1.0.0'),
            description=config_data.get('description', ''),
            entry_point=config_data.get(
                'entry_point', DataAppConstants.DEFAULT_ENTRY_POINT),
            requirements=requirements,
            lake_requirements=lake_requirements,
            environment_configs=config_data.get('environments', {}),
            platform_specific=config_data.get('platforms', {})
        )

        return app_config

    def _merge_config(self, base_config: Dict[str, Any], env_config: Dict[str, Any]) -> Dict[str, Any]:
        """Recursively merge environment configuration into base configuration"""
        merged = base_config.copy()

        for key, value in env_config.items():
            if key in merged and isinstance(merged[key], dict) and isinstance(value, dict):
                merged[key] = self._merge_config(merged[key], value)
            else:
                merged[key] = value

        return merged

    def _load_requirements(self, requirements_path: Path) -> List[str]:
        """Load requirements from file"""
        if not requirements_path.exists():
            return []

        with open(requirements_path, 'r') as f:
            return [line.strip() for line in f if line.strip() and not line.startswith('#')]

    def package_app(self, app_path: str, target_platform: str = None, environment: str = None,
                    output_dir: str = "dist") -> str:
        """
        Package application as KDA (Kindling Data App) for deployment.

        Args:
            app_path: Path to application directory
            target_platform: Target platform (defaults to current platform)
            environment: Environment configuration to include
            output_dir: Output directory for KDA package

        Returns:
            Path to created KDA package
        """
        app_path = Path(app_path)
        output_dir = Path(output_dir)

        if not app_path.exists():
            raise FileNotFoundError(f"Application path not found: {app_path}")

        # Use current platform if target not specified
        if target_platform is None:
            target_platform = self.platform_name

        # Load application configuration
        app_config = self.load_app_config(app_path, environment)

        # Apply platform-specific configuration
        if target_platform in app_config.platform_specific:
            platform_config = app_config.platform_specific[target_platform]
            # Merge platform-specific config
            app_config = self._apply_platform_config(
                app_config, platform_config)

        print(
            f"📦 Packaging {app_config.name} v{app_config.version} for {target_platform}")

        # Create output directory
        output_dir.mkdir(exist_ok=True)

        # Create KDA package
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        kda_filename = f"{app_config.name}_{target_platform}_{timestamp}{DataAppConstants.KDA_EXTENSION}"
        kda_path = output_dir / kda_filename

        with zipfile.ZipFile(kda_path, 'w', zipfile.ZIP_DEFLATED) as kda_zip:
            # Add application files
            package_files = self._add_app_files(kda_zip, app_path)

            # Add wheel files if needed
            wheel_files = self._add_wheel_files(
                kda_zip, app_config, target_platform)

            # Create and add manifest
            manifest = KDAManifest(
                version=DataAppConstants.KDA_VERSION,
                created_at=datetime.now().isoformat(),
                app_config=app_config,
                platform=target_platform,
                package_files=package_files,
                wheel_files=wheel_files
            )

            manifest_json = json.dumps(asdict(manifest), indent=2)
            kda_zip.writestr(DataAppConstants.KDA_MANIFEST_FILE, manifest_json)

        print(f"✅ KDA package created: {kda_path}")
        return str(kda_path)

    def _apply_platform_config(self, app_config: DataAppConfig, platform_config: Dict[str, Any]) -> DataAppConfig:
        """Apply platform-specific configuration to app config"""
        # Create a copy of the config
        updated_config = DataAppConfig(
            name=app_config.name,
            version=app_config.version,
            description=platform_config.get(
                'description', app_config.description),
            entry_point=platform_config.get(
                'entry_point', app_config.entry_point),
            requirements=app_config.requirements +
            platform_config.get('requirements', []),
            lake_requirements=app_config.lake_requirements +
            platform_config.get('lake_requirements', []),
            environment_configs=app_config.environment_configs,
            platform_specific=app_config.platform_specific
        )

        return updated_config

    def _add_app_files(self, kda_zip: zipfile.ZipFile, app_path: Path) -> List[str]:
        """Add application files to KDA package"""
        package_files = []

        for file_path in app_path.rglob('*'):
            if file_path.is_file():
                # Skip hidden files and directories
                if any(part.startswith('.') for part in file_path.parts):
                    continue

                # Skip __pycache__ directories
                if '__pycache__' in file_path.parts:
                    continue

                # Skip .pyc files
                if file_path.suffix == '.pyc':
                    continue

                # Add file to package
                relative_path = file_path.relative_to(app_path)
                kda_zip.write(file_path, f"app/{relative_path}")
                package_files.append(str(relative_path))

        return package_files

    def _add_wheel_files(self, kda_zip: zipfile.ZipFile, app_config: DataAppConfig,
                         target_platform: str) -> List[str]:
        """Add wheel files to KDA package if needed"""
        wheel_files = []

        # Check if we need to include wheels for offline deployment
        # This could be controlled by app configuration
        include_wheels = app_config.platform_specific.get(
            target_platform, {}).get('include_wheels', False)

        if include_wheels:
            # In a real implementation, this would download and include required wheels
            # For now, just log the intention
            print(
                f"📦 Including wheels for offline deployment (platform: {target_platform})")

        return wheel_files

    def deploy_app(self, kda_path: str, deployment_config: Dict[str, Any] = None) -> Dict[str, Any]:
        """
        Deploy KDA package using platform-specific deployment service.

        Args:
            kda_path: Path to KDA package
            deployment_config: Platform-specific deployment configuration

        Returns:
            Deployment result information
        """
        print(f"🚀 Deploying KDA package: {kda_path}")
        print(f"🎯 Target platform: {self.platform_name}")

        # Get platform-specific deployment service
        deployment_service = self._get_deployment_service(
            deployment_config or {})

        if not deployment_service:
            raise RuntimeError(
                f"No deployment service available for platform: {self.platform_name}")

        # Deploy using platform service
        result = deployment_service.deploy_kda(kda_path)

        if result.get('status') == 'success':
            print(f"✅ Deployment successful!")
        else:
            print(
                f"❌ Deployment failed: {result.get('error', 'Unknown error')}")

        return result

    def _get_deployment_service(self, deployment_config: Dict[str, Any]):
        """Get platform-specific deployment service"""
        platform_name = self.platform_name

        try:
            if platform_name == "synapse":
                from .platform_synapse_simple import SynapseAppDeploymentService
                return SynapseAppDeploymentService(
                    workspace_name=deployment_config.get(
                        'workspace_name', 'default'),
                    subscription_id=deployment_config.get(
                        'subscription_id', ''),
                    resource_group=deployment_config.get('resource_group', '')
                )

            elif platform_name == "fabric":
                from .platform_fabric_simple import FabricAppDeploymentService
                return FabricAppDeploymentService(
                    workspace_id=deployment_config.get('workspace_id', ''),
                    tenant_id=deployment_config.get('tenant_id')
                )

            elif platform_name == "databricks":
                from .platform_databricks_simple import DatabricksAppDeploymentService
                return DatabricksAppDeploymentService(
                    workspace_url=deployment_config.get('workspace_url', ''),
                    token=deployment_config.get('token')
                )

            elif platform_name == "local":
                from .platform_local_simple import LocalAppDeploymentService
                return LocalAppDeploymentService(
                    workspace_dir=deployment_config.get(
                        'workspace_dir', './workspace')
                )

            else:
                raise ValueError(f"Unknown platform: {platform_name}")

        except ImportError as e:
            print(f"⚠️  Platform deployment service not available: {e}")
            return None

    def extract_kda(self, kda_path: str, extract_dir: str = None) -> Tuple[DataAppConfig, str]:
        """
        Extract KDA package and return configuration and extraction path.

        Args:
            kda_path: Path to KDA package
            extract_dir: Directory to extract to (defaults to temp directory)

        Returns:
            Tuple of (DataAppConfig, extraction_path)
        """
        if extract_dir is None:
            extract_dir = tempfile.mkdtemp(prefix="kda_extract_")

        extract_path = Path(extract_dir)
        extract_path.mkdir(exist_ok=True)

        # Extract KDA package
        with zipfile.ZipFile(kda_path, 'r') as kda_zip:
            kda_zip.extractall(extract_path)

        # Load manifest
        manifest_path = extract_path / DataAppConstants.KDA_MANIFEST_FILE
        if not manifest_path.exists():
            raise ValueError(f"Invalid KDA package: missing manifest file")

        with open(manifest_path, 'r') as f:
            manifest_data = json.load(f)

        # Convert manifest to objects
        app_config_data = manifest_data['app_config']
        app_config = DataAppConfig(**app_config_data)

        print(f"📂 Extracted KDA: {app_config.name} v{app_config.version}")
        return app_config, str(extract_path)


class KDARunner:
    """
    KDA (Kindling Data App) execution runner.

    This class handles the execution of KDA packages in their target environment.
    """

    def __init__(self, platform_name: str = None):
        """Initialize KDA runner with platform provider"""
        self.platform_provider = get_platform_provider(platform_name)
        self.platform_name = self.platform_provider.get_platform_name()

    def execute_kda(self, kda_path: str, execution_config: Dict[str, Any] = None) -> Dict[str, Any]:
        """
        Execute KDA package.

        Args:
            kda_path: Path to KDA package (local path or storage URL)
            execution_config: Execution configuration

        Returns:
            Execution result
        """
        print(f"🎬 Executing KDA: {kda_path}")
        print(f"🎯 Platform: {self.platform_name}")

        execution_config = execution_config or {}

        try:
            # Extract KDA package
            app_config, extract_path = self._extract_kda(kda_path)

            # Setup execution environment
            self._setup_execution_environment(
                app_config, extract_path, execution_config)

            # Execute the application
            result = self._execute_app(
                app_config, extract_path, execution_config)

            return {
                'status': 'success',
                'app_name': app_config.name,
                'app_version': app_config.version,
                'platform': self.platform_name,
                'result': result
            }

        except Exception as e:
            print(f"❌ KDA execution failed: {e}")
            return {
                'status': 'error',
                'error': str(e),
                'platform': self.platform_name
            }

    def _extract_kda(self, kda_path: str) -> Tuple[DataAppConfig, str]:
        """Extract KDA package"""
        manager = DataAppManager(self.platform_name)
        return manager.extract_kda(kda_path)

    def _setup_execution_environment(self, app_config: DataAppConfig, extract_path: str,
                                     execution_config: Dict[str, Any]):
        """Setup execution environment for the app"""

        # Install requirements if needed
        if app_config.requirements:
            print(f"📦 Installing requirements: {app_config.requirements}")
            # In a real implementation, this would install packages

        # Setup Spark session with platform-specific configuration
        spark_config = execution_config.get('spark_config', {})
        platform_config = app_config.platform_specific.get(
            self.platform_name, {})

        # Merge configurations
        final_config = {**platform_config.get('spark', {}), **spark_config}

        # Create Spark session through platform provider
        if final_config:
            spark = self.platform_provider.create_spark_session(final_config)
            print(f"✨ Created Spark session for {self.platform_name}")

    def _execute_app(self, app_config: DataAppConfig, extract_path: str,
                     execution_config: Dict[str, Any]) -> Any:
        """Execute the application"""

        app_path = Path(extract_path) / "app"
        entry_point = app_path / app_config.entry_point

        if not entry_point.exists():
            raise FileNotFoundError(f"Entry point not found: {entry_point}")

        print(f"🎬 Executing entry point: {app_config.entry_point}")

        # Execute the entry point
        # In a real implementation, this would execute the Python script
        # For now, just simulate execution
        import time
        time.sleep(1)  # Simulate execution time

        return {
            'entry_point': app_config.entry_point,
            'execution_time': 1.0,
            'status': 'completed'
        }

# Convenience functions for backward compatibility


def get_current_platform() -> str:
    """Get current platform name"""
    provider = get_platform_provider()
    return provider.get_platform_name()


def create_data_app_manager(platform_name: str = None) -> DataAppManager:
    """Create DataAppManager instance"""
    return DataAppManager(platform_name)


def package_data_app(app_path: str, target_platform: str = None, environment: str = None) -> str:
    """Package data app as KDA (convenience function)"""
    manager = create_data_app_manager()
    return manager.package_app(app_path, target_platform, environment)
