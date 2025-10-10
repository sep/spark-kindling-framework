# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   }
# META }

# CELL ********************

from typing import Any, Dict, List, Optional, Type, Union
from abc import ABC, abstractmethod
from dynaconf import Dynaconf
from pyspark.sql import SparkSession
  
notebook_import('.injection')
notebook_import(".spark_session")

import tempfile
from pathlib import Path
from typing import List, Optional

def download_config_files(
    artifacts_storage_path: str,
    environment: str,
    app_name: Optional[str] = None
) -> List[str]:
    """
    Download config files from lake to temp location for Dynaconf.
    
    Returns list of local file paths in priority order.
    """
    storage_utils = _get_storage_utils()
    
    if not storage_utils:
        raise Exception("Storage utilities not available for config loading")
    
    # Normalize artifacts_storage_path to relative path for Fabric
    # Remove abfss:// prefix if present
    if artifacts_storage_path.startswith('abfss://'):
        # Extract the path after the container/lakehouse
        # abfss://workspace@storage.../Files/artifacts -> Files/artifacts
        parts = artifacts_storage_path.split('/')
        # Find "Files" or similar directory marker
        try:
            files_index = next(i for i, p in enumerate(parts) if p in ['Files', 'files'])
            artifacts_storage_path = '/'.join(parts[files_index:])
        except StopIteration:
            # If no "Files" found, just take everything after domain
            artifacts_storage_path = '/'.join(parts[3:])  # Skip abfss://domain
    
    #print(f"Using artifacts path: {artifacts_storage_path}")
    
    # Create temp directory for config files
    temp_dir = tempfile.mkdtemp(prefix='kindling_config_')
    temp_path = Path(temp_dir)
    
    config_files = []
    
    # Files to download in priority order (relative paths)
    files_to_download = [
        f"{artifacts_storage_path}/config/settings.yaml",
        f"{artifacts_storage_path}/config/{environment}.yaml",
    ]
    
    # Download each file
    for remote_path in files_to_download:
        filename = remote_path.split('/')[-1]
        local_path = temp_path / f"{len(config_files)}_{filename}"  # Prefix with order
        
        try:
            #print(f"Attempting to download: {remote_path}")
            content = storage_utils.fs.head(remote_path, max_bytes=1024*1024)
            
            # head() returns bytes in Fabric, string in Synapse - handle both
            if isinstance(content, str):
                content = content.encode('utf-8')
            
            local_path.write_bytes(content)
            config_files.append(str(local_path))
            print(f"✓ Downloaded: {remote_path}")
        except Exception as e:
            # Config files are optional except for base settings.yaml
            if 'settings.yaml' in remote_path and not app_name:
                print(f"⚠ Warning: Base config not found at {remote_path}: {e}")
            else:
                print(f"  (Optional config not found: {remote_path})")
            # Continue with other files
    
    # If no files downloaded, create minimal default
    if not config_files:
        print("No config files found, using built-in defaults")
        default_config = temp_path / 'default_settings.yaml'
        default_config.write_text(_get_minimal_default_config())
        config_files.append(str(default_config))
        print("✓ Using built-in default config")
    
    return config_files


def _get_storage_utils():
    """Get platform storage utilities"""
    import __main__
    return getattr(__main__, 'mssparkutils', None) or getattr(__main__, 'dbutils', None)


def _get_minimal_default_config() -> str:
    """Minimal default config for when no files are found"""
    return """
default:
  kindling:
    version: "0.1.0"
    
    bootstrap:
      load_lake: false
      load_local: true
      required_packages: []
      ignored_folders: []
    
    delta:
      tablerefmode: "forName"
      optimize_write: true
    
    telemetry:
      logging:
        level: "INFO"
        print: true
    
    spark_configs: {}
"""

class ConfigService(ABC):
    @abstractmethod
    def get(self, key: str, default: Any = None) -> Any:
        pass
    
    @abstractmethod
    def set(self, key: str, value: Any) -> None:
        pass
    
    @abstractmethod
    def get_all(self) -> Dict[str, Any]:
        pass
    
    @abstractmethod
    def using_env(self, env: str):
        pass
    
    @abstractmethod
    def initialize(
        self,
        config_files: Optional[List[str]] = None,
        initial_config: Optional[Dict[str, Any]] = None,
        environment: str = "development"
    ) -> None:
        """Initialize/reconfigure the config service with actual parameters"""
        pass


@GlobalInjector.singleton_autobind()
class DynaconfConfig(ConfigService):
    def __init__(self):
        """Minimal initialization - will be properly configured via initialize()"""
        self.spark = None
        self.initial_config = {}
        self.dynaconf = None
    
    def initialize(
        self,
        config_files: Optional[List[str]] = None,
        initial_config: Optional[Dict[str, Any]] = None,
        environment: str = "development"
    ) -> None:
        
        print(f"Initializing DynaconfConfig:")
        print(f"  - Config files: {config_files}")
        print(f"  - Bootstrap config keys: {list((initial_config or {}).keys())}")
        
        self.spark = get_or_create_spark_session()
        self.initial_config = initial_config or {}
        
        settings_files = config_files or []
        
        # Load YAML configs first
        self.dynaconf = Dynaconf(
            settings_files=settings_files,
            environments=True,
            env=environment,
            MERGE_ENABLED_FOR_DYNACONF=True,
            envvar_prefix="KINDLING",
        )
        
        # Step 1: Translate YAML (new → old) and add to config
        self._translate_yaml_to_flat()
        
        # Step 2: Translate bootstrap (old → new) and add to config
        self._translate_bootstrap_to_nested()
        
        print(f"✓ DynaconfConfig initialized")

    def _translate_yaml_to_flat(self):
        """Translate YAML's nested keys back to flat bootstrap keys"""
        reverse_mappings = {
            'TELEMETRY.logging.level': 'log_level',
            'TELEMETRY.logging.print': 'print_logging',
            'TELEMETRY.tracing.print': 'print_trace',
            'DELTA.tablerefmode': 'DELTA_TABLE_ACCESS_MODE',
            'BOOTSTRAP.load_local': 'load_local_packages',
            'BOOTSTRAP.load_lake': 'use_lake_packages',
            'REQUIRED_PACKAGES': 'required_packages',
            'IGNORED_FOLDERS': 'ignored_folders',
        }
        
        for new_key, old_key in reverse_mappings.items():
            value = self.dynaconf.get(new_key)
            if value is not None:
                self.dynaconf.set(old_key, value)
                print(f"  - Reverse translation: {new_key} → {old_key} = {value}")

    def _translate_bootstrap_to_nested(self):
        """Translate bootstrap flat keys to nested structure"""
        merged_initial = self._apply_bootstrap_overrides(self.initial_config)
        
        # Apply all bootstrap values
        for key, value in merged_initial.items():
            self._set_nested(key, value)
        
        # Also preserve original flat keys
        for key, value in self.initial_config.items():
            if key not in ['spark_configs']:  # Already handled specially
                self.dynaconf.set(key, value)
                print(f"  - Bootstrap preserved: {key} = {value}")

    def _set_nested(self, key: str, value):
        """Set nested dict values recursively"""
        if isinstance(value, dict):
            for nested_key, nested_value in value.items():
                self._set_nested(f"{key}.{nested_key}", nested_value)
        else:
            self.dynaconf.set(key, value)
            print(f"  - Bootstrap translation: {key} = {value}")    
    
    def _apply_bootstrap_overrides(self, bootstrap_config: Dict) -> Dict:
        """
        Transform flat bootstrap config to match config structure.
        """
        transformed = {}
        processed_keys = set()
        
        # Map flat keys to nested structure where needed
        key_mappings = {
            'log_level': 'TELEMETRY.logging.level',
            'logging_level': 'TELEMETRY.logging.level',
            'print_logging': 'TELEMETRY.logging.print',
            'print_trace': 'TELEMETRY.tracing.print',
            'DELTA_TABLE_ACCESS_MODE': 'DELTA.tablerefmode',
            'load_local_packages': 'BOOTSTRAP.load_local',
            'use_lake_packages': 'BOOTSTRAP.load_lake',
            'required_packages': 'REQUIRED_PACKAGES',
            'ignored_folders': 'IGNORED_FOLDERS',
        }
        
        # Apply known mappings
        for old_key, new_key in key_mappings.items():
            if old_key in bootstrap_config:
                parts = new_key.split('.')
                current = transformed
                for part in parts[:-1]:
                    if part not in current:
                        current[part] = {}
                    current = current[part]
                current[parts[-1]] = bootstrap_config[old_key]
                processed_keys.add(old_key)
        
        # spark_configs at top level
        if 'spark_configs' in bootstrap_config:
            transformed['SPARK_CONFIGS'] = bootstrap_config['spark_configs']
            processed_keys.add('spark_configs')
        
        # ✅ PRESERVE unmapped keys at TOP LEVEL
        for key, value in bootstrap_config.items():
            if key not in processed_keys:
                transformed[key] = value  # Direct top-level assignment
                print(f"  - Preserving bootstrap key: {key} = {value}")
        
        return transformed    

    def get(self, key: str, default: Any = None) -> Any:
        if self.spark:
            try:
                spark_value = self.spark.conf.get(key.upper())
                if spark_value is not None:
                    return spark_value
            except:
                pass
        
        return self.dynaconf.get(key, default)
    
    def set(self, key: str, value: Any) -> None:
        self.dynaconf.set(key, value)
    
    def get_all(self) -> Dict[str, Any]:    
        all_config = {}
        
        for key in self.dynaconf.to_dict().keys():
            all_config[key] = self.dynaconf.get(key)
        
        if self.spark:
            try:
                for key, value in self.spark.conf.getAll():
                    all_config[key] = value
            except:
                pass
        
        return all_config
    
    def using_env(self, env: str):
        return self.dynaconf.using_env(env)
    
    def __getattr__(self, name: str) -> Any:
        if name.startswith('_') or name in ['spark', 'initial_config', 'dynaconf']:
            raise AttributeError(f"'{type(self).__name__}' object has no attribute '{name}'")
    
        value = self.get(name)
        if value is None:
            raise AttributeError(f"No configuration found for '{name}'")
        return value
    
    def reload(self) -> None:
        self.dynaconf.reload()
    
    def get_fresh(self, key: str, default: Any = None) -> Any:       
        if self.spark:
            try:
                spark_value = self.spark.conf.get(key)
                if spark_value is not None:
                    return spark_value
            except:
                pass
        
        return self.dynaconf.get_fresh(key, default=default)


def configure_injector_with_config(
    config_files: Optional[List[str]] = None,
    initial_config: Optional[Dict[str, Any]] = None,
    environment: str = "development",
    artifacts_storage_path: Optional[str] = None
) -> None:
    """
    Configure the GlobalInjector's ConfigService singleton.
    
    Args:
        config_files: List of downloaded config file paths
        initial_config: Bootstrap config overrides
        environment: Environment name (development, production, etc.)
        artifacts_storage_path: Path to artifacts (for reference only)
    """
    print("Setting up config ...")
    print(f"Config files: {config_files}")
    print(f"initial_config : {initial_config}")
    print(f"artifacts_storage_path: {artifacts_storage_path}")
    
    # Get the singleton instance from GlobalInjector (auto-created if needed)
    config_service = GlobalInjector.get(ConfigService)
    
    # Initialize it with proper parameters
    config_service.initialize(
        config_files=config_files,
        initial_config=initial_config,
        environment=environment
    )
    
    print("✓ Config configured in GlobalInjector")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
