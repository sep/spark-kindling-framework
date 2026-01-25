import tempfile
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any, Dict, List, Optional, Type, Union

from dynaconf import Dynaconf
from pyspark.sql import SparkSession

from .injection import *
from .spark_session import *


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
        environment: str = "development",
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
        environment: str = "development",
    ) -> None:

        print(f"Initializing DynaconfConfig:")

        self.spark = get_or_create_spark_session()
        self.initial_config = initial_config or {}

        settings_files = config_files or []

        # Load YAML configs first
        # NOTE: environments=False because Kindling uses separate files (settings.yaml, development.yaml)
        # NOT environment blocks within files (default:, development:)
        self.dynaconf = Dynaconf(
            settings_files=settings_files,
            environments=False,
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
            "kindling.TELEMETRY.logging.level": "log_level",
            "kindling.TELEMETRY.logging.print": "print_logging",
            "kindling.TELEMETRY.tracing.print": "print_trace",
            "kindling.DELTA.tablerefmode": "DELTA_TABLE_ACCESS_MODE",
            "kindling.BOOTSTRAP.load_local": "load_local_packages",
            "kindling.BOOTSTRAP.load_lake": "use_lake_packages",
            "kindling.REQUIRED_PACKAGES": "required_packages",
            "kindling.EXTENSIONS": "extensions",
            "kindling.IGNORED_FOLDERS": "ignored_folders",
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
            if key not in ["spark_configs"]:  # Already handled specially
                self.dynaconf.set(key, value)
                # print(f"  - Bootstrap preserved: {key} = {value}")

    def _set_nested(self, key: str, value):
        """Set nested dict values recursively"""
        if isinstance(value, dict):
            for nested_key, nested_value in value.items():
                self._set_nested(f"{key}.{nested_key}", nested_value)
        else:
            self.dynaconf.set(key, value)
            # print(f"  - Bootstrap translation: {key} = {value}")

    def _apply_bootstrap_overrides(self, bootstrap_config: Dict) -> Dict:
        """
        Transform flat bootstrap config to match config structure.
        """
        transformed = {}
        processed_keys = set()

        # Map flat keys to nested structure where needed
        key_mappings = {
            "log_level": "TELEMETRY.logging.level",
            "logging_level": "TELEMETRY.logging.level",
            "print_logging": "TELEMETRY.logging.print",
            "print_trace": "TELEMETRY.tracing.print",
            "DELTA_TABLE_ACCESS_MODE": "DELTA.tablerefmode",
            "load_local_packages": "BOOTSTRAP.load_local",
            "use_lake_packages": "BOOTSTRAP.load_lake",
            "required_packages": "REQUIRED_PACKAGES",
            "extensions": "EXTENSIONS",
            "ignored_folders": "IGNORED_FOLDERS",
        }

        # Apply known mappings
        for old_key, new_key in key_mappings.items():
            if old_key in bootstrap_config:
                parts = new_key.split(".")
                current = transformed
                for part in parts[:-1]:
                    if part not in current:
                        current[part] = {}
                    current = current[part]
                current[parts[-1]] = bootstrap_config[old_key]
                processed_keys.add(old_key)

        if "spark_configs" in bootstrap_config:
            transformed["SPARK_CONFIGS"] = bootstrap_config["spark_configs"]
            processed_keys.add("spark_configs")

        for key, value in bootstrap_config.items():
            if key not in processed_keys:
                transformed[key] = value  # Direct top-level assignment
                # print(f"  - Preserving bootstrap key: {key} = {value}")

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
        if name.startswith("_") or name in ["spark", "initial_config", "dynaconf"]:
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
    artifacts_storage_path: Optional[str] = None,
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
    # print(f"Config files: {config_files}")
    # print(f"initial_config : {initial_config}")
    # print(f"artifacts_storage_path: {artifacts_storage_path}")

    # Get the singleton instance from GlobalInjector (auto-created if needed)
    config_service = GlobalInjector.get(ConfigService)

    # Initialize it with proper parameters
    config_service.initialize(
        config_files=config_files, initial_config=initial_config, environment=environment
    )

    print("✓ Config configured in GlobalInjector")
