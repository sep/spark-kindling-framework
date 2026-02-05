import tempfile
import threading
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any, Dict, List, Optional, Type, Union

from dynaconf import Dynaconf
from pyspark.sql import SparkSession

from .injection import *
from .spark_session import *


class ConfigService(ABC):
    """Abstract configuration service interface.

    Signals:
        config.pre_reload: Emitted before config reload starts
            Payload: {old_config: Dict[str, Any]}
        config.post_reload: Emitted after config reload completes
            Payload: {changes: Dict[str, tuple], version: int, new_config: Dict[str, Any]}
        config.reload_failed: Emitted if reload fails
            Payload: {error: Exception, old_config: Dict[str, Any]}
    """

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

    @abstractmethod
    def reload(self) -> Dict[str, Any]:
        """Reload configuration from source.

        Returns:
            Dictionary with reload summary: {version, changes, status}
        """
        pass

    @abstractmethod
    def set_entity_tags(self, entityid: str, tags: Dict[str, str]) -> None:
        """Store tag overrides for an entity.

        Args:
            entityid: Entity ID to store tags for
            tags: Dictionary of tag key-value pairs to merge with entity base tags
        """
        pass

    @abstractmethod
    def get_entity_tags(self, entityid: str) -> Dict[str, str]:
        """Get tag overrides for an entity.

        Args:
            entityid: Entity ID to retrieve tags for

        Returns:
            Dictionary of tag overrides (empty dict if none configured)
        """
        pass


@GlobalInjector.singleton_autobind()
class DynaconfConfig(ConfigService):
    def __init__(self):
        """Minimal initialization - will be properly configured via initialize()"""
        self.spark = None
        self.initial_config = {}
        self.dynaconf = None
        self._config_lock = threading.RLock()
        self._version = 0
        self._reload_context = None  # Stores context for hot-reload

        # Create signals for config reload notifications
        from kindling.signaling import SignalProvider

        try:
            signal_provider = get_kindling_service(SignalProvider)
            self.pre_reload_signal = signal_provider.create_signal(
                "config.pre_reload", doc="Emitted before configuration reload starts"
            )
            self.post_reload_signal = signal_provider.create_signal(
                "config.post_reload",
                doc="Emitted after configuration reload completes successfully",
            )
            self.reload_failed_signal = signal_provider.create_signal(
                "config.reload_failed", doc="Emitted when configuration reload fails"
            )
        except Exception:
            # Signals are optional - service works without them
            self.pre_reload_signal = None
            self.post_reload_signal = None
            self.reload_failed_signal = None

    def initialize(
        self,
        config_files: Optional[List[str]] = None,
        initial_config: Optional[Dict[str, Any]] = None,
        environment: str = "development",
        reload_context: Optional[Dict[str, Any]] = None,
    ) -> None:

        print(f"Initializing DynaconfConfig:")

        self.spark = get_or_create_spark_session()
        self.initial_config = initial_config or {}
        self._reload_context = reload_context  # Store for hot-reload

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

        # Debug: Print ALL keys that Dynaconf loaded
        print(f"🔍 DEBUG: All Dynaconf keys loaded from YAML:")
        try:
            all_data = self.dynaconf.as_dict()
            for key in sorted(all_data.keys()):
                value = all_data[key]
                # Show nested structure for debugging
                if isinstance(value, dict):
                    print(f"  {key}: {list(value.keys())}")
                else:
                    print(f"  {key}: {value}")
        except Exception as e:
            print(f"  Error reading Dynaconf keys: {e}")

        reverse_mappings = {
            "kindling.TELEMETRY.logging.level": "log_level",
            "kindling.TELEMETRY.logging.print": "print_logging",
            "kindling.TELEMETRY.tracing.print": "print_trace",
            "kindling.DELTA.tablerefmode": "DELTA_TABLE_ACCESS_MODE",
            "kindling.BOOTSTRAP.load_local": "load_local_packages",
            "kindling.BOOTSTRAP.load_lake": "use_lake_packages",
            "kindling.REQUIRED_PACKAGES": "required_packages",
            "kindling.extensions": "extensions",  # lowercase - matches YAML
            "kindling.EXTENSIONS": "extensions",  # uppercase - backwards compat
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
        with self._config_lock:
            if self.spark:
                try:
                    spark_value = self.spark.conf.get(key.upper())
                    if spark_value is not None:
                        return spark_value
                except:
                    pass

            return self.dynaconf.get(key, default)

    def set(self, key: str, value: Any) -> None:
        with self._config_lock:
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

    def reload(self) -> Dict[str, Any]:
        """Hot-reload configuration from storage.

        Returns:
            Dict with reload summary: {version, changes, status, error}

        Raises:
            ConfigReloadError: If reload fails and cannot rollback
        """
        with self._config_lock:
            old_config = self.get_all()
            old_dynaconf = self.dynaconf

            # Emit pre_reload signal
            if self.pre_reload_signal:
                try:
                    self.pre_reload_signal.send(self, old_config=old_config)
                except Exception as e:
                    print(f"⚠️  pre_reload signal handler error: {e}")

            try:
                # If we have reload context, re-download fresh config files
                if self._reload_context:
                    from kindling.bootstrap import download_config_files

                    print("♻️  Reloading configuration from storage...")
                    config_files = download_config_files(
                        artifacts_storage_path=self._reload_context["artifacts_storage_path"],
                        environment=self._reload_context["environment"],
                        platform=self._reload_context.get("platform"),
                        workspace_id=self._reload_context.get("workspace_id"),
                        app_name=self._reload_context.get("app_name"),
                    )

                    # Reload Dynaconf with fresh files
                    self.dynaconf = Dynaconf(
                        settings_files=config_files,
                        environments=False,
                        MERGE_ENABLED_FOR_DYNACONF=True,
                        envvar_prefix="KINDLING",
                    )

                    # Re-run translations
                    self._translate_yaml_to_flat()
                    self._translate_bootstrap_to_nested()
                else:
                    # Fallback: reload from existing temp files
                    print("♻️  Reloading configuration from temp files...")
                    self.dynaconf.reload()

                # Increment version
                self._version += 1

                # Compute changes
                new_config = self.get_all()
                changes = self._compute_changes(old_config, new_config)

                # Log changes
                print(f"✓ Config reloaded (version {self._version}, {len(changes)} changes)")
                for key, (old_val, new_val) in changes.items():
                    print(f"  {key}: {old_val} → {new_val}")

                result = {
                    "version": self._version,
                    "changes": changes,
                    "status": "success",
                    "change_count": len(changes),
                }

                # Emit post_reload signal
                if self.post_reload_signal:
                    try:
                        self.post_reload_signal.send(
                            self, changes=changes, version=self._version, new_config=new_config
                        )
                    except Exception as e:
                        print(f"⚠️  post_reload signal handler error: {e}")

                return result

            except Exception as e:
                # Rollback on failure
                self.dynaconf = old_dynaconf
                error_msg = f"Config reload failed: {e}"
                print(f"❌ {error_msg}")

                # Emit reload_failed signal
                if self.reload_failed_signal:
                    try:
                        self.reload_failed_signal.send(self, error=e, old_config=old_config)
                    except Exception as signal_error:
                        print(f"⚠️  reload_failed signal handler error: {signal_error}")

                return {
                    "version": self._version,
                    "status": "failed",
                    "error": str(e),
                }

    def _compute_changes(self, old_config: Dict, new_config: Dict) -> Dict[str, tuple]:
        """Compute configuration changes between old and new config.

        Args:
            old_config: Previous configuration
            new_config: New configuration

        Returns:
            Dict mapping changed keys to (old_value, new_value) tuples
        """
        changes = {}
        all_keys = set(old_config.keys()) | set(new_config.keys())

        for key in all_keys:
            old_val = old_config.get(key)
            new_val = new_config.get(key)
            if old_val != new_val:
                changes[key] = (old_val, new_val)

        return changes

    def get_fresh(self, key: str, default: Any = None) -> Any:
        with self._config_lock:
            if self.spark:
                try:
                    spark_value = self.spark.conf.get(key)
                    if spark_value is not None:
                        return spark_value
                except:
                    pass

            return self.dynaconf.get_fresh(key, default=default)

    def set_entity_tags(self, entityid: str, tags: Dict[str, str]) -> None:
        """Store tag overrides for an entity."""
        with self._config_lock:
            # Get entire entity_tags dict, update it, and set it back
            all_entity_tags = self.dynaconf.get("entity_tags", {})
            if not isinstance(all_entity_tags, dict):
                all_entity_tags = {}
            all_entity_tags[entityid] = tags
            self.dynaconf.set("entity_tags", all_entity_tags)

    def get_entity_tags(self, entityid: str) -> Dict[str, str]:
        """Get tag overrides for an entity."""
        with self._config_lock:
            # Get the entire entity_tags dictionary first
            all_entity_tags = self.dynaconf.get("entity_tags", {})
            if not isinstance(all_entity_tags, dict):
                return {}
            # Look up the entityid as a key (entityid may contain dots like "bronze.orders")
            tags = all_entity_tags.get(entityid, {})
            return tags if isinstance(tags, dict) else {}


def configure_injector_with_config(
    config_files: Optional[List[str]] = None,
    initial_config: Optional[Dict[str, Any]] = None,
    environment: str = "development",
    artifacts_storage_path: Optional[str] = None,
    platform: Optional[str] = None,
    workspace_id: Optional[str] = None,
    app_name: Optional[str] = None,
) -> None:
    """
    Configure the GlobalInjector's ConfigService singleton.

    Args:
        config_files: List of downloaded config file paths
        initial_config: Bootstrap config overrides
        environment: Environment name (development, production, etc.)
        artifacts_storage_path: Path to artifacts storage (enables hot-reload)
        platform: Platform name (fabric, synapse, databricks)
        workspace_id: Workspace ID for workspace-specific config
        app_name: Application name for app-specific config
    """
    print("Setting up config ...")
    # print(f"Config files: {config_files}")
    # print(f"initial_config : {initial_config}")
    # print(f"artifacts_storage_path: {artifacts_storage_path}")

    # Build reload context for hot-reload capability
    reload_context = None
    if artifacts_storage_path:
        reload_context = {
            "artifacts_storage_path": artifacts_storage_path,
            "environment": environment,
            "platform": platform,
            "workspace_id": workspace_id,
            "app_name": app_name,
        }

    # Get the singleton instance from GlobalInjector (auto-created if needed)
    config_service = GlobalInjector.get(ConfigService)

    # Initialize it with proper parameters
    config_service.initialize(
        config_files=config_files,
        initial_config=initial_config,
        environment=environment,
        reload_context=reload_context,
    )

    print("✓ Config configured in GlobalInjector")
