import logging
import shutil
import tempfile
import threading
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Type, Union

from dynaconf import Dynaconf
from pyspark.sql import SparkSession

from .injection import *
from .spark_session import *

_CONFIG_LOGGER = logging.getLogger("kindling.config")
_MISSING = object()
_CONFIG_FILES_SOURCE_METADATA_KEY = "_kindling_config_files_source_key"


def _config_file_paths(value: Any) -> List[str]:
    if value is None:
        return []
    if isinstance(value, (str, Path)):
        return [str(value)]
    try:
        return [str(path) for path in value]
    except TypeError:
        return [str(value)]


def _path_exists(path: str) -> bool:
    try:
        return Path(path).exists()
    except (OSError, ValueError):
        return False


def _warn_missing_explicit_config_files(initial_config: Dict[str, Any], source_key: str) -> None:
    paths = _config_file_paths(initial_config.get("config_files"))
    if paths and not any(_path_exists(path) for path in paths):
        _CONFIG_LOGGER.warning(
            "None of the explicit configuration paths from %s exist on the local filesystem: %s",
            source_key,
            ", ".join(paths),
        )


def _log_settings_files_load_order(settings_files: List[str]) -> None:
    """Log the Dynaconf settings_files list in merge order (lowest -> highest precedence).

    Files listed later in ``settings_files`` win when the same key appears in
    more than one: mappings deep-merge, lists and scalars replace (see
    ``merge_settings_layers``). Missing files are silently skipped by Dynaconf with no
    complaint, which is easy to mistake for "the key isn't set anywhere" when
    it's really "this file never loaded" — flagging that here up front saves
    a debug cycle.
    """
    if not settings_files:
        _CONFIG_LOGGER.info("Config file load order: none (no settings_files provided)")
        return
    lines = []
    for index, path in enumerate(settings_files):
        exists = Path(path).exists()
        lines.append(f"  [{index}] {'FOUND  ' if exists else 'MISSING'} {path}")
    _CONFIG_LOGGER.info(
        "Config file load order (lowest -> highest precedence):\n" + "\n".join(lines)
    )


_MERGE_MARKER = "dynaconf_merge"
_MERGE_UNIQUE_MARKER = "dynaconf_merge_unique"
_MERGE_TOKEN = "@merge"
_MERGE_UNIQUE_TOKEN = "@merge_unique"


def _parse_merge_token(value: str, token: str) -> Any:
    """`@merge X` / `@merge_unique X`: X as YAML (`[a, b]`, `{k: v}`), or a
    comma-separated list."""
    import yaml

    rest = value[len(token) :].strip()
    try:
        parsed = yaml.safe_load(rest) if rest else []
    except yaml.YAMLError:
        parsed = rest
    if isinstance(parsed, str):
        parsed = [item.strip() for item in parsed.split(",") if item.strip()]
    elif not isinstance(parsed, (list, dict)):
        parsed = [parsed]  # `@merge 3` appends one item
    return parsed


def merge_settings_layers(base: Any, override: Any) -> Any:
    """Merge one settings layer over the layers below it.

    Mappings deep-merge; lists and scalars replace -- the same rule as
    Kindling's config overlays (config_patterns) and `kindling bundle build`,
    so a setting resolves identically everywhere. A layer can append to a list
    on purpose with Dynaconf's markers: a `dynaconf_merge` (or
    `dynaconf_merge_unique`) item in the list, or an `@merge [...]` string. A
    mapping with `dynaconf_merge: false` replaces instead of merging.
    """
    if isinstance(override, str) and override.strip().startswith(_MERGE_TOKEN):
        stripped = override.strip()
        unique = stripped.startswith(_MERGE_UNIQUE_TOKEN)
        override = _parse_merge_token(stripped, _MERGE_UNIQUE_TOKEN if unique else _MERGE_TOKEN)
        if isinstance(base, list) and isinstance(override, list):
            return _append_unique(base, override) if unique else base + override
        if isinstance(base, dict) and isinstance(override, dict):
            return merge_settings_layers(base, override)
        return override
    if isinstance(override, list):
        if _MERGE_UNIQUE_MARKER in override:
            items = [item for item in override if item != _MERGE_UNIQUE_MARKER]
            return _append_unique(base if isinstance(base, list) else [], items)
        if _MERGE_MARKER in override:
            items = [item for item in override if item != _MERGE_MARKER]
            return (base if isinstance(base, list) else []) + items
        return list(override)
    if isinstance(override, dict):
        override = dict(override)
        merge_flag = override.pop(_MERGE_MARKER, True)
        if not isinstance(base, dict) or merge_flag is False:
            return {k: merge_settings_layers(None, v) for k, v in override.items()}
        merged = dict(base)
        for key, value in override.items():
            # Dynaconf keys are case-insensitive: a later layer's TELEMETRY
            # overrides an earlier telemetry (the earlier spelling is kept).
            existing_key = _matching_key(merged, key)
            target = key if existing_key is None else existing_key
            merged[target] = merge_settings_layers(merged.get(target), value)
        return merged
    return override


def _matching_key(mapping: Dict[Any, Any], key: Any) -> Any:
    if key in mapping or not isinstance(key, str):
        return key if key in mapping else None
    lowered = key.lower()
    return next((k for k in mapping if isinstance(k, str) and k.lower() == lowered), None)


def _append_unique(base: List[Any], items: List[Any]) -> List[Any]:
    result = list(base)
    for item in items:
        if item not in result:
            result.append(item)
    return result


def _merged_settings_file(config_files: List[str]) -> Optional[str]:
    """Merge the YAML settings layers (lowest precedence first) into one file
    for Dynaconf, in a private temp directory.

    Kindling merges the layers itself rather than handing Dynaconf the file
    list because Dynaconf (a) appends lists across files, unlike every other
    Kindling merge, and (b) silently loads a `<name>.local.yaml` beside every
    file it loads, which made a developer's settings.local.yaml override
    every environment, and load twice when env=local. `@format`, secrets and
    KINDLING_ environment variables are still resolved by Dynaconf.
    """
    import yaml

    merged: Any = {}
    for path in config_files:
        file_path = Path(path)
        if not file_path.is_file():
            continue
        layer = yaml.safe_load(file_path.read_text(encoding="utf-8")) or {}
        if not isinstance(layer, dict):
            raise ValueError(f"Settings file {path} must contain a mapping at the top level")
        merged = merge_settings_layers(merged, layer)
    if not merged:
        return None
    directory = Path(tempfile.mkdtemp(prefix="kindling-settings-"))
    target = directory / "merged-settings.yaml"
    target.write_text(yaml.safe_dump(merged, sort_keys=False), encoding="utf-8")
    return str(target)


def _build_dynaconf(config_files: Optional[List[str]]) -> Tuple[Dynaconf, Optional[str]]:
    """Dynaconf over the merged layers, plus the merged file's path. The file
    must outlive the instance (Dynaconf reads it lazily and again for
    get_fresh); callers remove it with _remove_snapshot when done."""
    merged_file = _merged_settings_file(list(config_files or []))
    settings = Dynaconf(
        settings_files=[merged_file] if merged_file else [],
        environments=False,
        MERGE_ENABLED_FOR_DYNACONF=True,
        envvar_prefix="KINDLING",
    )
    return settings, merged_file


def _remove_snapshot(merged_file: Optional[str]) -> None:
    if merged_file:
        shutil.rmtree(Path(merged_file).parent, ignore_errors=True)


def peek_settings_value(config_files: Optional[List[str]], key: str, default: Any = None) -> Any:
    """Read one value from explicit settings files, merged as the runtime does."""
    if not config_files:
        return default
    settings, merged_file = _build_dynaconf(config_files)
    try:
        return settings.get(key, default)
    finally:
        _remove_snapshot(merged_file)


# Nested YAML keys mirrored into the flat keys older code reads.
_NESTED_TO_FLAT_KEYS = {
    "kindling.TELEMETRY.logging.level": "log_level",
    "kindling.TELEMETRY.logging.print": "print_logging",
    "kindling.TELEMETRY.tracing.print": "print_trace",
    "kindling.DELTA.access_mode": "DELTA_ACCESS_MODE",
    "kindling.BOOTSTRAP.load_local": "load_local_packages",  # deprecated alias
    "kindling.BOOTSTRAP.load_workspace_packages": "load_workspace_packages",
    "kindling.BOOTSTRAP.load_lake": "use_lake_packages",
    "kindling.BOOTSTRAP.declaration_only": "declaration_only",
    "kindling.BOOTSTRAP.discover_config_files": "discover_config_files",
    "kindling.REQUIRED_PACKAGES": "required_packages",
    "kindling.extensions": "extensions",  # lowercase - matches YAML
    "kindling.EXTENSIONS": "extensions",  # uppercase - backwards compat
    "kindling.IGNORED_FOLDERS": "ignored_folders",
}


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
    def get(self, key: str, default: Any = _MISSING) -> Any:
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

        initial_config_values = dict(initial_config or {})
        config_files_source_key = initial_config_values.pop(
            _CONFIG_FILES_SOURCE_METADATA_KEY, "config_files"
        )
        self.spark = get_or_create_spark_session()
        self.initial_config = initial_config_values
        self._reload_context = reload_context  # Store for hot-reload

        settings_files = config_files or []
        _warn_missing_explicit_config_files(self.initial_config, config_files_source_key)
        _log_settings_files_load_order(settings_files)

        # Load YAML configs first, merged by Kindling (see _merged_settings_file).
        # NOTE: environments=False because Kindling uses separate files (settings.yaml, development.yaml)
        # NOT environment blocks within files (default:, development:)
        self._settings_files = list(settings_files)
        self.dynaconf, self._settings_snapshot = _build_dynaconf(settings_files)

        # Step 1: Translate YAML (new → old) and add to config
        self._translate_yaml_to_flat()

        # Step 2: Translate bootstrap (old → new) and add to config
        self._translate_bootstrap_to_nested()

        _CONFIG_LOGGER.debug("DynaconfConfig initialized")

    def _replace_dynaconf(self, config_files: List[str]) -> None:
        """Swap in a Dynaconf over freshly merged files. The previous snapshot
        is left for _reload to remove once the reload succeeds, since a
        failed reload rolls back to the previous instance."""
        self.dynaconf, self._settings_snapshot = _build_dynaconf(config_files)

    def _translate_yaml_to_flat(self):
        """Translate YAML's nested keys back to flat bootstrap keys"""

        try:
            all_data = self.dynaconf.as_dict()
            _CONFIG_LOGGER.debug("Dynaconf keys loaded from YAML: %s", sorted(all_data.keys()))
        except Exception as e:
            _CONFIG_LOGGER.debug("Error reading Dynaconf keys: %s", e)

        reverse_mappings = _NESTED_TO_FLAT_KEYS

        for new_key, old_key in reverse_mappings.items():
            value = self.dynaconf.get(new_key)
            if value is not None:
                # An alias mirrors the resolved value; never list-merge into it.
                self.dynaconf.set(old_key, value, merge=False)
                _CONFIG_LOGGER.debug("Reverse translation: %s -> %s = %s", new_key, old_key, value)

        try:
            if self.dynaconf.get("kindling.delta.access_mode") is None:
                configured_mode = self.dynaconf.get("kindling.DELTA.access_mode")
                if configured_mode is not None:
                    self.dynaconf.set("kindling.delta.access_mode", configured_mode)
        except Exception:
            pass

    def _translate_bootstrap_to_nested(self):
        """Translate bootstrap flat keys to nested structure.

        Bootstrap keys may be dot-separated flat strings (e.g.
        ``"kindling.secrets.service.api_token"``, as produced by flattening
        a job's ``config_overrides`` for command-line transport) or already-
        nested dicts. Both forms get consolidated into one nested tree per
        top-level namespace, then each namespace is applied with exactly
        one ``dynaconf.set()`` call.

        Setting the whole namespace in one call (instead of one per leaf)
        minimizes the number of merges Dynaconf's own merge/lazy-resolution
        machinery performs against the existing tree, which is the surface
        a known Dynaconf regression (merging into an existing namespace can
        eagerly evaluate an unrelated sibling ``@format`` lazy value, e.g. a
        settings YAML's ``secret_templates.auth_header`` referencing
        ``this.kindling.secrets.service.api_token``, before its target is
        ever set, raising ``DynaconfFormatError``) can trigger. That
        regression is confirmed present in dynaconf 3.3.4 and absent in
        3.3.1/3.2.13; the real fix is the version constraint in
        ``pyproject.toml``, which pins dynaconf below the affected range.
        """
        merged_initial = self._apply_bootstrap_overrides(self.initial_config)

        nested: Dict[str, Any] = {}
        for key, value in merged_initial.items():
            self._merge_dotted_key(nested, key, value)

        # Preserve original flat keys too (kept alongside their transformed
        # form for any caller reading the literal flat key) -- merged into
        # the SAME tree, never a second pass of individual .set() calls.
        for key, value in self.initial_config.items():
            if key != "spark_configs":  # Already handled specially
                self._merge_dotted_key(nested, key, value)

        for top_level_key, value in nested.items():
            self.dynaconf.set(top_level_key, value)

        # A parameter that sets a nested key (e.g. --param
        # kindling.telemetry.logging.level=DEBUG) must also update the flat
        # key code reads (log_level), which _translate_yaml_to_flat copied
        # from the files before parameters applied.
        explicit_flat = {str(k).lower() for k in merged_initial} | {
            str(k).lower() for k in self.initial_config
        }
        for nested_key, flat_key in _NESTED_TO_FLAT_KEYS.items():
            # An explicitly supplied flat key (e.g. spark.kindling.bootstrap.
            # log_level) keeps its own value.
            if flat_key.lower() in explicit_flat:
                continue
            if self._dotted_key_in(nested, nested_key):
                value = self.dynaconf.get(nested_key)
                if value is not None:
                    self.dynaconf.set(flat_key, value, merge=False)

    @staticmethod
    def _dotted_key_in(tree: Dict[str, Any], dotted_key: str) -> bool:
        """Whether dotted_key (case-insensitive) is present in a nested tree."""
        node: Any = tree
        for part in dotted_key.split("."):
            if not isinstance(node, dict):
                return False
            match = next((k for k in node if str(k).lower() == part.lower()), None)
            if match is None:
                return False
            node = node[match]
        return True

    @staticmethod
    def _merge_dotted_key(target: Dict[str, Any], dotted_key: str, value: Any) -> None:
        """Merge a dot-separated key (or a nested-dict value) into `target`.

        Builds/merges intermediate dicts along the path rather than
        overwriting a sibling already placed there by an earlier key in
        the same batch.
        """
        parts = dotted_key.split(".")
        current = target
        for part in parts[:-1]:
            existing = current.get(part)
            if not isinstance(existing, dict):
                existing = {}
                current[part] = existing
            current = existing

        leaf_key = parts[-1]
        if isinstance(value, dict):
            existing_leaf = current.get(leaf_key)
            merged = existing_leaf if isinstance(existing_leaf, dict) else {}
            DynaconfConfig._deep_merge_literal(merged, value)
            current[leaf_key] = merged
        else:
            current[leaf_key] = value

    @staticmethod
    def _deep_merge_literal(target: Dict[str, Any], value: Dict[str, Any]) -> None:
        """Deep-merge dict payloads without interpreting their keys as dotted paths."""
        for key, nested_value in value.items():
            existing = target.get(key)
            if isinstance(existing, dict) and isinstance(nested_value, dict):
                DynaconfConfig._deep_merge_literal(existing, nested_value)
            else:
                target[key] = nested_value

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
            "DELTA_ACCESS_MODE": "DELTA.access_mode",
            "load_local_packages": "BOOTSTRAP.load_local",  # deprecated alias
            "load_workspace_packages": "BOOTSTRAP.load_workspace_packages",
            "use_lake_packages": "BOOTSTRAP.load_lake",
            "declaration_only": "BOOTSTRAP.declaration_only",
            "discover_config_files": "BOOTSTRAP.discover_config_files",
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

        def _set_transformed_dot_key(dot_key: str, value: Any) -> None:
            parts = dot_key.split(".")
            current = transformed
            for part in parts[:-1]:
                if part not in current or not isinstance(current[part], dict):
                    current[part] = {}
                current = current[part]
            current[parts[-1]] = value

        # Provider/global configs should be available under kindling.* as canonical keys,
        # even if bootstrap passes legacy flat keys.
        if "DELTA_ACCESS_MODE" in bootstrap_config:
            _set_transformed_dot_key(
                "kindling.delta.access_mode", bootstrap_config["DELTA_ACCESS_MODE"]
            )
        if "base_checkpoint_path" in bootstrap_config:
            _set_transformed_dot_key(
                "kindling.storage.checkpoint_root", bootstrap_config["base_checkpoint_path"]
            )
        if "temp_path" in bootstrap_config:
            _set_transformed_dot_key("kindling.temp_path", bootstrap_config["temp_path"])
        if "platform" in bootstrap_config:
            _set_transformed_dot_key("kindling.platform.name", bootstrap_config["platform"])
        elif "platform_environment" in bootstrap_config:
            _set_transformed_dot_key(
                "kindling.platform.name", bootstrap_config["platform_environment"]
            )

        if "spark_configs" in bootstrap_config:
            transformed["SPARK_CONFIGS"] = bootstrap_config["spark_configs"]
            processed_keys.add("spark_configs")

        for key, value in bootstrap_config.items():
            if key not in processed_keys:
                transformed[key] = value  # Direct top-level assignment

        return transformed

    def get(self, key: str, default: Any = _MISSING) -> Any:
        with self._config_lock:
            if self.spark:
                try:
                    spark_value = self.spark.conf.get(key.upper())
                    if spark_value is not None:
                        return spark_value
                except Exception:
                    pass

            missing_default = default is type(self).get.__defaults__[0]
            if missing_default:
                value = self.dynaconf.get(key, default)
                if value is default:
                    _CONFIG_LOGGER.debug("Config key %s not found and no default supplied", key)
                    return None
                return value

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
            except Exception:
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

    def _reload_trace_span(self):
        """Standard-tier span for hot reloads.

        Resolved lazily: the trace providers inject ConfigService, so the
        config service cannot hold one from construction. Reloads are rare;
        per-call resolution is fine. Never lets tracing break a reload.
        """
        try:
            from contextlib import nullcontext

            from kindling.trace_ops import COMPONENT_CONFIG, tracing_gates

            if not tracing_gates(self).standard:
                return nullcontext()
            from kindling.injection import GlobalInjector
            from kindling.spark_trace import SparkTraceProvider

            tp = GlobalInjector.get(SparkTraceProvider)
            return tp.span(operation="reload", component=COMPONENT_CONFIG, reraise=True)
        except Exception:
            from contextlib import nullcontext

            return nullcontext()

    def reload(self) -> Dict[str, Any]:
        """Hot-reload configuration from storage.

        Returns:
            Dict with reload summary: {version, changes, status, error}

        Raises:
            ConfigReloadError: If reload fails and cannot rollback
        """
        with self._reload_trace_span():
            return self._reload()

    def _reload(self) -> Dict[str, Any]:
        with self._config_lock:
            old_config = self.get_all()
            old_dynaconf = self.dynaconf
            old_snapshot = getattr(self, "_settings_snapshot", None)

            # Emit pre_reload signal
            if self.pre_reload_signal:
                try:
                    self.pre_reload_signal.send(self, old_config=old_config)
                except Exception as e:
                    _CONFIG_LOGGER.warning("pre_reload signal handler error: %s", e)

            try:
                # If we have reload context, re-download fresh config files
                if self._reload_context:
                    from kindling.bootstrap import download_config_files

                    _CONFIG_LOGGER.info("Reloading configuration from storage")
                    config_files = download_config_files(
                        artifacts_storage_path=self._reload_context["artifacts_storage_path"],
                        environment=self._reload_context["environment"],
                        platform=self._reload_context.get("platform"),
                        workspace_id=self._reload_context.get("workspace_id"),
                        app_name=self._reload_context.get("app_name"),
                    )

                    # Reload Dynaconf with fresh files
                    _log_settings_files_load_order(config_files)
                    self._settings_files = list(config_files)
                    self._replace_dynaconf(config_files)

                    # Re-run translations
                    self._translate_yaml_to_flat()
                    self._translate_bootstrap_to_nested()
                else:
                    # Fallback: reload from existing temp files
                    _CONFIG_LOGGER.info("Reloading configuration from temp files")
                    # Re-merge the source files (the merged file is a snapshot).
                    self._replace_dynaconf(getattr(self, "_settings_files", []))
                    self._translate_yaml_to_flat()
                    self._translate_bootstrap_to_nested()

                # Increment version
                self._version += 1

                # Compute changes
                new_config = self.get_all()
                changes = self._compute_changes(old_config, new_config)

                _CONFIG_LOGGER.info(
                    "Config reloaded (version %s, %s changes)", self._version, len(changes)
                )
                for key, (old_val, new_val) in changes.items():
                    _CONFIG_LOGGER.debug("Config changed: %s: %s -> %s", key, old_val, new_val)

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
                        _CONFIG_LOGGER.warning("post_reload signal handler error: %s", e)

                if self._settings_snapshot != old_snapshot:
                    _remove_snapshot(old_snapshot)
                return result

            except Exception as e:
                # Rollback on failure
                new_snapshot = getattr(self, "_settings_snapshot", None)
                if new_snapshot != old_snapshot:
                    _remove_snapshot(new_snapshot)
                self._settings_snapshot = old_snapshot
                self.dynaconf = old_dynaconf
                error_msg = f"Config reload failed: {e}"
                _CONFIG_LOGGER.error(error_msg)

                # Emit reload_failed signal
                if self.reload_failed_signal:
                    try:
                        self.reload_failed_signal.send(self, error=e, old_config=old_config)
                    except Exception as signal_error:
                        _CONFIG_LOGGER.warning(
                            "reload_failed signal handler error: %s", signal_error
                        )

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

    def get_fresh(self, key: str, default: Any = _MISSING) -> Any:
        with self._config_lock:
            if self.spark:
                try:
                    spark_value = self.spark.conf.get(key)
                    if spark_value is not None:
                        return spark_value
                except Exception:
                    pass

            missing_default = default is type(self).get_fresh.__defaults__[0]
            if missing_default:
                value = self.dynaconf.get_fresh(key, default=default)
                if value is default:
                    _CONFIG_LOGGER.debug("Config key %s not found and no default supplied", key)
                    return None
                return value

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
    # Ensure Dynaconf runs Kindling's custom secret loader.
    from kindling.config_loaders import register_kindling_loaders

    register_kindling_loaders()

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

    _CONFIG_LOGGER.debug("Config configured in GlobalInjector")
