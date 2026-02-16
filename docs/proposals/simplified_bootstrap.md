# Proposal: Simplified Bootstrap & Dynamic Loading

**Status:** In Progress (Partially Implemented)
**Date:** 2026-02-16
**Author:** AI-assisted analysis

## Problem Statement

Kindling's current bootstrapping process is complex, with multiple overlapping paths that have accumulated workarounds and platform-specific hacks. The process involves:

1. **Runtime bootstrap script** (`kindling_bootstrap.py`) that detects the platform, optionally downloads/installs the kindling wheel from datalake, then hands off to...
2. **Framework bootstrap** (`bootstrap.py:initialize_framework()`) which re-detects the platform, downloads config files, configures Dynaconf, installs dependencies/extensions, imports platform modules, and optionally loads workspace notebook packages.
3. **Notebook framework** (`notebook_framework.py`) for loading workspace packages from notebooks via `%run`-equivalent mechanisms.
4. **Data app framework** (`data_apps.py`) with its own dependency resolution, wheel downloading, and code execution pipeline.

The user's stated goal: **"Dynamically load kindling from somewhere if needed, and then initialize, but if kindling is already available (e.g., installed into a cluster directly), we don't need to go looking, we can just initialize."**

## Current Architecture Analysis

### Bootstrap Entry Points (Current)

| Entry Point | Used By | What It Does |
|---|---|---|
| `runtime/scripts/kindling_bootstrap.py` | Fabric/Synapse/Databricks notebooks | Detects platform, installs kindling wheel from lake when needed, calls `initialize_framework()` |
| `kindling.bootstrap.initialize_framework()` | All platforms after wheel install | Config loading, platform init, dependency install, workspace package loading |
| `kindling.initialize()` | Pre-installed clusters / direct import path | Thin wrapper around `initialize_framework()` |
| `kindling.bootstrap_framework()` | Legacy callers | Backward-compat wrapper around `initialize_framework()` |
| `kindling.__init__` | `import kindling` | Eagerly imports ALL 33+ modules at package level |

### Key Pain Points

#### 1. Historical Duplication Across Scripts (Now Reduced)
The Databricks-specific runtime script has been removed and unified into `kindling_bootstrap.py`, eliminating one major source of duplicated logic and platform drift.

#### 2. Double Platform Detection
Platform is detected twice:
1. In the runtime script (to find the right wheel: `kindling_fabric`, `kindling_databricks`, etc.)
2. Again in `initialize_framework()` (to import the right `platform_*.py` module)

#### 3. Heavy-handed Package Installation
`_install_wheel()` performs an aggressive cleanup cycle on every run:
- Lists all installed packages via `pip list`
- Uninstalls all kindling variants (`kindling-fabric`, `kindling-databricks`, `kindling-synapse`, `kindling`)
- Physically deletes kindling directories from all site-packages
- Removes `.dist-info`/`.egg-info` metadata
- Clears `sys.modules`
- Then installs the new wheel

This is necessary because of stale module caching, but it's extremely slow and fragile.

#### 4. Eager Module Loading
`__init__.py` imports all 33+ modules at package load time. This means even `import kindling` triggers loading of every entity provider, every platform utility, etc., regardless of what's needed.

#### 5. Config Loading Complexity
`initialize_framework()` downloads up to 5 config files from lake storage, with environment-specific merging, Dynaconf initialization, bidirectional key translation (flat-to-nested and nested-to-flat), and Spark config overlay. This interleaves with pip operations and platform detection.

#### 6. Debug Noise
Both runtime scripts and `bootstrap.py` contain extensive debug print statements with emoji, execution IDs, timestamps, and PIDs. While useful during development, this makes the logs hard to read in production.

**Important context**: Some of this is unavoidable — the `PythonLoggerProvider` isn't available until after `configure_injector_with_config()` completes, which is well into the bootstrap process. Everything before that point (platform detection, config download, wheel installation, dependency install) has no logging provider and must use `print()`. The issue isn't that `print()` is used pre-logger, but that the output format is inconsistent and verbose even in production.

### Current Flow (Notebook-Based)

```
Notebook Cell 1: Define BOOTSTRAP_CONFIG dict
Notebook Cell 2: %run kindling_bootstrap.py
    |-- Script-level: detect BOOTSTRAP_CONFIG in __main__
    |-- Script-level: platform detection
    |-- Script-level: get_storage_utils()
    |-- bootstrap()
    |   |-- detect_platform() [1st time]
    |   |-- get_storage_utils() [2nd time]
    |   |-- load_config() - config from lake
    |   |-- is_kindling_available()
    |   |-- install_kindling_from_datalake()
    |   |   |-- detect_platform() [2nd time - to find wheel name]
    |   |   |-- install_framework_package()
    |   |   |   |-- get_storage_utils() [3rd time]
    |   |   |   '-- _install_wheel()
    |   |   |       |-- pip list
    |   |   |       |-- pip uninstall (4 variants)
    |   |   |       |-- shutil.rmtree (all site-packages)
    |   |   |       |-- clear sys.modules
    |   |   |       |-- fs.cp (download wheel)
    |   |   |       |-- pip install
    |   |   |       '-- site-packages fixup
    |   |   '-- is_kindling_available() [verify]
    |   '-- initialize_framework()
    |       |-- is_framework_initialized() [guard]
    |       |-- detect_platform() [3rd time - for config]
    |       |-- _get_workspace_id_for_platform()
    |       |-- download_config_files() [5 files]
    |       |-- configure_injector_with_config()
    |       |   '-- DynaconfConfig.initialize()
    |       |-- install_bootstrap_dependencies()
    |       |   |-- required_packages (pip install each)
    |       |   '-- extensions (download + pip install each)
    |       |-- detect_platform() [4th time - for services]
    |       |-- initialize_platform_services()
    |       |   '-- importlib.import_module(platform_{name})
    |       '-- load_workspace_packages() [optional]
    '-- Auto-execute guard at end of script
```

### Current Flow (Cluster-Installed / Pre-Installed)

When kindling is pre-installed on a cluster (via init script or library management), the flow is simpler but still requires the full runtime bootstrap script to be available. There's no clean "just initialize" path — even setting `use_lake_packages=False` still requires running the runtime bootstrap script.

## Proposed Simplification

### Design Principles

1. **Single responsibility**: Separate "get kindling available" from "initialize kindling"
2. **Detect once**: Platform detection happens once and propagates
3. **Skip what's unnecessary**: If kindling is already installed, skip installation entirely
4. **Minimal import cost**: Don't load everything at `import kindling`
5. **One script**: Merge the two nearly-identical runtime scripts into one
6. **Structured logging**: Replace debug prints with proper logging levels

### Proposed Architecture

```
+-------------------------------------+
|         User Entry Point            |
|                                     |
|  Option A: kindling pre-installed   |
|    from kindling import initialize  |
|    initialize(config)               |
|                                     |
|  Option B: kindling not installed   |
|    %run kindling_bootstrap.py       |
|    (or exec() from storage)         |
+------------------+------------------+
                   |
                   v
+--------------------------------------+
|     Phase 1: Ensure Availability     |
|     (kindling_bootstrap.py)          |
|                                      |
|  1. is_kindling_available()?         |
|     +-- Yes -> skip to Phase 2      |
|     +-- No  -> install from lake    |
|                                      |
|  2. Install (if needed):            |
|     - Detect platform (once)        |
|     - Download wheel from lake      |
|     - pip install wheel             |
|                                      |
|  3. Hand off to Phase 2             |
+------------------+-------------------+
                   |
                   v
+--------------------------------------+
|     Phase 2: Initialize Framework    |
|     (kindling.initialize())          |
|                                      |
|  Uses Phase 1 platform if available  |
|                                      |
|  1. Load configuration              |
|     - Download YAML configs from lake|
|     - Initialize Dynaconf           |
|     - Merge bootstrap overrides     |
|                                      |
|  2. Install dependencies            |
|     - Required packages (PyPI)      |
|     - Extensions (lake wheels)      |
|                                      |
|  3. Initialize platform services    |
|     - Import platform module        |
|     - Register platform service     |
|                                      |
|  4. Load workspace packages         |
|     - Discover notebook packages    |
|     - Import as Python modules      |
|                                      |
|  5. Run data app (if specified)     |
+--------------------------------------+
```

### Key Changes

#### 1. Unified Runtime Bootstrap Script

`kindling_bootstrap.py` is now the single runtime script with platform-aware branches:

```python
# kindling_bootstrap.py (unified)

def ensure_kindling(config: dict) -> bool:
    """Ensure kindling is available. Returns True if initialization needed."""

    # Fast path: already installed and no force reinstall
    if _is_kindling_available() and not config.get("force_reinstall", False):
        return True

    # Slow path: install from lake
    platform = _detect_platform(config)
    storage = _get_storage_utils()

    wheel_path = _find_wheel(storage, config, platform)
    _install_wheel(wheel_path, storage, platform)

    return _is_kindling_available()

def bootstrap(config: dict):
    """Full bootstrap: ensure + initialize."""
    if not ensure_kindling(config):
        raise RuntimeError("Failed to make kindling available")

    from kindling import initialize
    initialize(config)
```

**Result**: ~800 lines eliminated, one file to maintain instead of two.

#### 2. Clean `initialize()` Entry Point

Add a top-level `kindling.initialize()` function that replaces the need for the runtime bootstrap script when kindling is pre-installed:

```python
# kindling/__init__.py

def initialize(config: dict = None, app_name: str = None):
    """Initialize the Kindling framework.

    This is the primary entry point when kindling is already installed
    (e.g., via cluster library management, pip install, or init script).

    Args:
        config: Configuration dictionary. Can include:
            - artifacts_storage_path: Lake path for configs/packages
            - environment: dev/staging/prod
            - platform: explicit platform override
            - app_name: data app to auto-run
        app_name: Shorthand for config["app_name"]
    """
    from kindling.bootstrap import initialize_framework

    config = config or {}
    if app_name:
        config["app_name"] = app_name

    return initialize_framework(config)
```

**Usage when pre-installed**:
```python
# Notebook cell 1 - that's it
import kindling
kindling.initialize({
    "artifacts_storage_path": "abfss://...",
    "environment": "production"
})
```

**Usage when not pre-installed**:
```python
# Notebook cell 1
BOOTSTRAP_CONFIG = {
    "artifacts_storage_path": "abfss://...",
    "environment": "production"
}
# Notebook cell 2
%run kindling_bootstrap.py
```

#### 3. Lazy Module Loading

Replace the eager `__init__.py` with lazy imports:

```python
# kindling/__init__.py

# Only import the essentials at package level
from .bootstrap import initialize_framework, is_framework_initialized

def initialize(config=None, app_name=None):
    """Public initialize entry point."""
    config = config or {}
    if app_name:
        config["app_name"] = app_name
    return initialize_framework(config)

# Lazy imports for everything else
def __getattr__(name):
    """Lazy-load modules on first access."""
    import importlib
    submodules = {
        'data_entities', 'data_pipes', 'entity_provider',
        'entity_provider_delta', 'entity_provider_csv',
        'entity_provider_eventhub', 'entity_provider_memory',
        'entity_provider_registry', 'execution_strategy',
        'file_ingestion', 'watermarking', 'signaling',
        'pipe_streaming', 'common_transforms', 'spark_jdbc',
        # ... etc
    }
    if name in submodules:
        return importlib.import_module(f'.{name}', __name__)
    raise AttributeError(f"module 'kindling' has no attribute {name}")
```

**Caveat**: The `GlobalInjector.singleton_autobind()` pattern relies on decorators executing at import time. Lazy imports would defer this registration. Need to verify that `initialize_framework()` doesn't depend on all providers being registered before it runs. Initial analysis suggests it only needs `ConfigService`, `PlatformServiceProvider`, and `PythonLoggerProvider` during bootstrap — the rest can be lazy. However, the `@GlobalInjector.singleton_autobind()` decorator is used on ~20+ classes, and the injector needs to know about implementations before they can be resolved. This may require an explicit "register all providers" step in `initialize_framework()` rather than relying on import-time side effects.

**Safer alternative**: Keep eager imports but make them explicit rather than star-imports, and ensure they don't trigger platform-specific code at import time.

#### 4. Detect Once, Propagate

Create a `BootstrapContext` that carries platform info through the pipeline:

```python
@dataclass
class BootstrapContext:
    platform: str
    storage_utils: Any  # mssparkutils or dbutils
    config: dict
    workspace_id: Optional[str] = None
    temp_path: Optional[str] = None
```

Instead of calling `detect_platform()` 4 times and `get_storage_utils()` 3 times, detect once and pass the context:

```python
def initialize_framework(config, context: BootstrapContext = None):
    if context is None:
        context = BootstrapContext(
            platform=detect_platform(config),
            storage_utils=_get_storage_utils(),
            config=config,
        )
    # Use context.platform everywhere instead of re-detecting
```

#### 5. Conditional Installation Strategy

Replace the aggressive "uninstall everything, reinstall" approach with a smarter check:

```python
def _should_install(config: dict) -> bool:
    """Determine if installation is needed."""

    # Force reinstall requested
    if config.get("force_reinstall", False):
        return True

    # Not installed at all
    if not _is_kindling_available():
        return True

    # Version mismatch (check if specific version requested)
    requested_version = config.get("kindling_version")
    if requested_version and requested_version != "latest":
        installed_version = _get_installed_version()
        if installed_version != requested_version:
            return True

    return False
```

This eliminates the expensive pip list/uninstall/rmtree/install cycle when kindling is already at the right version.

#### 6. Structured Logging

The `PythonLoggerProvider` isn't available until after Dynaconf config is loaded and the injector is configured — that's midway through the bootstrap process. Everything before that point (runtime script, platform detection, config download, wheel install) *must* use `print()`. The goal isn't to eliminate `print()` from early bootstrap, but to make the pre-logger output consistent and respect a verbosity level:

```python
class BootstrapLogger:
    """Lightweight print-based logger for pre-injector bootstrap phases.

    Used before PythonLoggerProvider is available. After injector setup,
    bootstrap switches to the real logger automatically.
    """

    def __init__(self, level="INFO"):
        self.level = level
        self._start_time = time.time()

    def phase(self, name: str):
        """Mark start of a bootstrap phase (always shown)."""
        elapsed = time.time() - self._start_time
        print(f"[kindling {elapsed:.1f}s] {name}")

    def detail(self, msg: str):
        """Detail message (only shown at DEBUG level)."""
        if self.level == "DEBUG":
            elapsed = time.time() - self._start_time
            print(f"[kindling {elapsed:.1f}s]   {msg}")

    def success(self, msg: str):
        print(f"[kindling] OK: {msg}")

    def error(self, msg: str):
        print(f"[kindling] ERROR: {msg}")

    def upgrade_to_provider(self, logger_provider):
        """Switch to real logger once PythonLoggerProvider is available."""
        # Future: could swap internal methods to delegate to the real logger
        pass
```

The key improvement is respecting a verbosity setting (e.g., from `BOOTSTRAP_CONFIG["log_level"]`) even during early bootstrap, so production runs show phase-level output while debug runs show the full detail.

## Migration Path

### Phase 1: Unify Bootstrap Scripts (Low Risk) — Complete
1. Merged to a single runtime script (`kindling_bootstrap.py`)
2. Added `kindling.initialize()` as a thin wrapper
3. Existing notebooks using `%run kindling_bootstrap.py` continue to work
4. Runtime behavior updated to skip lake install when kindling is already available (unless force reinstall)

### Phase 2: Clean Up `initialize_framework()` (Medium Risk)
1. Introduce `BootstrapContext` to avoid redundant detection
2. Add `_should_install()` to skip unnecessary reinstalls
3. Replace debug prints with structured logging
4. Keep backward-compatible `bootstrap_framework()` wrapper

### Phase 3: Optimize Import Chain (Higher Risk)
1. Evaluate lazy loading for `__init__.py`
2. Test that `GlobalInjector` decorators still register correctly
3. Profile import time before/after
4. This phase may not be worth the risk if `import kindling` is fast enough

### Phase 4: Simplify Config Loading (Medium Risk)
1. Separate config loading from initialization
2. Allow config to be passed directly (no lake download needed for simple cases)
3. Support both "config from lake" and "config from dict" cleanly
4. Consider allowing initialization without any lake connectivity (pure local mode)

## Impact Assessment

| Change | Lines Removed | Risk | User-Facing Impact |
|---|---|---|---|
| Merge runtime scripts | ~750 | Low | None — same behavior |
| Add `kindling.initialize()` | +20 | None | New, simpler API |
| `BootstrapContext` | ~50 removed | Low | Faster initialization |
| Smart install check | ~100 removed | Low | Much faster when pre-installed |
| Structured logging | ~200 removed | Low | Cleaner logs |
| Lazy `__init__.py` | Net zero | Medium | Faster `import kindling` |

## User Experience: Before vs After

### Before (current)
```python
# Even when kindling is pre-installed on the cluster:
BOOTSTRAP_CONFIG = {
    "artifacts_storage_path": "abfss://container@storage.dfs.core.windows.net/artifacts",
    "environment": "production",
    "platform": "fabric",
    "use_lake_packages": True,  # still downloads & reinstalls wheel!
}
%run kindling_bootstrap.py
# -> 30-60 seconds of pip operations even though kindling is already there
```

### After (proposed)
```python
# When kindling is pre-installed on the cluster:
import kindling
kindling.initialize({
    "artifacts_storage_path": "abfss://container@storage.dfs.core.windows.net/artifacts",
    "environment": "production",
})
# -> ~5 seconds (config download + platform init only, no pip)
```

```python
# When kindling is NOT pre-installed:
BOOTSTRAP_CONFIG = {
    "artifacts_storage_path": "abfss://container@storage.dfs.core.windows.net/artifacts",
    "environment": "production",
}
%run kindling_bootstrap.py
# -> Same as before, but with unified script and smarter install check
```

## Relationship to Other Proposals

- **[Single Notebook Bootstrap](single_notebook_bootstrap.md)**: The single-notebook approach embeds the wheel inline. This proposal complements it — if kindling is already available (e.g., from the embedded notebook), `kindling.initialize()` is the entry point.
- **[Domain Package Development](domain_package_development.md)**: Workspace packages loaded during bootstrap. The simplified bootstrap still supports this via the `load_workspace_packages()` step, but makes it optional.

## Open Questions

1. **`use_lake_packages` behavior**: Resolved for runtime bootstrap. Default behavior is now **install if missing** (skip reinstall when already available), with `force_reinstall=True` as explicit override.

2. **Version pinning**: When kindling is pre-installed at version X but the lake has version Y, what should happen? Options:
   - Always use the installed version (fast, but may be stale)
   - Always upgrade from lake (slow, but consistent)
   - Compare versions and upgrade only if lake is newer (best UX, medium complexity)

3. **Eager vs lazy imports**: The `GlobalInjector.singleton_autobind()` pattern relies on decorators executing at import time. Lazy imports would defer this. Need to verify that `initialize_framework()` doesn't depend on all providers being registered before it runs.

4. **Backward compatibility for `BOOTSTRAP_CONFIG`**: The runtime scripts check for `BOOTSTRAP_CONFIG` in `__main__`. Should the new unified script support this pattern, or should users migrate to passing config as a function argument? Recommend supporting both during transition.

5. **Data app bootstrap**: `DataAppManager` has its own parallel dependency resolution and installation pipeline. Should this be unified with the main bootstrap dependency installation, or is the separation intentional (app dependencies are separate from framework dependencies)?
