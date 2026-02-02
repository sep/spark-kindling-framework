# Package Configuration & Declaration Override Architecture

> **Created:** 2026-02-02
> **Status:** Design Proposal (Under Review)
> **Context:** Extending configuration system to support package-level config and decorator overrides

---

## Critical Evaluation

### ✅ What Makes Sense

1. **Hierarchical Config Precedence** - The layered approach (package → settings → platform → env) is sound and mirrors how most config systems work (Kubernetes ConfigMaps, Spring profiles, etc.)

2. **Code-First with Config Override** - Decorators define the source of truth, config augments. This is the right default - code is versioned and tested, config is deployment-specific.

3. **Deep Merge for Tags** - Tags are additive metadata, so deep merge (add new, override existing) is correct. You want to add compliance tags without losing domain tags.

4. **Feature Flags (enabled/disabled)** - Simple, powerful pattern for toggling pipes/entities without code changes.

5. **Backward Compatibility** - Optional package_name parameter means existing code works unchanged.

### ❌ What Doesn't Make Sense

1. **Package Config Registration is Boilerplate-Heavy**
   - Requiring `register_package_config()` in `__init__.py` is tedious
   - Forces package authors to write infrastructure code
   - **Better:** Auto-discover `kindling_config.yaml` in package root via importlib.resources

2. **Applying Overrides at Decorator Execution Time is Problematic**
   - Decorators run at import time, before bootstrap/config is loaded
   - Config service may not exist yet → try/except fallback is fragile
   - **Better:** Apply overrides at registration time in the registry, not in decorator

3. **Package Namespace in Decorator is Wrong Location**
   - `@DataPipes.pipe(package_name="my_sales")` puts infrastructure concern in business code
   - Package should declare its namespace once, not on every decorator
   - **Better:** Package-level declaration or auto-detect from module path

4. **Missing Wildcard/Pattern Matching**
   - Can't say "all bronze.* pipes get these tags"
   - Leads to config duplication across many entities/pipes
   - **Essential:** Support `bronze.*`, `*.ingest_*`, `**` patterns

5. **No Config-Only Declarations**
   - Can't define a pipe/entity entirely in config (without code)
   - Useful for: wrappers, virtual entities, environment-specific additions
   - **Consider:** Allow config to declare items that don't exist in code

6. **Missing Tag Removal Capability**
   - Can only add/override tags, can't remove one
   - Sometimes need to remove a tag in production
   - **Add:** Special syntax like `tags: { remove: ["debug", "test"] }`

7. **Conflicting Design: Package Config vs No Package Namespacing**
   - If config is `my_sales.datapipes.bronze.ingest`, user must know package name
   - But if no package_name in decorator, how does system know which package?
   - **Clarify:** Need consistent namespacing strategy

### ⚠️ Gray Areas Needing Further Thought

1. **Should Config Override input_entity_ids?**
   - Changing inputs could break transform logic expecting specific schemas
   - Very powerful but dangerous
   - **Maybe:** Allow but with explicit `allow_input_override: true` flag

2. **When Does Config Apply?**
   - At import time (current)? At bootstrap time? At execution time?
   - Each has tradeoffs for testability, hot-reload, predictability
   - **Recommend:** Bootstrap time, with explicit reload capability

3. **Validation Strictness**
   - Should unknown config keys error or warn?
   - Strict catches typos, lenient allows forward compatibility
   - **Recommend:** Warn by default, strict mode optional

---

## Problem Statement

**Current Limitation:** Configuration is entirely Kindling-framework-specific. Packages (domain logic with DataPipes, DataEntities) have no configuration mechanism.

**User Need:**
1. **Package-level configuration** - Domain packages need their own configurable settings
2. **Declaration overrides** - Config should augment/override code-first `@pipe` and `@entity` decorators
3. **Environment flexibility** - Override tags, properties across dev/staging/prod without code changes
4. **Team autonomy** - Different teams can configure the same package differently

**Example Use Cases:**

```python
# Code declares:
@DataPipes.pipe(
    pipeid="bronze.ingest_orders",
    tags={"domain": "sales", "priority": "high"},
    input_entity_ids=["source.orders"],
    output_entity_id="bronze.orders"
)
def ingest_orders(source_orders):
    return transform(source_orders)

# Config overrides in production:
datapipes:
  bronze.ingest_orders:
    tags:
      priority: "critical"      # Override existing
      owner: "team-a"           # Add new
      pii: "true"               # Add compliance tag
    enabled: true               # Feature flag
```

---

## Design Goals

1. **Backward Compatible** - Existing code works without config changes
2. **Hierarchical** - Package config respects platform/workspace/env hierarchy
3. **Code-First Default** - Decorators are source of truth, config provides overrides
4. **Type Safe** - Config overrides validated against decorator schema
5. **Discoverable** - Easy to see what's configurable and what's overridden
6. **Package Portable** - Packages can ship with default config
7. **Flexible Placement** - Config can live in package, base settings, or app settings

---

## Architecture Overview

### Configuration Precedence (Lowest → Highest Priority)

```
1. Code Defaults (decorator parameters)
     ↓
2. Package Default Config (package's config.yaml - optional)
     ↓
3. Base Settings (settings.yaml)
     ↓
4. Platform Config (platform_fabric.yaml)
     ↓
5. Workspace Config (workspace_{id}.yaml)
     ↓
6. Environment Config (env_prod.yaml)
     ↓
7. App Config (data-apps/{app}/settings.yaml)
     ↓
8. Runtime Overrides (BOOTSTRAP_CONFIG)
```

Each layer can override or augment the previous layers.

---

## Option Analysis

### Option A: Package-Embedded Config Only

**Structure:**
```
my-sales-package/
  my_sales/
    __init__.py
    pipes.py
    entities.py
    config.yaml          # Package default config
```

**Pros:**
- ✅ Self-contained, portable
- ✅ Package ships with sensible defaults
- ✅ Clear ownership

**Cons:**
- ❌ Hard to override across environments
- ❌ Requires package rebuild for config changes
- ❌ No central visibility

**Verdict:** ❌ Not flexible enough for enterprise needs

---

### Option B: Centralized Config Only

**Structure:**
```
settings.yaml:
  kindling:
    version: "0.2.0"

  my_sales_package:            # Package section
    datapipes:
      bronze.ingest_orders:
        tags: {...}
    dataentities:
      bronze.orders:
        tags: {...}
```

**Pros:**
- ✅ Central control
- ✅ Environment overrides work naturally
- ✅ No package changes needed

**Cons:**
- ❌ Base config grows large
- ❌ Packages not portable
- ❌ Package defaults duplicated in every deployment

**Verdict:** ❌ Not portable enough for reusable packages

---

### Option C: Hierarchical Package Config (Recommended) ✅

**Structure:**
```
my-sales-package/
  my_sales/
    __init__.py
    pipes.py
    entities.py
    default_config.yaml      # OPTIONAL package defaults

workspace/
  config/
    settings.yaml             # Can override package config
    platform_fabric.yaml      # Platform overrides
    workspace_abc.yaml        # Workspace overrides
    env_prod.yaml             # Environment overrides
```

**Config Merge Example:**

```yaml
# my-sales-package/my_sales/default_config.yaml (shipped with package)
my_sales_package:
  datapipes:
    bronze.ingest_orders:
      tags:
        domain: "sales"
        priority: "medium"
      retry_count: 3
      timeout_seconds: 300

# settings.yaml (deployment-specific)
my_sales_package:
  datapipes:
    bronze.ingest_orders:
      tags:
        priority: "high"        # Override
        owner: "team-a"         # Add new
      timeout_seconds: 600      # Override

# env_prod.yaml (production overrides)
my_sales_package:
  datapipes:
    bronze.ingest_orders:
      tags:
        priority: "critical"    # Override again
        audit: "true"           # Add compliance
```

**Final Merged Config:**
```yaml
my_sales_package:
  datapipes:
    bronze.ingest_orders:
      tags:
        domain: "sales"         # From package default
        priority: "critical"    # From env_prod.yaml (highest)
        owner: "team-a"         # From settings.yaml
        audit: "true"           # From env_prod.yaml
      retry_count: 3            # From package default
      timeout_seconds: 600      # From settings.yaml
```

**Pros:**
- ✅ Package portability (ships with defaults)
- ✅ Deployment flexibility (override anything)
- ✅ Environment-aware (hierarchical config applies)
- ✅ Clear precedence rules
- ✅ Best of both worlds

**Cons:**
- ⚠️ More complex precedence rules (but well-defined)
- ⚠️ Need to discover package configs

**Verdict:** ✅ **RECOMMENDED** - Balances portability with flexibility

---

## Revised Design: Addressing the Issues

### Key Changes from Original Proposal

| Issue | Original Approach | Revised Approach |
|-------|-------------------|------------------|
| Package registration | Explicit `register_package_config()` | Auto-discover `kindling.yaml` in package |
| Namespace declaration | `package_name` param on every decorator | Single `__kindling_package__` in `__init__.py` |
| Override timing | At decorator execution (import time) | At bootstrap/registration time |
| Wildcard support | None | Full glob pattern matching |
| Tag removal | Not possible | `_remove_tags: [...]` syntax |

### Wildcard Pattern Matching

**Syntax:**
- `*` - Match any single segment (e.g., `bronze.*` matches `bronze.orders`, `bronze.customers`)
- `**` - Match any number of segments (e.g., `**.ingest` matches `bronze.ingest`, `silver.etl.ingest`)
- `?` - Match any single character (e.g., `bronze.order?` matches `bronze.order1`, `bronze.orders`)

**Pattern Precedence (Most Specific Wins):**
```
1. Exact match:     bronze.ingest_orders     (highest priority)
2. Single wildcard: bronze.*
3. Multi wildcard:  **                       (lowest priority)
```

**Example Config with Wildcards:**

```yaml
datapipes:
  # Apply to ALL pipes (lowest priority)
  "**":
    tags:
      framework: "kindling"
      managed: "true"

  # Apply to all bronze layer pipes
  "bronze.*":
    tags:
      layer: "bronze"
      sla: "4h"
    timeout_seconds: 300

  # Apply to all silver layer pipes
  "silver.*":
    tags:
      layer: "silver"
      sla: "2h"
    timeout_seconds: 600

  # Apply to all gold layer pipes
  "gold.*":
    tags:
      layer: "gold"
      sla: "1h"
    timeout_seconds: 900

  # Apply to any ingest pipe at any layer
  "*.ingest_*":
    tags:
      type: "ingestion"
    retry_count: 5

  # Exact match override (highest priority)
  bronze.ingest_orders:
    tags:
      priority: "critical"
      owner: "team-a"
    timeout_seconds: 600  # Override the bronze.* default

dataentities:
  # All bronze entities get these defaults
  "bronze.*":
    tags:
      layer: "bronze"
      retention: "90d"
    delta_properties:
      delta.logRetentionDuration: "interval 30 days"

  # All entities with PII
  "*.customers":
    tags:
      pii: "true"
      gdpr: "applicable"
```

**Resolution Example:**

For pipe `bronze.ingest_orders`:
```yaml
# Matches (in order of application):
1. "**"                    → tags: {framework: "kindling", managed: "true"}
2. "bronze.*"              → tags: {layer: "bronze", sla: "4h"}, timeout: 300
3. "*.ingest_*"            → tags: {type: "ingestion"}, retry_count: 5
4. "bronze.ingest_orders"  → tags: {priority: "critical", owner: "team-a"}, timeout: 600

# Final merged result:
tags:
  framework: "kindling"   # from **
  managed: "true"         # from **
  layer: "bronze"         # from bronze.*
  sla: "4h"               # from bronze.*
  type: "ingestion"       # from *.ingest_*
  priority: "critical"    # from exact match
  owner: "team-a"         # from exact match
timeout_seconds: 600      # from exact match (overrode bronze.*)
retry_count: 5            # from *.ingest_*
```

### Tag Removal Syntax

Sometimes you need to REMOVE a tag that was added by a wildcard pattern:

```yaml
datapipes:
  # All pipes get debug tag
  "**":
    tags:
      debug: "true"

  # Production: remove debug tag from all pipes
  # (in env_prod.yaml)
  "**":
    _remove_tags:
      - debug
```

**Special Config Keys:**
- `_remove_tags: [...]` - Remove these tags (applied after merges)
- `_remove_all_tags: true` - Clear all tags, start fresh
- `_enabled: false` - Disable this pipe/entity

### Simplified Package Declaration

**Instead of registration boilerplate, use convention:**

```python
# my_sales/__init__.py
__kindling_package__ = "my_sales"  # Just this one line!
```

**Or auto-detect from module name:**
```python
# If no __kindling_package__, derive from __name__
# my_sales.pipes → package = "my_sales"
```

**Package config file convention:**
```
my_sales/
  __init__.py                # Contains __kindling_package__ = "my_sales"
  kindling.yaml              # Auto-discovered package defaults (optional)
  pipes.py
  entities.py
```

### Config Namespacing Strategy

**Option 1: Global Namespace (Simpler)**
```yaml
# settings.yaml - No package prefixes, just IDs
datapipes:
  bronze.ingest_orders:    # Globally unique pipe ID
    tags: {...}

dataentities:
  bronze.orders:           # Globally unique entity ID
    tags: {...}
```

**Pros:** Simpler, IDs already include layer prefix, no package mapping needed
**Cons:** ID collisions possible across packages (unlikely with layer.name convention)

**Option 2: Package Namespace (More Explicit)**
```yaml
# settings.yaml - Package prefixes required
packages:
  my_sales:
    datapipes:
      bronze.ingest_orders:
        tags: {...}

  my_inventory:
    datapipes:
      bronze.ingest_stock:
        tags: {...}
```

**Pros:** Explicit, no collision possible, clear ownership
**Cons:** More verbose, need to know package name

**Recommendation: Global Namespace as Default**

- Pipe/entity IDs like `bronze.orders`, `silver.customers` are already namespaced by layer
- Package prefix is rarely needed in practice
- Keep option for package namespace when needed (via `__kindling_package__`)

```yaml
# Default: Global namespace (recommended for most cases)
datapipes:
  bronze.ingest_orders:
    tags: {...}

# Optional: Package namespace (when needed for disambiguation)
packages:
  vendor_package:
    datapipes:
      bronze.ingest_orders:  # Different from global bronze.ingest_orders
        tags: {...}
```

---

## Alternative Approaches

### Alternative A: Annotation-Based Override (Rejected)

Use Python annotations instead of YAML config:

```python
@DataPipes.pipe(
    pipeid="bronze.ingest_orders",
    tags=ConfigOverride("my_sales.datapipes.bronze.ingest_orders.tags"),
    timeout=ConfigRef("my_sales.timeout", default=300)
)
```

**Rejected Because:**
- ❌ Config keys in code defeats the purpose
- ❌ Can't add new tags without code change
- ❌ Verbose and confusing

### Alternative B: Separate Override Files (Considered)

Dedicated override files per layer:

```
config/
  settings.yaml          # Framework config
  bronze_overrides.yaml  # All bronze pipe/entity overrides
  silver_overrides.yaml  # All silver overrides
  gold_overrides.yaml    # All gold overrides
```

**Assessment:**
- ✅ Clean separation by layer
- ❌ Doesn't scale to cross-cutting concerns (all *.ingest_*)
- ❌ Duplicate config across layers
- **Verdict:** Not as flexible as wildcards

### Alternative C: Config-as-Code with DSL (Future Consideration)

Python DSL for config that can be tested:

```python
# config.py
from kindling.config import ConfigBuilder

config = ConfigBuilder()

with config.datapipes("bronze.*") as bronze:
    bronze.tags(layer="bronze", sla="4h")
    bronze.timeout(300)

with config.datapipes("bronze.ingest_orders") as pipe:
    pipe.tags(priority="critical")
    pipe.override_timeout(600)

# Exports to YAML or applies directly
```

**Assessment:**
- ✅ Testable, type-checked
- ✅ Can use loops, conditionals
- ✅ IDE autocomplete
- ⚠️ More complex to implement
- **Verdict:** Good future enhancement, not MVP

---

## Configuration Schema

### DataPipes Configuration

```yaml
{package_name}:
  datapipes:
    {pipe_id}:
      # Override decorator parameters
      tags:                       # Dict - deep merge
        key: "value"
      name: "Display Name"        # String - replace
      enabled: true               # Boolean - feature flag

      # Execution configuration
      timeout_seconds: 300        # Int
      retry_count: 3              # Int
      retry_backoff: 2.0          # Float (exponential backoff multiplier)

      # Override inputs/outputs (advanced)
      input_entity_ids:           # List - replace
        - "bronze.orders_v2"
      output_entity_id: "silver.orders_v2"  # String - replace
      output_type: "delta"        # String - replace

      # Spark configuration overrides
      spark_config:               # Dict
        spark.executor.memory: "16g"
        spark.executor.cores: "4"
```

### DataEntities Configuration

```yaml
{package_name}:
  dataentities:
    {entity_id}:
      # Override decorator parameters
      tags:                       # Dict - deep merge
        layer: "bronze"
        pii: "true"
      name: "Display Name"        # String - replace

      # Override entity properties
      partition_columns:          # List - replace
        - "year"
        - "month"
      merge_columns:              # List - replace
        - "order_id"

      # Storage configuration
      storage_format: "delta"     # String - replace
      storage_path: "/custom/path"  # String - replace (advanced)

      # Delta table properties
      delta_properties:           # Dict
        delta.appendOnly: "true"
        delta.checkpointInterval: "10"
```

### Merge Strategies

| Config Type | Merge Strategy | Example |
|-------------|----------------|---------|
| **tags** (Dict) | Deep merge - add new, override existing | `{base: "a", new: "b"}` + `{base: "c"}` → `{base: "c", new: "b"}` |
| **Scalars** (String, Int, Bool) | Replace | `"old"` → `"new"` |
| **Lists** (partition_columns) | Replace entire list | `["a", "b"]` → `["c"]` |
| **Nested Objects** | Deep merge recursively | Config objects merge like tags |

---

## Implementation Design (Revised)

### Key Insight: Two-Phase Registration

The fundamental problem with the original design is **timing**:
- Decorators execute at **import time** (before bootstrap)
- Config is loaded at **bootstrap time** (after imports)

**Solution: Two-Phase Registration**

```
Phase 1 (Import Time):
  - Decorators register "raw" metadata to a pending queue
  - No config resolution yet

Phase 2 (Bootstrap Time):
  - Load config files
  - Process pending registrations
  - Apply wildcard patterns
  - Apply specific overrides
  - Move to active registry
```

### Phase 1: Wildcard Pattern Matcher

```python
# packages/kindling/config_patterns.py

import fnmatch
import re
from typing import Dict, Any, List, Tuple

class ConfigPatternMatcher:
    """Matches config patterns against pipe/entity IDs with precedence."""

    # Pattern specificity scoring
    EXACT_MATCH = 1000
    SINGLE_WILDCARD = 100  # * matches one segment
    MULTI_WILDCARD = 10    # ** matches multiple segments

    def __init__(self, config_section: Dict[str, Any]):
        """
        Args:
            config_section: Dict like {pattern: overrides, ...}
                e.g., {"bronze.*": {...}, "bronze.ingest_orders": {...}}
        """
        self.patterns = self._compile_patterns(config_section)

    def _compile_patterns(
        self,
        config_section: Dict[str, Any]
    ) -> List[Tuple[str, re.Pattern, int, Dict[str, Any]]]:
        """Compile patterns and sort by specificity (most specific first)."""
        compiled = []

        for pattern, overrides in config_section.items():
            if not isinstance(overrides, dict):
                continue  # Skip non-dict values

            regex, score = self._pattern_to_regex(pattern)
            compiled.append((pattern, regex, score, overrides))

        # Sort by score descending (most specific first)
        compiled.sort(key=lambda x: x[2], reverse=True)
        return compiled

    def _pattern_to_regex(self, pattern: str) -> Tuple[re.Pattern, int]:
        """Convert glob pattern to regex with specificity score."""
        score = 0

        if '**' not in pattern and '*' not in pattern and '?' not in pattern:
            # Exact match
            score = self.EXACT_MATCH
            regex = re.compile(f"^{re.escape(pattern)}$")
        else:
            # Count wildcards for scoring
            parts = pattern.split('.')
            for part in parts:
                if part == '**':
                    score += self.MULTI_WILDCARD
                elif '*' in part or '?' in part:
                    score += self.SINGLE_WILDCARD
                else:
                    score += self.EXACT_MATCH  # Exact segment

            # Convert to regex
            regex_str = pattern
            regex_str = regex_str.replace('.', r'\.')  # Escape dots
            regex_str = regex_str.replace('**', '<<<MULTI>>>')  # Placeholder
            regex_str = regex_str.replace('*', r'[^.]+')  # Single segment
            regex_str = regex_str.replace('?', r'[^.]')  # Single char
            regex_str = regex_str.replace('<<<MULTI>>>', r'.+')  # Multi segment
            regex = re.compile(f"^{regex_str}$")

        return regex, score

    def get_matching_overrides(self, item_id: str) -> List[Dict[str, Any]]:
        """Get all matching overrides for an ID, ordered by specificity.

        Returns list of override dicts from least specific to most specific.
        Caller should apply them in order to get correct precedence.
        """
        matches = []

        for pattern, regex, score, overrides in reversed(self.patterns):
            if regex.match(item_id):
                matches.append(overrides)

        return matches  # Least specific first, most specific last

    def resolve_overrides(self, item_id: str, base: Dict[str, Any]) -> Dict[str, Any]:
        """Resolve all matching patterns into final merged config.

        Args:
            item_id: Pipe or entity ID (e.g., "bronze.ingest_orders")
            base: Base decorator params

        Returns:
            Merged config with all pattern matches applied
        """
        result = base.copy()

        for overrides in self.get_matching_overrides(item_id):
            result = self._merge_override(result, overrides)

        return result

    def _merge_override(
        self,
        base: Dict[str, Any],
        override: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Merge override into base with proper strategies."""
        result = base.copy()

        for key, value in override.items():
            if key == '_remove_tags':
                # Special: remove specific tags
                if 'tags' in result:
                    for tag_to_remove in value:
                        result['tags'].pop(tag_to_remove, None)
            elif key == '_remove_all_tags':
                # Special: clear all tags
                if value:
                    result['tags'] = {}
            elif key == 'tags':
                # Deep merge tags
                result_tags = result.get('tags', {}).copy()
                result_tags.update(value)
                result['tags'] = result_tags
            elif isinstance(value, dict) and isinstance(result.get(key), dict):
                # Deep merge nested dicts
                result[key] = {**result.get(key, {}), **value}
            else:
                # Replace scalars and lists
                result[key] = value

        return result
```

### Phase 2: Two-Phase Registry

```python
# packages/kindling/data_pipes.py (revised)

from typing import Dict, Any, List, Optional
from dataclasses import dataclass, fields

@dataclass
class PendingPipeRegistration:
    """Holds decorator params until config is available."""
    pipe_id: str
    decorator_params: Dict[str, Any]
    module_path: str  # For package detection

class DataPipesRegistry(ABC):
    """Abstract base for pipe registration."""

    @abstractmethod
    def register_pipe(self, pipeid: str, **params):
        pass

    @abstractmethod
    def get_pipe_ids(self) -> List[str]:
        pass

    @abstractmethod
    def get_pipe_definition(self, name: str) -> Optional[PipeMetadata]:
        pass


@GlobalInjector.singleton_autobind()
class DataPipesManager(DataPipesRegistry):
    """Two-phase pipe registry with config override support."""

    def __init__(self):
        self._pending: List[PendingPipeRegistration] = []
        self._registry: Dict[str, PipeMetadata] = {}
        self._finalized = False
        self._config_matcher: Optional['ConfigPatternMatcher'] = None

    def register_pipe(self, pipeid: str, **decorator_params):
        """Phase 1: Queue registration (called at import time)."""
        import sys

        # Get calling module for package detection
        frame = sys._getframe(2)  # Skip decorator wrapper
        module_path = frame.f_globals.get('__name__', '')

        self._pending.append(PendingPipeRegistration(
            pipe_id=pipeid,
            decorator_params=decorator_params,
            module_path=module_path
        ))

        # If already finalized (hot reload), process immediately
        if self._finalized:
            self._process_single_registration(self._pending[-1])

    def finalize_registrations(self, config_service: 'ConfigService'):
        """Phase 2: Apply config overrides (called at bootstrap time)."""
        from kindling.config_patterns import ConfigPatternMatcher

        # Build pattern matcher from config
        datapipes_config = config_service.get('datapipes', default={})
        self._config_matcher = ConfigPatternMatcher(datapipes_config)

        # Process all pending registrations
        for pending in self._pending:
            self._process_single_registration(pending)

        self._pending.clear()
        self._finalized = True

    def _process_single_registration(self, pending: PendingPipeRegistration):
        """Process one registration with config overrides."""
        # Start with decorator params
        final_params = pending.decorator_params.copy()

        # Apply config overrides if matcher available
        if self._config_matcher:
            final_params = self._config_matcher.resolve_overrides(
                pending.pipe_id,
                final_params
            )

        # Check enabled flag
        if final_params.get('_enabled') is False:
            print(f"⚠️  Pipe disabled by config: {pending.pipe_id}")
            final_params['_enabled'] = False

        # Clean up internal keys before creating metadata
        internal_keys = ['_remove_tags', '_remove_all_tags', '_enabled']
        clean_params = {k: v for k, v in final_params.items() if k not in internal_keys}

        # Store enabled flag separately
        enabled = final_params.get('_enabled', True)

        # Create and store metadata
        self._registry[pending.pipe_id] = PipeMetadata(
            pipeid=pending.pipe_id,
            **clean_params,
            _internal_enabled=enabled
        )

    def get_pipe_ids(self) -> List[str]:
        return list(self._registry.keys())

    def get_pipe_definition(self, name: str) -> Optional[PipeMetadata]:
        return self._registry.get(name)
```

### Phase 3: Bootstrap Integration

```python
# packages/kindling/bootstrap.py (additions)

def apply_config_overrides():
    """Called during bootstrap to finalize registrations with config."""
    from kindling.data_pipes import DataPipesManager
    from kindling.data_entities import DataEntityManager
    from kindling.spark_config import ConfigService
    from kindling.injection import get_kindling_service

    config = get_kindling_service(ConfigService)

    # Finalize pipe registrations
    pipes_manager = get_kindling_service(DataPipesManager)
    pipes_manager.finalize_registrations(config)
    print(f"✓ Finalized {len(pipes_manager.get_pipe_ids())} pipe registrations")

    # Finalize entity registrations
    entities_manager = get_kindling_service(DataEntityManager)
    entities_manager.finalize_registrations(config)
    print(f"✓ Finalized {len(entities_manager.get_entity_ids())} entity registrations")
```

### Phase 4: Decorator (Simplified)

```python
# packages/kindling/data_pipes.py

class DataPipes:
    """Static class for pipe decorator."""

    dpregistry = None

    @classmethod
    def pipe(cls, **decorator_params):
        """Decorator for registering data pipes.

        No more package_name param - config uses pipe IDs directly.
        Config overrides are applied at bootstrap time, not here.
        """
        def decorator(func):
            if cls.dpregistry is None:
                cls.dpregistry = GlobalInjector.get(DataPipesRegistry)

            decorator_params["execute"] = func

            # Validate required fields
            required_fields = {field.name for field in fields(PipeMetadata)}
            missing_fields = required_fields - decorator_params.keys()
            if missing_fields:
                raise ValueError(
                    f"Missing required fields in pipe decorator: {missing_fields}"
                )

            pipeid = decorator_params["pipeid"]
            del decorator_params["pipeid"]

            # Simple registration - no config lookup here
            # Config applied later in finalize_registrations()
            cls.dpregistry.register_pipe(pipeid, **decorator_params)

            return func

        return decorator
```

---

## Configuration Schema (Updated)

### Full Schema with Wildcard Support

```yaml
# settings.yaml - Complete schema reference

# ============================================================
# DataPipes Configuration
# ============================================================
datapipes:
  # Global defaults (lowest priority - applies to ALL pipes)
  "**":
    tags:
      framework: "kindling"
    timeout_seconds: 300
    retry_count: 3
    retry_backoff: 2.0

  # Layer-level defaults
  "bronze.*":
    tags:
      layer: "bronze"
    timeout_seconds: 300

  "silver.*":
    tags:
      layer: "silver"
    timeout_seconds: 600

  "gold.*":
    tags:
      layer: "gold"
    timeout_seconds: 900

  # Pattern-based overrides
  "*.ingest_*":           # Any ingestion pipe
    tags:
      type: "ingestion"
    retry_count: 5

  "*.transform_*":        # Any transform pipe
    tags:
      type: "transformation"

  # Exact match overrides (highest priority)
  bronze.ingest_orders:
    # Metadata overrides
    tags:
      domain: "sales"
      priority: "critical"
      owner: "team-a"
      pii: "false"
    name: "Orders Ingestion Pipeline"

    # Feature flags
    _enabled: true                    # Set false to disable

    # Tag management
    _remove_tags:                     # Remove specific tags
      - debug
      - test

    # Execution config
    timeout_seconds: 600
    retry_count: 5
    retry_backoff: 2.0

    # Advanced: Override decorator parameters
    # (Use with caution - can break logic)
    # input_entity_ids:
    #   - "bronze.orders_v2"
    # output_entity_id: "silver.orders_v2"

    # Spark config for this pipe
    spark_config:
      spark.executor.memory: "16g"
      spark.executor.cores: "4"
      spark.sql.shuffle.partitions: "200"

# ============================================================
# DataEntities Configuration
# ============================================================
dataentities:
  # Global defaults
  "**":
    tags:
      managed_by: "kindling"

  # Layer defaults
  "bronze.*":
    tags:
      layer: "bronze"
      retention: "90d"
    delta_properties:
      delta.logRetentionDuration: "interval 30 days"
      delta.deletedFileRetentionDuration: "interval 7 days"

  "silver.*":
    tags:
      layer: "silver"
      retention: "365d"
    delta_properties:
      delta.logRetentionDuration: "interval 90 days"

  "gold.*":
    tags:
      layer: "gold"
      retention: "forever"
    delta_properties:
      delta.logRetentionDuration: "interval 365 days"

  # Pattern: All customer entities have PII
  "*.customers":
    tags:
      pii: "true"
      gdpr: "applicable"

  # Pattern: All fact tables
  "gold.fact_*":
    tags:
      type: "fact"
    partition_columns:
      - year
      - month

  # Exact match
  bronze.orders:
    tags:
      domain: "sales"
      sla: "4h"
    name: "Raw Orders"

    # Override schema/structure
    partition_columns:
      - order_date
    merge_columns:
      - order_id

    # Delta table properties
    delta_properties:
      delta.appendOnly: "true"
      delta.autoOptimize.optimizeWrite: "true"
```

### Merge Strategies (Updated)

| Config Type | Merge Strategy | Wildcard Behavior |
|-------------|----------------|-------------------|
| **tags** (Dict) | Deep merge | Accumulate from all matches |
| **_remove_tags** (List) | Applied after merge | Remove from final tags |
| **Scalars** (String, Int, Bool) | Replace | Most specific wins |
| **Lists** (partition_columns) | Replace | Most specific wins |
| **delta_properties** (Dict) | Deep merge | Accumulate from all matches |
| **spark_config** (Dict) | Deep merge | Accumulate from all matches |

### Special Configuration Keys

| Key | Type | Purpose |
|-----|------|---------|
| `_enabled` | bool | Feature flag - set `false` to disable |
| `_remove_tags` | list | Tags to remove after all merges |
| `_remove_all_tags` | bool | Clear all tags (start fresh) |

---

## Migration Strategy (Updated)

### Phase 1: Core Pattern Matching (Week 1-2)

```
- [ ] Create config_patterns.py module
- [ ] Implement ConfigPatternMatcher with glob support
- [ ] Implement pattern scoring/precedence
- [ ] Write comprehensive pattern matching tests
- [ ] Test edge cases (overlapping patterns, escaping)
```

### Phase 2: Two-Phase Registration (Week 3-4)

```
- [ ] Refactor DataPipesManager to two-phase model
- [ ] Refactor DataEntityManager to two-phase model
- [ ] Add finalize_registrations() to bootstrap
- [ ] Ensure backward compatibility
- [ ] Write integration tests
```

### Phase 3: Tag Management & Features (Week 5)

```
- [ ] Implement _remove_tags handling
- [ ] Implement _enabled feature flag
- [ ] Add introspection API (what patterns matched?)
- [ ] Add config validation
- [ ] Write feature tests
```

### Phase 4: Documentation & Polish (Week 6)

```
- [ ] Create docs/configuration_patterns.md
- [ ] Add examples to docs/data_pipes.md
- [ ] Add examples to docs/data_entities.md
- [ ] Create migration guide for existing users
```

---

## Example: Complete Flow (Updated)

### 1. Code Definition (Unchanged)

```python
# my_sales/pipes.py
from kindling import DataPipes

@DataPipes.pipe(
    pipeid="bronze.ingest_orders",
    name="Ingest Orders",
    tags={"domain": "sales"},           # Base tags from code
    input_entity_ids=["source.orders"],
    output_entity_id="bronze.orders",
    output_type="append"
)
def ingest_orders(source_orders):
    return transform(source_orders)

@DataPipes.pipe(
    pipeid="bronze.ingest_customers",
    name="Ingest Customers",
    tags={"domain": "sales"},
    input_entity_ids=["source.customers"],
    output_entity_id="bronze.customers",
    output_type="append"
)
def ingest_customers(source_customers):
    return transform(source_customers)
```

### 2. Config File (settings.yaml)

```yaml
kindling:
  version: "0.2.0"

datapipes:
  # All pipes get framework tag
  "**":
    tags:
      framework: "kindling"
      managed: "true"

  # All bronze pipes get layer defaults
  "bronze.*":
    tags:
      layer: "bronze"
      sla: "4h"
    timeout_seconds: 300
    retry_count: 3

  # All ingest pipes get type tag
  "*.ingest_*":
    tags:
      type: "ingestion"
    retry_count: 5

  # Specific override for orders
  bronze.ingest_orders:
    tags:
      priority: "critical"
      owner: "team-a"
```

### 3. Environment Override (env_prod.yaml)

```yaml
datapipes:
  # In prod, remove debug tags from everything
  "**":
    _remove_tags:
      - debug
      - test

  # In prod, orders are mission-critical
  bronze.ingest_orders:
    tags:
      environment: "production"
      alert_channel: "#orders-alerts"
    timeout_seconds: 600
    retry_count: 10
```

### 4. Resolution Trace for `bronze.ingest_orders`

```
Step 1: Start with decorator params
  tags: {domain: "sales"}
  name: "Ingest Orders"
  timeout_seconds: (not set)
  retry_count: (not set)

Step 2: Apply "**" (score: 10)
  tags: {domain: "sales", framework: "kindling", managed: "true"}

Step 3: Apply "bronze.*" (score: 200)
  tags: {domain: "sales", framework: "kindling", managed: "true", layer: "bronze", sla: "4h"}
  timeout_seconds: 300
  retry_count: 3

Step 4: Apply "*.ingest_*" (score: 200)
  tags: {domain: "sales", framework: "kindling", managed: "true", layer: "bronze", sla: "4h", type: "ingestion"}
  retry_count: 5  # Override

Step 5: Apply "bronze.ingest_orders" from settings (score: 1000)
  tags: {domain: "sales", framework: "kindling", managed: "true", layer: "bronze", sla: "4h", type: "ingestion", priority: "critical", owner: "team-a"}

Step 6: Apply "**" from env_prod (score: 10, but env_prod > settings)
  _remove_tags: [debug, test]  # No effect (those tags don't exist)

Step 7: Apply "bronze.ingest_orders" from env_prod (score: 1000)
  tags: {..., environment: "production", alert_channel: "#orders-alerts"}
  timeout_seconds: 600  # Override
  retry_count: 10  # Override

FINAL RESULT:
{
    "pipeid": "bronze.ingest_orders",
    "name": "Ingest Orders",
    "tags": {
        "domain": "sales",
        "framework": "kindling",
        "managed": "true",
        "layer": "bronze",
        "sla": "4h",
        "type": "ingestion",
        "priority": "critical",
        "owner": "team-a",
        "environment": "production",
        "alert_channel": "#orders-alerts"
    },
    "timeout_seconds": 600,
    "retry_count": 10,
    "input_entity_ids": ["source.orders"],
    "output_entity_id": "bronze.orders",
    "output_type": "append"
}
```

---

## Advanced Features

### Feature Flags

```yaml
datapipes:
  bronze.experimental_pipeline:
    _enabled: false  # Disable without code changes
```

```python
# In DataPipesExecuter.run_datapipes()
for pipeid in pipes:
    pipe = self.dpr.get_pipe_definition(pipeid)
    if getattr(pipe, '_internal_enabled', True) is False:
        self.logger.info(f"Skipping disabled pipe: {pipeid}")
        self.emit("datapipes.pipe_skipped", pipe_id=pipeid, reason="disabled_by_config")
        continue
    # ... execute pipe
```

### Dynamic Input/Output Overrides (Advanced)

⚠️ **Use with caution** - changing inputs/outputs can break transform logic.

```yaml
datapipes:
  # Switch to v2 entities in production
  silver.transform_orders:
    input_entity_ids:
      - "bronze.orders_v2"      # Override from v1 to v2
    output_entity_id: "silver.orders_v2"
```

### Spark Config Per Pipe

```yaml
datapipes:
  gold.heavy_aggregation:
    spark_config:
      spark.executor.memory: "32g"
      spark.executor.cores: "8"
      spark.sql.shuffle.partitions: "400"
```

### Config Introspection API

```python
# See what config was applied to a pipe
from kindling.config_patterns import ConfigPatternMatcher

matcher = ConfigPatternMatcher(config.get('datapipes', {}))

# Get all patterns that matched
matches = matcher.get_matching_overrides("bronze.ingest_orders")
for override in matches:
    print(f"Pattern matched: {override}")

# Get final resolved config
final = matcher.resolve_overrides("bronze.ingest_orders", base_params)
print(f"Final config: {final}")
```

---

## Summary

### Recommended Approach

**✅ Global Namespace with Wildcard Patterns + Two-Phase Registration**

Key design decisions:
1. **Global namespace** - Pipe/entity IDs are globally unique (layer.name convention)
2. **Wildcard patterns** - `bronze.*`, `*.ingest_*`, `**` for DRY config
3. **Two-phase registration** - Decorators queue, bootstrap finalizes with config
4. **No package_name in decorators** - Simplifies code, uses ID for config lookup

### Key Benefits

1. **DRY Configuration** - Wildcards eliminate duplication
2. **Layer-Based Defaults** - `bronze.*`, `silver.*`, `gold.*` patterns
3. **Additive Tags** - Deep merge accumulates tags across patterns
4. **Feature Flags** - `_enabled: false` disables without code changes
5. **Tag Removal** - `_remove_tags` for environment-specific cleanup
6. **Backward Compatible** - All config is optional, decorators work unchanged
7. **Correct Timing** - Bootstrap applies config, not import time

### What Changed from Original Proposal

| Aspect | Original | Revised |
|--------|----------|---------|
| Package namespace | `package_name` param on decorator | Not needed - global namespace |
| Timing | Config at import time (broken) | Config at bootstrap time (correct) |
| Wildcards | Not supported | Full glob pattern support |
| Tag removal | Not possible | `_remove_tags` syntax |
| Registration | Direct to registry | Two-phase (pending → finalize) |

### Implementation Effort (Updated)

| Phase | Weeks | Effort |
|-------|-------|--------|
| Pattern Matching (`config_patterns.py`) | 2 | Medium |
| Two-Phase Registration | 2 | Medium |
| Tag Management & Features | 1 | Low |
| Documentation & Polish | 1 | Low |
| **Total** | **6 weeks** | **Medium** |

### Resolved Design Decisions

1. **Hot Reload: YES** - Config reload updates registry entries in place. This happens early in bootstrap, before any pipes execute. The registry's `apply_config_overrides()` can be called again after config reload - it re-applies patterns to all registered items.

2. **Validation: Upsert Model** - No "unknown keys" validation. Config is an upsert - any key is valid because it's adding/overriding properties on registrations. This allows config to add new metadata fields without framework changes.

3. **Pattern Precedence: Last-In Wins** - When patterns have equal specificity (e.g., `bronze.*` and `*.orders` both matching `bronze.orders`), the last pattern in config file order wins. This makes overriding predictable - put your overrides after the defaults.

4. **Config File Structure & YAML Namespaces:**

   **YAML Namespace Convention:**
   ```yaml
   # Top-level keys define the namespace

   kindling:                    # Framework config namespace
     version: "0.2.0"
     TELEMETRY:
       logging:
         level: INFO

   my_sales_package:            # Package config namespace
     datapipes:
       "bronze.*":
         tags:
           layer: "bronze"
     dataentities:
       "bronze.*":
         tags:
           layer: "bronze"

   my_inventory_package:        # Another package namespace
     datapipes:
       # ...
   ```

   **File Organization (Optional - for larger deployments):**
   ```
   config/
     settings.yaml              # Can contain kindling: + all package namespaces
     platform_fabric.yaml       # Platform overrides for any namespace
     env_prod.yaml              # Environment overrides for any namespace

     my_sales_package/          # OR split package config into subdirectory
       settings.yaml            # Contains my_sales_package: namespace
       env_prod.yaml            # Package-specific prod overrides
   ```

   **Rationale:**
   - `kindling:` namespace = framework config (always at top level)
   - `{package_name}:` namespace = package config
   - Can coexist in same file or be split into subdirectories
   - Subdirectories useful for large deployments with many packages
   - Same hierarchical precedence (settings → platform → env) applies to all

### Open Questions

_(None remaining - all major decisions resolved)_

### Next Steps

1. **Prototype pattern matcher** - Validate glob pattern approach
2. **Test two-phase registration** - Ensure backward compatibility
3. **Gather feedback** - Validate wildcard syntax preferences
4. **Implement MVP** - Pattern matching + basic overrides
