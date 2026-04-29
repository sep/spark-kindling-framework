# Plan: First-Class Local Bootstrap for Kindling

**Date:** 2026-04-08
**Status:** Proposal
**Scope:** Enable a supported standalone/local bootstrap path without regressing Fabric, Synapse, or Databricks flows

---

## 1. Problem Statement

Kindling already has two useful ingredients for local development:

- a public `kindling.initialize(...)` entrypoint
- a `standalone` platform implementation for local / OSS Spark environments

But the main bootstrap pipeline is still optimized for cloud notebook and remote-job scenarios. In practice, the current `initialize_framework()` path assumes some combination of:

- cloud platform detection
- config download from `artifacts_storage_path`
- optional wheel or extension installation during bootstrap
- workspace package loading as part of initialization

This creates a mismatch:

- **local development works** when users stay below full bootstrap
- **standalone bootstrapping is awkward** because the supported path is still effectively cloud-first

The goal of this plan is to make `standalone` a first-class, documented bootstrap-capable platform while keeping cloud behavior stable.

---

## 2. Goals

### Primary goals

1. Support a clean standalone bootstrap path using preinstalled Kindling and filesystem-backed project resources.
2. Make `standalone` a real platform outcome in the bootstrap lifecycle.
3. Keep one shared bootstrap pipeline across standalone and cloud platforms.
4. Separate framework initialization from environment mutation such as pip installs.
5. Keep existing Fabric / Synapse / Databricks behavior working during the transition.

### Non-goals

- rewriting the entire bootstrap stack in one pass
- replacing system-test or platform-native notebook workflows
- guaranteeing identical runtime behavior between standalone and all cloud runtimes
- solving all project scaffolding and DX issues in the same change

---

## 3. Current Friction

The main blockers are:

1. **No first-class standalone detection in main bootstrap**
   `detect_platform()` currently only returns `databricks`, `fabric`, or `synapse`, and otherwise raises.

2. **Config/resource acquisition is effectively cloud-specialized**
   `initialize_framework()` is structured around lake-backed config acquisition when `use_lake_packages` and `artifacts_storage_path` are set, but `standalone` is not treated as a peer provider of equivalent config/resource lookup behavior.

3. **Bootstrap performs pip installation**
   Framework initialization currently mixes config loading, dependency installation, extension installation, and platform startup.

4. **Workspace loading is part of core init**
   Loading workspace packages is useful in notebooks, but it should be optional for standalone bootstrap rather than an assumed stage.

5. **Entry-point semantics are overloaded**
   Today `initialize()` is conceptually "preinstalled Kindling init," but it still delegates into a cloud-oriented initialization pipeline.

6. **Import cost is high**
   `import kindling` eagerly imports much of the package, which makes bootstrap heavier and obscures phase boundaries.

---

## 4. Proposed Design Direction

The key design decision is:

> Keep one shared bootstrap pipeline, but make platforms responsible for resource/config acquisition through platform-specific adapters. Separately, split "make Kindling available" from "initialize Kindling".

That yields three phases:

1. **Availability**
   Ensure the framework package is installed and importable.

2. **Initialization**
   Resolve platform, acquire config/resources through the platform, register services, initialize providers.

3. **Workspace/App Loading**
   Optionally load workspace packages and optionally run an app.

This makes standalone bootstrap straightforward:

- Kindling is already installed
- config comes from filesystem-backed platform resolution or explicit config dict
- platform resolves to `standalone`
- no pip install occurs unless explicitly requested
- workspace loading is optional

This is preferable to inventing a second bootstrap model for local use. `standalone` should be a peer platform, not an escape hatch.

---

## 5. Recommended End State API

### 5.1 `kindling.initialize(...)`

Keep this as the main public entrypoint for preinstalled usage, including standalone bootstrap.

Recommended behavior:

- accepts explicit config dict
- accepts explicit config and optional config file hints
- does not assume remote storage-backed acquisition
- does not install packages by default

### 5.2 `kindling.ensure_available(...)`

Optional higher-level helper for notebook/bootstrap scripts that need to download or install Kindling when absent.

Recommended behavior:

- cloud/job oriented
- handles wheel lookup and install only
- does not initialize framework services itself

### 5.3 `kindling.load_workspace(...)`

Explicit optional stage for standalone or notebook package loading.

Recommended behavior:

- separate from core initialization
- can be called by notebooks or local runners
- easier to disable in local unit/component workflows

### 5.4 `kindling.run_app(...)`

Optional explicit stage for app execution after initialization and optional workspace loading.

This removes some hidden behavior from `initialize_framework()` and gives clearer orchestration.

---

## 6. Phased Implementation Plan

## Phase 0: Document and stabilize current intended usage

**Goal:** Reduce confusion immediately without risky code changes.

### Changes

- Document that local development should prefer:
  - direct imports
  - local Spark tests
  - `kindling.initialize(...)` only when framework-level wiring is required
- Document that full bootstrap is currently cloud-first in implementation, even though `standalone` exists conceptually as a peer platform
- Add a short "standalone bootstrap limitations" section to bootstrap docs

### Deliverables

- docs update
- consumer-facing examples of preinstalled/local initialization

### Risk

- Low

### Value

- High clarity, low engineering cost

---

## Phase 1: Make `standalone` a first-class bootstrap-capable platform

**Goal:** Make standalone bootstrap supported without restructuring everything else.

### Changes

1. **Teach `detect_platform()` about `standalone`**
   - Accept explicit `platform=standalone` / `platform_service=standalone`
   - If cloud detection fails, optionally fall back to `standalone` when configured to do so

2. **Define standalone resource/config acquisition**
   - Support filesystem-backed config/resource lookup through the standalone platform
   - Allow explicit `config_files=[...]` as an override or hint, but keep the primary model platform-backed
   - Do not require `artifacts_storage_path` for standalone configuration

3. **Make dependency installation opt-in for standalone bootstrap**
   - Add a bootstrap flag such as `install_bootstrap_dependencies=False` by default for standalone mode
   - Skip PyPI and extension installation unless explicitly enabled

4. **Make workspace loading opt-in by mode**
   - Default `load_local_packages=False` for standalone unless explicitly requested
   - Keep cloud behavior unchanged unless explicitly overridden

5. **Wire `standalone` platform service into initialization**
   - Ensure `initialize_platform_services("standalone", ...)` is reachable and supported through the main init path

6. **Introduce a platform-facing config/resource contract**
   - Define the minimal interface the bootstrap pipeline needs from a platform:
     - resolve config file locations
     - read config artifacts/resources
     - resolve workspace roots
   - Implement this for cloud platforms with current remote behavior
   - Implement this for standalone with filesystem behavior

### Suggested config shape

```python
kindling.initialize(
    {
        "platform": "standalone",
        "environment": "local",
        "config_files": [
            "config/settings.yaml",
            "config/env.local.yaml",
        ],
        "install_bootstrap_dependencies": False,
        "load_local_packages": False,
        "local_workspace_path": "./notebooks",
    }
)
```

### Deliverables

- working standalone initialization path
- tests covering explicit standalone init
- docs showing standalone bootstrap usage

### Risk

- Low to medium

### Value

- Very high: unlocks true standalone bootstrap with minimal blast radius

---

## Phase 2: Separate initialization stages internally

**Goal:** Reduce coupling inside bootstrap without changing user-facing workflows too aggressively.

### Changes

Refactor `initialize_framework()` into internal stages:

1. `resolve_bootstrap_inputs(...)`
2. `acquire_configuration_resources(...)`
3. `load_configuration(...)`
4. `install_optional_bootstrap_dependencies(...)`
5. `initialize_platform(...)`
6. `load_optional_workspace_packages(...)`
7. `run_optional_app(...)`

### Why this matters

- standalone mode can skip or alter stages cleanly
- cloud mode remains expressible
- unit and integration tests can target smaller pieces
- future CLI commands can orchestrate phases explicitly

### Deliverables

- internal function split
- tests per stage
- no major behavior change intended

### Risk

- Medium

### Value

- High maintainability improvement

---

## Phase 3: Create a clean availability/init split

**Goal:** Make runtime bootstrap scripts thinner and less coupled to framework initialization.

### Changes

1. Move wheel/package acquisition logic behind `ensure_available(...)`
2. Keep `initialize(...)` focused on runtime initialization only
3. Update runtime bootstrap notebook/scripts to:
   - ensure availability
   - call `initialize(...)`
   - optionally load workspace/app

### Deliverables

- slim runtime bootstrap entrypoint
- cleaner preinstalled path
- clearer separation between cluster bootstrapping and framework startup

### Risk

- Medium

### Value

- High architectural clarity

---

## Phase 4: Reduce import and runtime noise

**Goal:** Make bootstrap cheaper and easier to reason about.

### Changes

1. Reduce eager imports in `kindling.__init__`
2. Normalize pre-logger diagnostics
3. Remove debug-heavy prints from normal flows or gate them behind a verbose flag

### Deliverables

- smaller import side effects
- cleaner logs
- easier debugging of bootstrap phases

### Risk

- Medium

### Value

- Medium to high

---

## 7. Testing Plan

The implementation should be validated with explicit test tiers.

### Unit tests

Add tests for:

- explicit `standalone` platform detection
- fallback platform logic
- standalone resource/config resolution
- config path selection when `config_files` are provided explicitly
- skip/install behavior for bootstrap dependencies
- skip/load behavior for workspace packages

### Integration tests

Add tests for:

- `kindling.initialize({... platform: "standalone" ...})`
- standalone config layering
- standalone service initialization
- local workspace path scanning when enabled

### Regression tests

Keep existing tests for:

- Databricks init behavior
- Fabric init behavior
- Synapse init behavior
- extension resolution logic
- system tests that depend on current cloud flows

### One key new contract test

Add a narrow but important test:

> Given preinstalled Kindling, local config files, and `platform=standalone`, initialization succeeds without `artifacts_storage_path`, without storage utils, and without pip installation.

And ideally a second contract test:

> Given `platform=standalone` and a project root / workspace root, configuration can be resolved via standalone platform-backed resource lookup without any cloud storage utilities.

That is the contract this plan is trying to establish.

---

## 8. Rollout Strategy

### Step 1

Ship Phase 1 behind the existing `initialize()` API with minimal new knobs.

### Step 2

Refactor internals in Phase 2 while preserving behavior.

### Step 3

Adopt the availability/init split in runtime bootstrap scripts.

### Step 4

Update docs, starter templates, and any future CLI scaffolding to use the standalone bootstrap path by default for local workflows.

---

## 9. Recommended Order of Work

If this is implemented incrementally, the recommended order is:

1. explicit `standalone` support in platform detection
2. platform-facing config/resource contract
3. standalone implementation of that contract
4. explicit `config_files` override support in initialization
5. opt-out flags for dependency installation and workspace loading
6. standalone initialization integration test
7. internal stage refactor
8. runtime bootstrap split
9. eager import cleanup

This ordering gets user value early and lowers the risk of a large bootstrap rewrite.

---

## 10. Acceptance Criteria

This issue should be considered addressed when all of the following are true:

1. A developer can run preinstalled Kindling locally with `platform=standalone`.
2. Standalone initialization works without `artifacts_storage_path`.
3. Standalone initialization can resolve config/resources through filesystem-backed platform behavior.
4. Explicit local YAML file overrides can be passed when needed.
5. Standalone initialization does not pip-install packages unless explicitly requested.
6. Standalone initialization does not require notebook/workspace package loading unless explicitly requested.
7. Existing cloud bootstrap flows still work.
8. The supported standalone bootstrap path is documented and tested.

---

## 11. Bottom Line

The right approach is not to replace the cloud bootstrap path. It is to make `standalone` a first-class sibling platform in the same bootstrap pipeline.

That means:

- keep cloud/job bootstrap for remote environments
- add a clean standalone platform-backed path for local environments
- separate availability, initialization, and workspace loading
- make standalone bootstrap conservative by default

The smallest useful milestone is Phase 1. It unlocks real local bootstrap support without requiring a full architectural rewrite.
