# Databricks Workspace Role Model

## Problem

The current Databricks workspace Terraform module mixes two different use cases:

1. A workspace used to build, validate, and release Kindling itself
2. A workspace used to run customer or business solutions that depend on Kindling

Those are not the same operational profile.

A platform workspace benefits from:

- dedicated Kindling infrastructure namespaces
- system-test scaffolding
- runtime temp/log/package volumes
- CI-oriented grants and outputs

A solution workspace should default to:

- minimal infrastructure
- minimal privileges
- alignment with the solution's existing catalog/schema layout
- no test-only resources unless explicitly enabled

If the module keeps one implicit shape, one of these two roles will always be over-provisioned or under-provisioned.

## Goal

Refactor the Terraform model so one module can support both roles with explicit intent.

The desired model is:

- one Databricks workspace module
- one explicit workspace role
- common baseline resources
- role-specific defaults
- opt-in platform/test scaffolding

## Proposed Top-Level Variable

Add:

```hcl
variable "workspace_role" {
  description = "Workspace operating model: platform or solution"
  type        = string
  default     = "solution"

  validation {
    condition     = contains(["platform", "solution"], var.workspace_role)
    error_message = "workspace_role must be one of: platform, solution."
  }
}
```

This variable should drive defaults, not block explicit overrides.

That means:

- `workspace_role = "solution"` gives the leanest safe defaults
- `workspace_role = "platform"` enables Kindling-centric defaults
- users can still override specific knobs where needed

## Role Definitions

### `platform`

Purpose:

- build and validate Kindling
- run system tests
- host artifacts, package wheels, temp/checkpoints, logs, and test apps

Expected characteristics:

- dedicated Kindling runtime namespace is appropriate
- CI/service principal needs runtime-operational permissions
- system-test resources are normal
- outputs should be CI-friendly

Default posture:

- opinionated
- self-hosting
- test-capable

### `solution`

Purpose:

- run a customer or business solution that happens to use Kindling

Expected characteristics:

- Kindling is a runtime dependency, not the primary product
- existing catalog/schema/storage layout may already exist
- test-only resources should not appear by default
- privileges should be tightly scoped

Default posture:

- minimal
- non-invasive
- platform/runtime only

## Resource Categories

The module should be thought of in four layers.

### 1. Common baseline

These are reasonable in either role:

- provider/auth configuration
- optional storage credential
- optional external location
- optional medallion catalog and schemas
- service principal/user presence
- workspace entitlements
- workspace config

### 2. Kindling artifacts

These support bootstrap and artifact distribution:

- Kindling catalog
- Kindling schema
- Kindling artifacts external volume
- artifacts mount for non-UC mode
- external location grants needed for artifact access

These may be needed in both roles, but should remain explicitly configurable.

### 3. Kindling runtime support

These support active Kindling execution:

- managed runtime volumes like:
  - `temp`
  - `logs`
  - `packages`
  - optionally `data_apps`
- runtime volume grants
- runtime outputs for CI/runtime env wiring

These should default on for platform workspaces and off for solution workspaces.

### 4. Platform-only scaffolding

These are specifically about building/testing Kindling:

- system-test compute defaults
- system-test schemas/paths
- extra outputs for CI
- EventHub/system-test integration assumptions
- broad test-only grants, if any

These should be opt-in and should not appear in a solution workspace by accident.

## Proposed Variables

Keep the current variables, but group new behavior behind a few clear toggles.

### Workspace role

```hcl
workspace_role = "solution" | "platform"
```

### Kindling artifacts

```hcl
variable "enable_kindling_artifacts" {
  description = "Whether to provision Kindling artifacts namespace and artifacts volume/mount"
  type        = bool
  default     = null
}
```

Default behavior:

- platform: `true`
- solution: `true` if bootstrap-from-artifacts is desired, otherwise `false`

### Kindling runtime support

```hcl
variable "enable_kindling_runtime_volumes" {
  description = "Whether to provision Kindling managed runtime volumes"
  type        = bool
  default     = null
}

variable "kindling_runtime_volume_names" {
  description = "Managed runtime volume names to create when enable_kindling_runtime_volumes=true"
  type        = list(string)
  default     = ["temp", "logs", "packages"]
}
```

Default behavior:

- platform: `true`
- solution: `false`

### Platform support

```hcl
variable "enable_kindling_platform_support" {
  description = "Whether to enable Kindling platform/test scaffolding in this workspace"
  type        = bool
  default     = null
}
```

Default behavior:

- platform: `true`
- solution: `false`

This can be used later to gate:

- system-test-specific outputs
- test compute
- test namespaces

## Default Matrix

### `workspace_role = "platform"`

Recommended defaults:

- `enable_kindling_artifacts = true`
- `enable_kindling_runtime_volumes = true`
- `kindling_runtime_volume_names = ["temp", "logs", "packages"]`
- `enable_kindling_platform_support = true`
- `kindling_runtime_volume_catalog_name = "kindling"`
- `kindling_runtime_volume_schema_name = "kindling"`

Typical runtime namespace:

- `kindling.kindling.artifacts`
- `kindling.kindling.temp`
- `kindling.kindling.logs`
- `kindling.kindling.packages`

### `workspace_role = "solution"`

Recommended defaults:

- `enable_kindling_artifacts = true` or `false` depending on deployment model
- `enable_kindling_runtime_volumes = false`
- `enable_kindling_platform_support = false`
- no default forced runtime namespace

Typical runtime namespace:

- solution-defined
- could be `medallion.default.*`
- could be existing customer namespace
- could be no managed volumes at all

## Mapping Existing Resources

This is the concrete resource movement plan.

### Keep as common baseline

- `databricks_storage_credential.datalake`
- `databricks_external_location.artifacts`
- `databricks_catalog.main`
- `databricks_schema.schemas`
- `databricks_service_principal.sp`
- `databricks_user.users`
- `databricks_entitlements.*`
- `workspace_conf`

### Gate behind `enable_kindling_artifacts`

- `databricks_catalog.kindling`
- `databricks_schema.kindling`
- `databricks_volume.kindling_artifacts`
- non-UC artifacts mount resources
- artifacts-specific grants and outputs

### Gate behind `enable_kindling_runtime_volumes`

- `databricks_volume.managed`
- runtime-volume-specific grants
- runtime volume outputs

### Gate behind `enable_kindling_platform_support`

Initially:

- CI/system-test-oriented outputs
- engineering-specific docs/examples

Later, if added:

- engineering clusters
- system-test helper resources

## Runtime/CI Integration

The Terraform outputs should become the canonical source of runtime namespace wiring for platform workspaces.

Important outputs:

- `kindling_runtime_volume_namespace`
- `runtime_volume_paths`
- `artifacts_storage_path`

These should feed dev/test env such as:

- `KINDLING_DATABRICKS_RUNTIME_VOLUME_CATALOG`
- `KINDLING_DATABRICKS_RUNTIME_VOLUME_SCHEMA`
- `KINDLING_DATABRICKS_RUNTIME_TEMP_VOLUME`

This keeps runtime/test configuration aligned with Terraform instead of hardcoding paths like:

- `/Volumes/medallion/default/temp`

## Permission Model Implications

### Engineering workspace

Expected grants may include:

- `READ_VOLUME` / `WRITE_VOLUME` on Kindling runtime volumes
- artifact access grants
- broader service-principal runtime permissions

### Solution workspace

Default should be narrower:

- only the grants necessary for the solution’s actual runtime model
- no automatic creation of test-oriented volume grants
- no implicit expectation that `temp`, `logs`, or `packages` volumes exist

## Migration Approach

### Phase 1

Add the role and feature toggles, but preserve current behavior by default.

Suggested initial compatibility default:

- `workspace_role = "platform"` for current internal workspaces

### Phase 2

Move existing Kindling-specific resources behind:

- `enable_kindling_artifacts`
- `enable_kindling_runtime_volumes`

without changing actual SEP behavior.

### Phase 3

Create a lean solution workspace example:

- no runtime volumes by default
- no engineering scaffolding
- explicit opt-in for artifacts/runtime support

## Recommendation

Short term:

1. Introduce `workspace_role`
2. Add:
   - `enable_kindling_artifacts`
   - `enable_kindling_runtime_volumes`
   - `enable_kindling_platform_support`
3. Treat current SEP dev as:
   - `workspace_role = "platform"`
4. Keep `rrc.dev` closer to:
   - `workspace_role = "solution"`

This gives us a path to:

- keep the internal platform workspace fully Kindling-native
- avoid baking engineering assumptions into customer solution workspaces

