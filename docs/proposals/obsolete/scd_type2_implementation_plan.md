# TASK-20260429-001: SCD Type 2 — Implementation Plan

**Task:** TASK-20260429-001
**Status:** Implemented — all steps complete (PRs #77, #82, 2026-04-30)
**Design source:** `docs/proposals/obsolete/scd_type2_support.md` (2026-04-29 revision)

> **Archived.** This implementation plan has been fully executed. Refer to the git history of PRs #77 and #82 for the final implementation.

---

## 1. File-by-File Implementation Order

### Step 1 — `packages/kindling/data_entities.py`
**Why first:** Everything downstream depends on `SCDConfig` and `scd_config_from_tags`. The module is already imported by `entity_provider_delta.py` so no new imports are needed.

Add (in order):
- `ROUTING_KEY_METHODS: tuple[str, ...] = ("hash", "concat")` — module-level constant
- `SCDConfig` frozen dataclass (fields below)
- `scd_config_from_tags(entity) -> SCDConfig` — module-level function
- `_validate_scd_config(entity: EntityMetadata) -> None` — module-level private
- `DataEntityManager._register_scd2_current_companion(base, cfg)` — new instance method
- Modify `DataEntityManager.register_entity` to call validate then companion registration

### Step 2 — `packages/kindling/entity_provider_delta.py` (registry + stubs)
**Why second:** Registry and strategy ABCs must exist before `_merge_to_delta_table` can delegate.

Add at module level before `DeltaEntityProvider`:
- `DeltaMergeStrategy` ABC with `apply(delta_table, df, entity, merge_condition) -> None`
- `DeltaMergeStrategies` class-level registry (raises on duplicate, `get()` returns fresh instance)
- `SCD1MergeStrategy` decorated `@DeltaMergeStrategies.register(name="scd1")` — extract existing merge body
- `SCD2MergeStrategy` stub decorated `@DeltaMergeStrategies.register(name="scd2")`

Modify `DeltaEntityProvider._merge_to_delta_table`: replace body with strategy resolution and delegation.

### Step 3 — `packages/kindling/entity_provider_delta.py` (SCD2 logic + schema)
**Why third:** Requires Step 1 (SCDConfig) and Step 2 (strategy classes).

Add:
- `_execute_scd2_merge(delta_table, df, entity)` — module-level private (not a method; callable from strategy without circular dep)
- Full `SCD2MergeStrategy.apply` body calling `_execute_scd2_merge`
- `DeltaEntityProvider._augment_schema_for_scd2(schema, cfg) -> StructType`
- Schema augmentation calls in `_create_physical_table` and `_create_managed_table`, gated on `cfg.enabled`
- `DeltaEntityProvider.read_entity_as_of(entity, point_in_time) -> DataFrame`

### Step 4 — `packages/kindling/entity_provider_current_view.py` (new file)
**Why fourth:** Requires SCDConfig (Step 1) and DeltaEntityProvider (Step 3).

Create `CurrentViewEntityProvider(BaseEntityProvider)`:
- Read delegates to base entity's provider filtered to `is_current=true`
- All write methods raise `NotImplementedError` with clear message
- Inject `DataEntityManager` lazily (deferred import to avoid circular dep)

### Step 5 — `packages/kindling/entity_provider_registry.py`
**Why last:** Requires `CurrentViewEntityProvider` to exist (Step 4).

Add `self.register_provider("current_view", CurrentViewEntityProvider)` in `_register_builtin_providers` using same try/except import guard pattern as `csv` and `eventhub`.

---

## 2. New Classes and Functions — Exact Signatures

### `data_entities.py`

```python
ROUTING_KEY_METHODS: tuple[str, ...] = ("hash", "concat")

@dataclass(frozen=True)
class SCDConfig:
    enabled: bool
    tracked_columns: Optional[List[str]]       # None = track all non-key non-temporal cols
    effective_from_column: str                 # default "__effective_from"
    effective_to_column: str                   # default "__effective_to"
    is_current_column: str                     # default "__is_current"
    current_entity_id: str                     # default "{entityid}.current"
    routing_key_method: str                    # "hash" | "concat"

def scd_config_from_tags(entity) -> SCDConfig: ...
    # Returns SCDConfig(enabled=False, ...) if scd.type tag absent
    # Raises ValueError if scd.type present but != "2"
    # Raises ValueError if scd.routing_key not in ROUTING_KEY_METHODS

def _validate_scd_config(entity: EntityMetadata) -> None: ...
    # Calls scd_config_from_tags; when enabled:
    #   raises if merge_columns empty
    #   raises if tracked_columns overlaps merge_columns
    #   raises if temporal column names collide with business schema columns

# Inside DataEntityManager:
def _register_scd2_current_companion(self, base: EntityMetadata, cfg: SCDConfig) -> None: ...
    # Skips if cfg.current_entity_id already in registry (user-declared wins)
    # Builds companion EntityMetadata with tags:
    #   "scd.companion_of": base.entityid
    #   "scd.view_type": "current"
    #   "provider.read_only": "true"
    #   "provider_type": "current_view"
    # Strips all scd.* tags from base except the above
    # Emits "entity.scd2_companion_registered"
```

### `entity_provider_delta.py`

```python
class DeltaMergeStrategy(ABC):
    @abstractmethod
    def apply(
        self,
        delta_table,          # DeltaTable
        df: DataFrame,
        entity,               # EntityMetadata
        merge_condition: str,
    ) -> None: ...

class DeltaMergeStrategies:
    _registry: Dict[str, Type[DeltaMergeStrategy]] = {}

    @classmethod
    def register(cls, name: str): ...          # decorator; raises ValueError on duplicate
    @classmethod
    def get(cls, name: str) -> DeltaMergeStrategy: ...  # fresh instance; raises ValueError if unknown

@DeltaMergeStrategies.register(name="scd1")
class SCD1MergeStrategy(DeltaMergeStrategy):
    def apply(self, delta_table, df, entity, merge_condition): ...
    # Body: exact extract of current _merge_to_delta_table

@DeltaMergeStrategies.register(name="scd2")
class SCD2MergeStrategy(DeltaMergeStrategy):
    def apply(self, delta_table, df, entity, merge_condition): ...
    # Calls _execute_scd2_merge(delta_table, df, entity)
    # merge_condition accepted for interface uniformity but unused

def _execute_scd2_merge(delta_table, df: DataFrame, entity) -> None: ...
    # Module-level; see Section 3

# Inside DeltaEntityProvider:
def _merge_to_delta_table(self, df: DataFrame, entity, table_ref: DeltaTableReference) -> None:
    scd_config = scd_config_from_tags(entity)
    strategy_name = "scd2" if scd_config.enabled else "scd1"
    strategy = DeltaMergeStrategies.get(strategy_name)
    merge_condition = self._build_merge_condition("old", "new", entity.merge_columns)
    strategy.apply(table_ref.get_delta_table(), df, entity, merge_condition)

def _augment_schema_for_scd2(self, schema: StructType, cfg: SCDConfig) -> StructType: ...
    # Appends effective_from (TIMESTAMP NOT NULL), effective_to (TIMESTAMP NULL),
    # is_current (BOOLEAN NOT NULL) — only if not already present

def read_entity_as_of(self, entity, point_in_time) -> DataFrame: ...
    # SCD2: filter effective_from<=t AND (effective_to IS NULL OR effective_to>t)
    # Non-SCD2: Delta time travel via timestampAsOf option
```

### `entity_provider_current_view.py` (new file)

```python
@GlobalInjector.singleton_autobind()
class CurrentViewEntityProvider(BaseEntityProvider):
    @inject
    def __init__(self, logger_provider: PythonLoggerProvider): ...
    # NOTE: DataEntityManager and EntityProviderRegistry resolved lazily
    # at read time via GlobalInjector.get() — deferred to avoid circular import

    def read_entity(self, entity_metadata: EntityMetadata) -> DataFrame: ...
    def check_entity_exists(self, entity) -> bool: ...
    # All write methods raise NotImplementedError
```

---

## 3. SCD2 Merge Logic — `_execute_scd2_merge`

### Pre-conditions
- `entity.merge_columns` is non-empty (enforced at registration)
- `df` contains business columns only — no temporal columns
- Target table already has temporal columns (added by `_augment_schema_for_scd2` at creation)

### Step-by-step

**3.1 Resolve config**
```python
cfg = scd_config_from_tags(entity)
biz_keys = entity.merge_columns
scd2_meta_cols = {cfg.effective_from_column, cfg.effective_to_column, cfg.is_current_column}
tracked = cfg.tracked_columns or [
    c for c in df.columns if c not in biz_keys and c not in scd2_meta_cols
]
```

**3.2 Build change condition** (null-safe per-column OR chain)
```python
change_exprs = [
    f"(source.`{c}` != target.`{c}` OR (source.`{c}` IS NULL) != (target.`{c}` IS NULL))"
    for c in tracked
]
change_condition = " OR ".join(change_exprs)
```

**3.3 Routing key expression**

`hash` (default): `sha2(to_json(struct(*biz_key_cols)), 256)` on source side;
equivalent SQL expression using `named_struct` on target side.

`concat`: `concat_ws("||", *[col(k).cast("string") for k in biz_keys])`.

**3.4 Build staged DataFrame** (union of two groups)

*Group A — NULL key (new versions of changed rows):*
```python
changed_rows = (
    df.alias("source")
    .join(
        delta_table.toDF().filter(col(cfg.is_current_column) == True).alias("target"),
        on=biz_keys, how="inner"
    )
    .where(change_condition)
    .select("source.*")
    .withColumn("__merge_key", lit(None).cast("string"))
)
```
NULL key ensures these rows fall through to `whenNotMatchedInsert` (NULL never equals anything).

*Group B — Real key (close existing / insert first-time rows):*
```python
keyed_rows = df.withColumn("__merge_key", routing_key_col_expr)
```

```python
staged = changed_rows.unionByName(keyed_rows, allowMissingColumns=True)
```

**3.5 Insert values map**
```python
source_cols = [c for c in df.columns if c not in scd2_meta_cols]
insert_values = {f"`{c}`": f"staged.`{c}`" for c in source_cols}
insert_values[f"`{cfg.effective_from_column}`"] = "current_timestamp()"
insert_values[f"`{cfg.effective_to_column}`"]   = "NULL"
insert_values[f"`{cfg.is_current_column}`"]      = "true"
```

**3.6 Single-pass MERGE**
```python
merge_condition = (
    f"{target_routing_key_sql} = staged.__merge_key "
    f"AND target.`{cfg.is_current_column}` = true"
)
merge_builder = (
    delta_table.alias("target")
    .merge(source=staged.alias("staged"), condition=merge_condition)
    .whenMatchedUpdate(set={
        f"`{cfg.effective_to_column}`": "current_timestamp()",
        f"`{cfg.is_current_column}`":   "false",
    })
    .whenNotMatchedInsert(values=insert_values)
)
if cfg.tags.get("scd.close_on_missing") == "true":   # see step 3.7
    merge_builder = merge_builder.whenNotMatchedBySourceUpdate(
        condition=f"target.`{cfg.is_current_column}` = true",
        set={
            f"`{cfg.effective_to_column}`": "current_timestamp()",
            f"`{cfg.is_current_column}`":   "false",
        }
    )
merge_builder.execute()
```

**3.7 `scd.close_on_missing`** — `whenNotMatchedBySourceUpdate` closes any current target row absent from source. Requires Delta Lake 2.3+ (project minimum is PySpark 3.4+ / Delta 2.4+, so always available).

---

## 4. Test Plan

Run with `poe test-unit`. All tests use `pytest` + `pytest-mock`. Mock all Spark/Delta calls.

### `tests/unit/test_scd_config.py`
Tests for `SCDConfig`, `scd_config_from_tags`, `_validate_scd_config`.
No Spark needed — construct `EntityMetadata` directly.

Key tests:
```
test_scd_config_from_tags_no_tag_returns_disabled_config
test_scd_config_from_tags_scd_type_2_returns_enabled_config
test_scd_config_from_tags_invalid_scd_type_raises_value_error
test_scd_config_from_tags_custom_column_names_respected
test_scd_config_from_tags_tracked_columns_parsed_from_comma_list
test_scd_config_from_tags_empty_tracked_returns_none
test_scd_config_from_tags_routing_key_hash_is_default
test_scd_config_from_tags_routing_key_concat_accepted
test_scd_config_from_tags_invalid_routing_key_raises_value_error
test_scd_config_from_tags_default_current_entity_id_is_entityid_dot_current
test_validate_scd_config_scd2_missing_merge_columns_raises_value_error
test_validate_scd_config_tracked_cols_overlap_merge_cols_raises_value_error
test_validate_scd_config_temporal_col_collides_with_schema_raises_value_error
test_validate_scd_config_non_scd_entity_passes
```

### `tests/unit/test_scd_merge_strategies.py`
Tests for `DeltaMergeStrategies` registry and `SCD1MergeStrategy`.
Mocks: `MagicMock()` for `delta_table`, `df`, `entity`.

```
test_delta_merge_strategies_register_stores_class
test_delta_merge_strategies_register_duplicate_raises_value_error
test_delta_merge_strategies_get_returns_fresh_instance_each_call
test_delta_merge_strategies_get_unknown_name_raises_value_error
test_scd1_strategy_apply_calls_when_matched_update_all
test_scd1_strategy_apply_calls_when_not_matched_insert_all
test_scd1_strategy_apply_calls_execute
test_scd1_strategy_apply_uses_provided_merge_condition
```

### `tests/unit/test_scd2_merge.py`
Tests for `_execute_scd2_merge` (importable as a module-level function).
Mocks: full `DeltaTable` mock chain, `DataFrame` mock with `.columns`, `.alias()`, `.join()`, `.where()`, `.withColumn()`, `.unionByName()`.

```
test_execute_scd2_merge_calls_delta_table_merge
test_execute_scd2_merge_when_matched_update_closes_current_row
test_execute_scd2_merge_when_not_matched_insert_sets_is_current_true
test_execute_scd2_merge_when_not_matched_insert_sets_effective_from_to_current_timestamp
test_execute_scd2_merge_when_not_matched_insert_sets_effective_to_null
test_execute_scd2_merge_tracked_columns_default_excludes_keys_and_temporal
test_execute_scd2_merge_routing_key_hash_method_used_by_default
test_execute_scd2_merge_routing_key_concat_method_used_when_configured
test_execute_scd2_merge_close_on_missing_adds_when_not_matched_by_source_update
test_execute_scd2_merge_close_on_missing_false_omits_extra_arm
```

### `tests/unit/test_scd2_schema_augmentation.py`
Tests for `_augment_schema_for_scd2` and schema-path calls.

```
test_augment_schema_adds_all_three_temporal_columns
test_augment_schema_skips_columns_already_in_schema
test_augment_schema_effective_from_is_not_nullable
test_augment_schema_effective_to_is_nullable
test_augment_schema_is_current_is_not_nullable
test_augment_schema_uses_custom_column_names_from_config
test_create_physical_table_augments_schema_when_scd2_enabled
test_create_physical_table_no_augment_when_scd2_disabled
```

### `tests/unit/test_current_view_provider.py`
Tests for `CurrentViewEntityProvider`.

```
test_read_entity_filters_to_is_current_true
test_read_entity_uses_is_current_column_from_scd_config
test_read_entity_delegates_to_base_delta_provider
test_check_entity_exists_delegates_to_base_provider
test_merge_to_entity_raises_not_implemented_error
test_write_to_entity_raises_not_implemented_error
test_append_to_entity_raises_not_implemented_error
test_read_entity_missing_companion_of_tag_raises_value_error
```

### `tests/unit/test_scd2_registration.py`
Tests for `DataEntityManager` companion auto-registration.

```
test_register_entity_scd2_creates_companion_in_registry
test_register_entity_scd2_companion_id_is_entityid_dot_current
test_register_entity_scd2_companion_has_provider_type_current_view
test_register_entity_scd2_companion_has_scd_companion_of_tag
test_register_entity_scd2_companion_inherits_merge_columns
test_register_entity_scd2_custom_current_entity_id_used
test_register_entity_scd2_user_declared_companion_not_overwritten
test_register_entity_scd2_emits_companion_registered_signal
test_register_entity_non_scd2_does_not_create_companion
test_register_entity_scd2_invalid_config_raises_before_registration
```

---

## 5. Sequencing Constraints

| Step | Requires |
|------|----------|
| Step 1 (`SCDConfig`, `scd_config_from_tags`) | Nothing |
| Step 1 (`DataEntityManager` companion registration) | `SCDConfig`, `scd_config_from_tags` |
| Step 2 (registry + SCD1 extraction) | Nothing (SCD1 is an extraction) |
| Step 2 (`_merge_to_delta_table` refactor) | Step 1 + Step 2 registry |
| Step 3 (`_execute_scd2_merge`) | Step 1 |
| Step 3 (schema augmentation) | Step 1 |
| Step 4 (`CurrentViewEntityProvider`) | Step 1, Step 3 |
| Step 5 (registry wiring) | Step 4 |
| Tests 6a (`test_scd_config.py`) | Step 1 |
| Tests 6b (`test_scd_merge_strategies.py`) | Step 2 |
| Tests 6c (`test_scd2_merge.py`) | Step 3 |
| Tests 6d (`test_scd2_schema_augmentation.py`) | Step 3 |
| Tests 6e (`test_current_view_provider.py`) | Step 4 |
| Tests 6f (`test_scd2_registration.py`) | Step 1 |

### Circular import hazard
`CurrentViewEntityProvider` needs `DataEntityManager` (in `data_entities.py`). Resolve via `GlobalInjector.get()` inside the method body at call time — not at `__init__` or module scope. This is already the pattern used by other builtin providers.

### Registry is class-level
`DeltaMergeStrategies._registry` populates at module import. Built-in strategies self-register when their class bodies execute. `entity_provider_delta.py` is imported at startup via `EntityProviderRegistry._register_builtin_providers` so ordering is never an issue in production or in tests that import the module.
