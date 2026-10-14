# Surrogate Key Synthesis

**Status:** Proposed
**Created:** 2026-07-23
**Related:** [SCD2 support](obsolete/scd_type2_support.md) (explicitly deferred
surrogate keys — "a separate design decision"; this is it),
[migration desired-state design](migration_fit_for_purpose_findings.md)
(column-level declaration metadata), the tags-first convention.

## Summary

Kindling entities should be able to declare a surrogate key column and have
the framework synthesize it at persist time. Declaration on the entity
(tags-first), synthesis on the existing signal seam (an aspect, like
watermarking), deterministic-by-default (retry- and merge-safe), with
Delta IDENTITY as the capability-gated alternative for warehouse-style
integer keys.

## Motivating use cases

- **Dimensional modeling**: fact tables join dimensions on a stable,
  meaningless key rather than wide/composite/dirty business keys; SCD2
  dimensions need a *per-version* key so facts can pin the version that was
  current when the fact occurred.
- **Data Vault-style hubs/links**: deterministic hash keys over business
  keys are the backbone of the pattern.
- **Join-width reduction**: replace multi-column joins with one column.
- **Pseudonymization**: a stable non-reversible stand-in for a natural key
  at solution boundaries.

## Field evidence (2026-10)

A domain team porting its dimensions to Kindling had its shared transforms
generate a `uuid()` id column and merged SCD1 on business columns. The SCD1
strategy's `whenMatchedUpdateAll` rewrote every matched row's id on every
run, breaking the ids other tables referenced. That is exactly why the `hash`
strategy below rejects `uuid()`. The same column on an SCD2 entity is worse:
the default tracked columns include the id, so every run closes and
re-versions every row.

Mitigations available without this proposal:

- Compute `sha2(to_json(struct(<business keys>)), 256)` in the transform.
  This is the `hash` strategy's expression, so adopting the strategy later
  changes no key values.
- Set `scd.tracked` on SCD2 entities to keep the id out of change detection.
- Use an app-level `DeltaEntityProvider` subclass that honors a
  `merge.ignore_columns` tag, leaving listed columns out of the matched-row
  update (still written on insert). Batch only: the streaming `foreachBatch`
  path resolves strategies from the built-in registry and never sees a
  subclass.

## Prior art in the codebase

SCD2 merge routing already synthesizes
`sha2(to_json(struct(business_keys)), 256)` — ephemeral, computed per merge
(`_routing_key_column_expr` in `entity_provider_delta.py`), with a validated
`scd.routing_key: hash | concat` vocabulary. Surrogate keys are the
*persistent* generalization of the same computation.

## Declaration (tags-first)

```python
DataEntities.entity(
    entityid="silver.dim_customer",
    merge_columns=["customer_id", "source_system"],
    tags={
        "surrogate.column": "customer_sk",
        "surrogate.strategy": "hash",              # hash (default) | identity
        "surrogate.from": "customer_id,source_system",  # default: merge_columns
    },
    schema=...,   # customer_sk declared in the schema like any column
)
```

Sugar per the annotations-as-sugar convention:

```python
DataEntities.entity(..., surrogate_key="customer_sk")   # sets default tags
```

Validation at registration (same posture as `write.mode` / `scd.*`):
strategy vocabulary; `surrogate.from` columns must exist in the schema when
one is declared; the surrogate column must not appear in `surrogate.from`;
`identity` requires a Delta output and a declared schema.

## Strategies

### `hash` (default)

`sha2(to_json(struct(from_columns)), 256)` — the exact routing-key
expression, persisted. Properties that make it the default:

- **Deterministic**: retries, re-pulls, and backfills produce the same key —
  composes with at-least-once ingestion and merge dedup, unlike `uuid()`
  (which is explicitly rejected: nondeterminism breaks every idempotency
  property the framework leans on).
- **Coordination-free**: no max-key lookups, no single-writer constraint,
  streaming-safe (a pure column expression works in `foreachBatch` and
  direct sinks alike).
- **Uniqueness semantics** (be precise, people ask): distinct inputs
  colliding is birthday-bounded at ~k²/2²⁵⁷ — ~10⁻⁵⁴ at a trillion distinct
  keys, i.e. below storage bit-flip rates; no SHA-256 collision has ever
  been produced. Identical inputs *intentionally* produce identical keys
  (that is the retry/merge/conformity property). Therefore row-level
  surrogate uniqueness equals row-level uniqueness of `surrogate.from` —
  duplicate business keys yield duplicate surrogates, faithfully. Enforce
  key quality via validation (key-uniqueness checks), not key generation.
- `surrogate.hash.algo: xxhash64` offers a compact BIGINT key for
  join-width-sensitive cases with real collision math: 64-bit space is
  ~50% collision odds near 5 billion keys, ~10⁻⁴ at 100 million — opt-in
  with documented tradeoff, never the default.

### `identity` (capability-gated)

Delta `GENERATED ALWAYS AS IDENTITY` for warehouse-style compact integer
keys. Kindling does not compute anything — the declaration flows into DDL:

- `ensure_entity_table` / migration render the column as IDENTITY (this is
  column-level declaration metadata — the same StructField-metadata channel
  the migration desired-state design uses for `rename_from`).
- The write path must *exclude* the column (Delta populates it); merge
  inserts omit it.
- Gated on a runtime feature probe (`delta.identity_columns`), like
  `delta.cluster_by` — not all engines/versions support it, and identity
  tables are single-writer.
- Not retry-safe on the append path (retried appends mint new keys);
  the existing retry-warning machinery extends to warn on
  `identity` + retry + non-merge, mirroring the append warning today.

**Verified 2026-10-14** (local Spark 3.5.5, delta-spark 3.3.2):

- Kindling's SCD1 strategy works unchanged against an IDENTITY table
  (`withSchemaEvolution().whenMatchedUpdateAll().whenNotMatchedInsertAll()`).
  With the column absent from the source, inserts mint keys and matched rows
  keep theirs. "Write-path exclusion" therefore just means the DataFrame
  must not carry the column. The merge builders need no changes.
- A source that does carry the column fails with
  `DELTA_IDENTITY_COLUMNS_UPDATE_NOT_SUPPORTED`. The exclusion has to be
  enforced (drop the column, or fail fast with a Kindling-level message),
  not assumed.
- Plain appends also mint keys. Values are unique but not contiguous: we
  observed 1, 2, 3, then 5.
- Open-source Delta on Spark 3.5 does not parse the SQL column syntax
  `GENERATED ALWAYS AS IDENTITY` (`PARSE_SYNTAX_ERROR`). The builder
  `DeltaTable.create(...).addColumn(..., generatedAlwaysAs=IdentityGenerator())`
  works. DDL rendering should go through the builder, and the
  `delta.identity_columns` probe should exercise the path DDL actually uses,
  not just the SQL parser. Databricks parses the SQL syntax (DBR 10.4+).
  Fabric (Delta 3.2) is unverified; Synapse (Delta 2.4) predates support.
- Expected but unverified: on SCD2, new versions are inserted from the
  source's own columns, so a column absent from the source should mint a
  fresh identity per version. That would make identity a per-version key
  with no help from the merge.

**Non-goal:** `max(key) + row_number()` sequences. Unsafe under concurrent
writers, requires a serializing read-before-write, and `identity` covers the
need where the engine supports it.

## Synthesis seam

A **`SurrogateKeyAspect`**, registered at bootstrap like `WatermarkAspect`,
subscribing to `persist.before_persist`: when the output entity declares
`surrogate.column` with strategy `hash` and the DataFrame lacks the column,
return the DataFrame with it added. No provider changes; works for every
provider. The recent persist-signal fix (before_persist inside the failure
boundary) makes this seam contractually sound. The exception is the SCD2
version key, which cannot be computed before persist; it is synthesized
inside the SCD2 merge (see SCD2 interplay).

Streaming: the same column expression applies before the sink; the aspect
hooks the streaming pipe start path the same way the stream-merge work wired
`write.mode`.

`identity` needs no aspect — it is entirely a DDL/write-exclusion concern in
the Delta provider and migration.

## SCD2 interplay

- The dimension's surrogate key is a **version key**: `surrogate.from`
  defaults to `merge_columns + [effective_from]` when the entity is SCD2, so
  each version row gets its own key and facts can pin versions. An explicit
  `surrogate.from` of just the merge columns opts into a **durable key**,
  the same across all versions of a business key.
- **The version key cannot be synthesized at `persist.before_persist`**
  (gap found 2026-10-14). The SCD2 merge assigns `effective_from` itself:
  either `current_timestamp()` at merge time, or the `sequence_by` value.
  Without `sequence_by`, the value does not exist before persist. So the
  version key is computed in the SCD2 merge's insert values, beside
  `effective_from`, over the staged row. With `sequence_by`, the aspect
  could hash `merge_columns + [sequence_by]` to the same value, but one code
  path inside the merge covers both modes.
- **Change detection must exclude the surrogate column.** By default, SCD2
  tracks every column except the merge keys, the temporal columns and
  `sequence_by`. A version key differs on every version, so tracking it
  marks every incoming row changed, which is the `uuid()` failure mode.
  `surrogate.column` joins the default exclusions, and `scd.tracked`
  validation rejects it the way it rejects merge columns. A durable key is
  harmless if tracked, since it never differs for a given business key, but
  excluding every surrogate column keeps the rule simple.
- SCD1 needs no exclusion for `hash` keys: `whenMatchedUpdateAll` rewrites
  the column with an identical value.
- The `current_view` companion carries the current version's key
  automatically (it is just a column).
- The ephemeral routing key remains internal — same expression, different
  lifecycle; implementation should share `_routing_key_column_expr`.

## Unknown-member convention

Dimensional loads conventionally include an "unknown" row (key `-1` or the
hash of all-null business keys) so fact joins never drop rows.
`surrogate.unknown_member: "true"` seeds that row at `ensure_destination`
time. Phase 2; cheap once synthesis exists.

## Later: key resolution on fact loads

Synthesis is half the story; fact pipes need to *resolve* dimension keys
(join business keys → surrogate key, unknown-member fallback, late-arriving
dimension policy). That is a transform-library concern
(`resolve_surrogate(df, dimension_entity)`) layered on synthesis, and for
`hash` strategy it degenerates beautifully: resolution is *recomputation* —
no join needed. This asymmetry is the strongest argument for hash-first.

## Phases

1. `hash` strategy: tags + sugar + registration validation +
   `SurrogateKeyAspect` on the batch persist path; SCD2 version key
   synthesized inside the SCD2 merge; surrogate column excluded from
   default SCD2 tracked columns. Share the routing-key expression.
2. Streaming path wiring; unknown-member seeding; `xxhash64` option.
3. `identity`: DDL in ensure/migration via the `DeltaTable` builder (with
   the feature probe), enforced write-path exclusion (drop the column or
   fail fast), retry warnings.
4. Resolution helpers for fact loads.

## Open questions

- Column position/typing: hash keys are STRING(64) (or BIGINT for
  xxhash64) — declared explicitly in the schema, or injected into the
  declared schema by the framework when absent?
- Should `surrogate.from` normalization (trim/upper) be declarable? Data
  Vault practice says yes; adds a small vocabulary
  (`surrogate.normalize: trim_upper`).
- Cross-entity consistency: two entities hashing the same business keys get
  the same key by construction — document as a feature (conformed
  dimensions) rather than accident.
- `identity`: do Kindling's migration planning and schema-drift checks
  tolerate a column that is declared in the schema but absent from every
  write? Does Fabric's Delta 3.2 support identity columns?
