# Migration: From Schema Reconciliation to Fit-for-Purpose Data Evolution

**Status:** Proposed
**Created:** 2026-07-23 (as review findings; revised into a proposal the same day)
**Related:** [Great Expectations validation](great_expectations_validation.md)
(shares the validator and signals direction),
[declaration conventions](../contributing/declaration_conventions.md)
(tags-first surface any new declarations must follow)

## Summary

The migration feature (MigrationService, `kindling migrate` CLI) is fit for a
deliberately narrow purpose: developer-led reconciliation of Delta table
schemas and permanent SQL views — table creation, simple additive schema
evolution, and carefully reviewed destructive rewrites. It should be described
as **Delta schema reconciliation** until the gaps below are closed.

This proposal has two parts:

1. **Guardrail fixes** (landed 2026-07-23, same change as this revision):
   correctness hazards in the existing implementation that did not need new
   design — see "Resolved" below.
2. **Completing the desired-state model** above the existing planner and
   Delta execution primitives: declaration enrichment so convergence is
   unambiguous (rename lineage, defaults, backfills), lifecycle hooks,
   durable run state, and real post-rewrite validation. The declaration
   describes the world as it should be and kindling converges to it — no
   versioned migration scripts. Specified here at the level of goals and
   shape, not committed API.

## What is implemented today

For registered entities, the planner detects:

- Delta table creation;
- column additions, removals, and data-type changes;
- partition-column and liquid-clustering changes; and
- SQL view creation or replacement when the registered SQL hash changes.

Column additions, table creation, view creation/replacement, and cluster
changes are classified as safe. Column removal, type change, and partition
change require an explicit destructive opt-in. Catalog-mode destructive
changes use a blue/green table rewrite; storage-mode changes overwrite the
Delta table in place. An optional pre-rewrite Delta CLONE snapshot is
available.

Sources: [planner](../../packages/kindling/migration.py),
[applier](../../packages/kindling/migration.py), and
[CLI](../../packages/kindling_cli/kindling_cli/cli.py) (`migrate` group).

## Coverage assessment

| Migration concern | Current behaviour | Assessment |
| --- | --- | --- |
| Create a table or a view | Supported | Good baseline capability. |
| Add a nullable column | Supported through ALTER TABLE ADD COLUMNS | Suitable for simple additive evolution. |
| Remove a column | Destructive full-table rewrite | Supported, with explicit approval. |
| Widen a primitive type | Full-table rewrite; selected primitive pairs are auto-cast | Reasonable initial support, but still operationally heavy. |
| Other type conversions | A caller may supply a Spark Column expression keyed by column name | Narrow escape hatch only. |
| Change partitions | Destructive rewrite | Supported in principle. |
| Change clustering | Best-effort ALTER TABLE CLUSTER BY | The migration can report success when this operation fails. |
| Update a permanent SQL view | CREATE OR REPLACE VIEW using a stored SQL hash | Useful, but has no dependency or grant model. |
| Rename a column | Seen as add plus remove | Not safely supported (F-01). |
| Default an added field | Not supported | Missing (F-02). |
| Backfill existing rows | Not supported as a migration operation | Missing (F-02). |
| Change nullability, comments, metadata, constraints, generated columns, or field order | Not compared or applied | Missing (F-04). |
| Evolve nested fields or decimals precisely | Treated as a whole-type change | Too coarse for a general schema-evolution feature. |

## Resolved (guardrail fixes, 2026-07-23)

These findings from the original review were fixed directly because they were
correctness hazards in existing behaviour, not new design:

- **Non-Delta entities are no longer planned as Delta tables** (was F-07,
  originally rated Medium; verification showed it deserved High). The planner
  sent every registered non-SQL entity to the injected DeltaEntityProvider
  with no `provider_type` check, and the name-mapper fallback resolves a
  table name for *any* entity — so a memory/parquet/eventhub entity was
  planned as a **safe** `CREATE_TABLE`, and a plain `migrate apply` (no
  `--destructive`) created a spurious managed Delta table for it. The planner
  now skips entities whose `provider_type` (default `delta`) is not `delta`,
  reports them as `[SKIP]` with the reason, and `is_up_to_date` excludes them.
- **Apply fails fast on plan errors** (was F-06). The applier previously
  logged a warning, skipped errored entities, migrated the rest, and the CLI
  printed "Migration complete." — a partial infrastructure mutation reported
  as success. `MigrationApplier.apply` now raises before applying anything
  when the plan contains inspection errors, and both `migrate plan` and
  `migrate apply` check errors *before* the up-to-date short-circuit
  (an errors-only plan has no changes and previously printed "All entities
  are up to date.").
- **The transform unit tests are no longer order-dependent** (part of F-09).
  The two failing tests built real `F.col()` expressions, which assert an
  active SparkContext — they failed in isolation and passed in full-suite
  runs only because an earlier test left a session behind (which is why CI
  stayed green). They now stub the column expression.
- **Dead `_normalize_sql` removed.** The view-SQL hash is deliberately over
  the raw declared SQL: normalizing (lowercase/collapse whitespace) would
  risk missing real changes inside string literals, and the cost of raw
  hashing is only a benign idempotent view replace when declaration
  formatting changes. The hash docstring now says so.

## Open findings (motivation for the proposal)

### F-01 — A column rename can lose live values

**Severity:** High

The planner represents a rename as ADD_COLUMNS(new_name) plus
REMOVE_COLUMNS(old_name). Safe changes are applied before the destructive
rewrite, so the new column is created empty and the rewrite then drops the
old column; the transform mechanism only runs for type changes and cannot
copy old values across. The *live* table silently loses the values. (There is
a recovery window — catalog mode keeps the blue archive until cleanup, and
storage mode has Delta time travel until VACUUM — but the migration itself
produced a wrong live table.)

### F-02 — Added fields cannot declare or apply defaults and backfills

**Severity:** High

`ADD COLUMNS` renders only the column name and type. There is no way to
declare a database default, populate historical rows, or express
`rename_from` in the entity model. The most common non-breaking evolution is
therefore incomplete: a column can be added, but existing data cannot be made
valid under a new non-null or business-required contract.

### F-03 — The only override path is too narrow and not available from the CLI

**Severity:** High

`MigrationApplier.apply()` accepts `user_transforms` — column name to
already-created Spark Column expression — used only for type changes. There
is no generic DataFrame transform, no per-entity or per-change callback, no
pre/post migration hook, no migration signal, and no CLI option for supplying
an override. The hook cannot implement a controlled backfill, a rename, an
entity split or merge, a lookup-based conversion, custom validation, or a
post-cutover action.

### F-04 — Schema drift detection omits important contract properties

**Severity:** Medium

Columns are compared by name and dataType only: nullability, field metadata,
comments, defaults, constraints, generated/identity columns, table
properties, and ordering are not modelled; nested changes surface as one
top-level type change. Adequate for physical reconciliation, not for a
logical data contract.

### F-05 — Destructive migrations have insufficient validation and execution state

**Severity:** High

Catalog-mode blue/green validates only that row counts match; storage-mode
overwrite performs no validation at all. Neither records a migration run,
plan hash, timestamps, validation result, executor, or recovery state. Fixed
`_migration_blue` / `_migration_green` names have no run identifier or lock,
leaving concurrent operations and failed retries ambiguous. **The blue/green
swap itself is two separate RENAME statements and is not atomic: a crash
between them leaves no live table at the original name.** Rollback exists
only for catalog mode; storage mode relies on an optional snapshot or manual
Delta recovery.

### F-08 — Several declared change kinds have no planner path

**Severity:** Low

TAG_UPDATE and DROP_VIEW exist in the change-kind enumeration (and TAG_UPDATE
has an applier branch), but the planner never produces either, making the
public surface look more complete than the coverage is.

### F-09 (remainder) — System coverage does not establish safety for risky paths

**Severity:** Medium

The cross-platform migration test app exercises table creation,
permanent-view creation, and one additive-column case. Destructive casts,
custom transforms, removal, partition rewrite, snapshot, rollback, cleanup,
retry, and validation failure are not exercised on any platform.

## Fit-for-purpose decision

The feature is appropriate today when all of the following are true:

- the target is a Delta table or permanent SQL view;
- changes are table creation or simple nullable additions, or a carefully
  reviewed destructive rewrite;
- a team can write and validate any required data conversion outside the
  migration framework;
- migration execution is serialized operationally; and
- storage-mode rollback is handled through an explicit backup/recovery plan.

It is not appropriate as the sole mechanism for business-critical contract
evolution, especially where a change requires preserved values, defaults,
historical backfill, semantic validation, auditability, or reliable retry and
rollback.

## Proposal: desired-state convergence, completed

**Design commitment:** migration stays a desired-state system — the entity
declaration describes the world as it should be, and kindling figures out how
to make it happen. There are no versioned migration scripts and no ledger of
which steps ran where. This is the model the feature already implements at
the physical level; the findings above are cases where the declared state is
*ambiguous* (a rename diffs as drop+add; a backfill value is not derivable
from the target schema), and the fix is to enrich the declaration until
convergence is unambiguous — never to switch to transition scripts.

Two properties fall out of holding this line:

- **Environment promotion needs no machinery.** Each environment converges
  from wherever it currently is; a dev table three renames behind and a prod
  table one rename behind both converge to the same declaration.
- **Ambiguity is reported, not guessed.** Where the differ cannot infer a
  safe transition, the plan says so and names the annotation that would
  resolve it — the Terraform `moved`-block posture.

Retain the existing planner and Delta execution primitives; build the
following above them before expanding the product claim. Ordered so each
piece is independently useful.

### 1. Declaration enrichment for unambiguous convergence (addresses F-01, F-02, F-04)

Declared columns gain desired-state attributes: `rename_from` (lineage —
"this column was previously known as X", naturally carried in StructField
metadata or `evolution.*` tags per the tags-first convention), `default`
(part of the column's contract), `backfill` (what pre-existing rows should
hold — also part of the new column's contract), and nullability/constraint
intent. These are annotations on the *desired state*, not steps:
`rename_from` fires only when a column by the old name exists and the new
one does not, so it is idempotent and self-neutralizing — it can remain in
the declaration indefinitely without effect once every environment has
converged. The planner turns them into first-class change kinds
(RENAME_COLUMN with value preservation, ADD_COLUMN with default and
backfill). A rename detected *without* lineage is reported as ambiguous
(drop+add with a pointer to `rename_from`), not silently planned.

### 2. Lifecycle hooks on the existing signal seam (addresses F-03)

Migration emits lifecycle signals (`migration.before_apply`,
`migration.before_entity`, `migration.transform`, `migration.validate`,
`migration.after_entity`, `migration.failed`) through the existing
`DataSignals` mechanism rather than a parallel callback system — the same
seam the validation proposal builds on, with the same synchronous-gate
semantics the batch persist path now guarantees. Declarative rules cover
simple expressions; registered handlers cover joins and complex
transformations. The CLI grows a way to point at a migration module that
registers handlers.

### 3. Durable migration-run record and locking (addresses F-05)

A migration run writes a record (plan hash, per-entity status, timestamps,
validation results, executor identity, artifact names) to a kindling-managed
entity, and blue/green artifacts carry a run identifier instead of fixed
names. A lock (or lease) makes concurrent applies fail explicitly. Retry
becomes idempotent: a re-run of the same plan hash resumes or no-ops per
entity. The non-atomic swap window is recorded so recovery tooling can detect
and complete or roll back an interrupted swap.

### 4. Required post-rewrite verification (addresses F-05)

Row count stays as a default check but stops being the only one: key
uniqueness over `merge_columns`, null checks against declared intent, and
registered custom validators (the validation-runner work in the GX proposal
is the natural home). Storage mode gets the same verification as catalog mode
— verify against a staged write before promoting, rather than after an
irreversible in-place overwrite.

### 5. Explicit outcomes for best-effort operations (addresses F-08 and the CLUSTER BY gap)

An operation that cannot be applied (CLUSTER BY unsupported on the runtime,
TAG_UPDATE never planned) must surface as a distinct per-change outcome
(`applied`, `skipped`, `failed`) in the run record and CLI output, not as a
warning inside a run that reports overall success. Unplannable change kinds
are removed from the enum until they have a planner path.

### 6. Integration coverage for the risky paths (addresses F-09)

The migration test app grows scenarios for: destructive cast with auto-cast
and with a registered transform, rename with preservation, default+backfill,
removal, partition rewrite, snapshot, rollback, cleanup, interrupted-swap
recovery, and validation failure — on both catalog and storage modes.

Suggested sequencing: (1) and (5) first — they change what the planner
*claims*, which is the trust surface; then (3)+(4) together, since run
records without verification (or verification without records) each deliver
half the safety story; (2) alongside (3); (6) grows with each phase.

## Open questions

- ~~Where does evolution metadata live~~ Resolved by the desired-state
  commitment: in the entity declaration itself (StructField metadata for
  column-level attributes, `evolution.*` tags for entity-level intent).
  Versioned migration files are explicitly rejected. Remaining detail:
  StructField metadata vs tags for the column-level attributes.
- Some transformations genuinely are not expressible as state (split one
  column into two with logic, entity merges). These stay as registered
  convergence hooks on the signal seam (part 2) — the rare escape hatch,
  not the paradigm. Where is the line documented for users?
- Does a migration-run entity belong in kindling core or stay platform-side
  (a Delta table per workspace)? It must itself be exempt from migration.
- Is the storage-mode in-place overwrite worth keeping at all, versus staging
  a copy and atomically repointing (write new path, update locator), which
  would give storage mode the same verify-then-promote shape as catalog mode?
- How do migration signals interact with the SDP/Lakeflow declarative engine,
  where table lifecycle is owned by the downstream engine?

## Review evidence

Based on packages/kindling/migration.py, the `kindling migrate` CLI, the
entity declaration model, tests/unit/test_migration.py, and the
cross-platform migration test app. The focused unit suite
(`tests/unit/test_migration.py`) passes in isolation as of the guardrail-fix
change (61 tests); before it, two transform tests failed when the file ran
alone because building real Column expressions requires an active
SparkContext.
