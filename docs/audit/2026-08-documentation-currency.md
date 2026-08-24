# Documentation Currency Audit — 2026-08

**Date run:** 2026-08-16
**Scope:** `docs/reference/`, `docs/contributing/`, `docs/proposals/`, and CLI-command docs, cross-checked against the actual code.
**Method:** one background research pass reading the proposal docs then present,
`config_reference.md`, `databricks_execution_contract.md`, `cli_reference.md`,
and referenced provider/CLI source, verifying claims against code and
file-existence checks rather than trusting prose. A 2026-08-17 reconciliation
also inspected the relevant SDP, Databricks, and temporal extension packages;
its corrections and proposal dispositions are recorded below.

This is a point-in-time snapshot, not a living document — code and docs both keep moving. Entries below are already annotated where later work in the same session resolved them; everything else was re-verified as still current at write time. Re-run the same kind of pass periodically rather than treating this file as authoritative indefinitely.

## Reconciliation — 2026-08-17

The original proposal table was compared with the extension implementations
and the repository's archival convention. The following completed records were
moved to `docs/proposals/obsolete/`:

- `app_settings_overlay_model.md`
- `comprehensive_tracing_instrumentation.md`
- `config_driven_execution_options.md`
- `entity_provider_roadmap.md`
- `load_workspace_packages_rename.md`
- `memory_provider_seed_rows.md`
- `sdp_engine_phase1_notes.md`
- `temporal_condition_sources.md`

Two unimplemented designs were archived as superseded:

- `kindling_patterns_cli.md` — superseded by `app init --pattern`.
- `stage_processor_signal_inversion.md` — superseded by the adopted
  aspect-on-persist-signals direction.

Three active proposal headers were corrected:

- `declarative_pipelines_engine.md` now records shipped Phases 1–3 and 5. The
  original audit's "Phase 1 shipped" verdict missed later code in
  `kindling_ext_sdp` and `kindling_ext_databricks`.
- `event_hub_kafka_transport.md` now distinguishes the shipped transport
  selector/Databricks behavior from the open cross-platform default policy.
- `package_config_architecture.md` now distinguishes shipped Phases 1–2 from
  the open issue #32 work.

`kindling_cli_devex_gaps.md` was omitted from the original supposedly complete
inventory even though it preceded this audit commit. It is an active proposal:
the config show/diff and entity-tag work has since begun, but its wider command
surface should be assessed against its own acceptance criteria before changing
its status.

`dataproc_platform_evaluation.md` remains active pending a product decision.
No Dataproc implementation exists, and its design predates the current split
between runtime platform services and design-time SDK submission APIs. Archive
it if Dataproc is no longer planned; otherwise rewrite it against the current
architecture before implementation.

## Status legend

- 🔴 **Stale** — doc contradicts current code (would mislead a reader).
- 🟡 **Missing** — real, shipped behavior with no doc coverage.
- ✅ **Resolved since audit** — fixed later in the same working session; kept here for the record.

---

## Stale documentation

### 🔴 `docs/guide/entity_configuration.md:36,62` — wrong tag key in example
Example code uses `"provider.type": "delta"`. The actual selector key read everywhere in code (`entity_provider_registry.py:167`, `simple_read_persist_strategy.py:287,330,345,351,359`, `migration.py:272`) is `provider_type` (no dot). `config_reference.md:248` documents `provider_type` correctly — only this guide's example is wrong. Following the guide's example as written silently falls back to the delta default instead of selecting the intended provider.

**Fix:** change `"provider.type"` → `"provider_type"` in both example blocks.

### 🔴 `docs/contributing/job_deployment.md:201,214` — references a file that doesn't exist
Claims a system test runner at `tests/system/runners/fabric_runner.py` ("Example FabricTestRunner using framework deployment"). No such file or `FabricTestRunner` symbol exists anywhere in the repo or git history.

**Fix:** either write the referenced example, or remove the reference and point at whatever the current job-deployment system test actually is (`tests/system/core/test_platform_job_deployment.py`).

### 🔴 `docs/contributing/azure_cloud_support.md:123` — deleted test file
References `tests/integration/test_azure_storage_example.py` as a runnable pytest target. The file is gone (only a stale `.pyc` remnant remains under `__pycache__`).

**Fix:** point at the current Azure storage integration coverage, or remove the runnable-example claim.

### 🔴 `docs/contributing/graph_based_pipe_execution_plan.md:402,412-414` — consolidated test files, doc never updated
References `tests/integration/test_graph_execution.py` and three per-platform files (`tests/system/test_{fabric,synapse,databricks}_job_deployment.py`). None exist — `.pyc` remnants confirm the per-platform files were real once and have since been consolidated into `tests/system/core/test_platform_job_deployment.py` (correctly referenced elsewhere, e.g. `platform_api_summary.md:44`). Graph-DAG coverage now actually lives in `tests/integration/test_pipe_graph_integration.py`.

**Fix:** update all four references to the consolidated file names.

---

## Missing documentation

### 🟡 Three EventHub-adjacent providers undocumented in `config_reference.md`
The "Provider Configuration via Entity Tags" section only documents `delta`, `csv`, `eventhub`, `memory`. Missing entirely:
- **parquet** (`entity_provider_parquet.py`): `provider.path`, `provider.merge_schema`, `provider.save_mode`, `provider.option.*`
- **adx-api** (`entity_provider_adx.py`): `provider.cluster`, `.database`, `.table`, `.query`, `.time_column`, `.lookback`, `.slice`, `.auth`, etc.
- **sql** (`entity_provider_sql.py`): `provider.table_name`
- **cosmos** (`packages/extensions/kindling_ext_cosmos`) — also absent

`docs/contributing/entity_providers.md` has the same gap (only covers Delta + generic interfaces).

### 🟡 `kindling.execution.*` config namespace entirely undocumented
`docs/proposals/obsolete/config_driven_execution_options.md` records Phases
1–3 shipped (#169, #173), and the code confirms it:
`generation_executor.py:505,583,604` reads
`kindling.execution.{parallel,max_workers,error_strategy,pipe_timeout,auto_cache,retry.*}`
and per-pipe `kindling.execution.pipes.<pipeid>.retry.*`. This is a
fully-shipped, actively-used config surface with zero entries in
`config_reference.md`.

### 🟡 `kindling.delta.ensure_on_write` undocumented
Read and acted on at `entity_provider_delta.py:1565` (skips pre-write destination-ensure when `false`) — not documented anywhere in `config_reference.md`'s Delta section.

### ✅ `provider.transport` (EventHub) — still missing as of this write-up
`entity_provider_eventhub.py:94-113` supports `auto`/`eventhubs`/`kafka`, auto-resolving to kafka on Databricks — not documented in the EventHub section of `config_reference.md`. **Not fixed** by this session's later EventHub work (`provider.preprocess`/`provider.amqp_headers` were added and documented, but this pre-existing gap on `provider.transport` was not touched). Still open.

---

## Proposal doc status vs. actual implementation

Verdict per file that was active when the original audit ran (excluding files
already under `docs/proposals/obsolete/` at that time), verified against the
described feature/module rather than trusting the doc's own status header.
Rows now archived retain their original names to preserve the audit trail.

| Proposal | Verdict |
|---|---|
| `app_settings_overlay_model.md` | Implemented apart from an intentionally retained compatibility shim; archived 2026-08-17 |
| `comprehensive_tracing_instrumentation.md` | Fully implemented; archived 2026-08-17 |
| `config_driven_execution_options.md` | Fully implemented (`generation_executor.py`); archived 2026-08-17, but see the missing config-reference coverage above |
| `dataproc_platform_evaluation.md` | Not started; product decision required because its architecture baseline is stale |
| `declarative_pipelines_engine.md` | Partially implemented — Phases 1–3 and 5 shipped across `kindling_ext_sdp` and `kindling_ext_databricks`; generic streaming/append-flow and parity work remains |
| `entity_provider_roadmap.md` | Fully implemented (ADX, Cosmos, Parquet); archived 2026-08-17 |
| `event_condition_episode_ontology.md` | Conceptual/ontology doc, not directly code-mapped |
| `event_hub_kafka_transport.md` | Partially implemented — `provider.transport` exists and defaults to Kafka on Databricks; header corrected 2026-08-17, while the cross-platform policy remains open |
| `governed_artifact_platform.md` | Not started — correctly marked "Concept (pre-proposal)" |
| `great_expectations_validation.md` | Not started — no code exists. **Now tracked**: [issue #245](https://github.com/sep/spark-kindling-framework/issues/245) |
| `json_column_processing.md` | Not started — tracked by issue #64 |
| `kindling_cli_devex_gaps.md` | Active proposal omitted from the original inventory; implementation has begun and needs an acceptance-criteria review before reclassification |
| `kindling_core_runner_split.md` | Not started, deliberately ("not yet scheduled"). **Now tracked**: [issue #246](https://github.com/sep/spark-kindling-framework/issues/246) |
| `kindling_patterns_cli.md` | Superseded by `app init --pattern`; archived 2026-08-17 |
| `load_workspace_packages_rename.md` | Fully implemented; archived 2026-08-17, with alias removal separately tracked by issue #202 |
| `memory_provider_seed_rows.md` | Fully implemented and documented; archived 2026-08-17 |
| `migration_fit_for_purpose_findings.md` | Findings doc, "Proposed." **Now tracked**: [issue #247](https://github.com/sep/spark-kindling-framework/issues/247) |
| `package_config_architecture.md` | Partially implemented — Phases 1–2 shipped; header corrected 2026-08-17, while Phase 3 remains tracked by issue #32 |
| `rest_api_entity_provider.md` | Not started — tracked by issue #200 |
| `sdp_engine_phase1_notes.md` | Fully implemented Phase-1 record; archived 2026-08-17 |
| `stage_processor_signal_inversion.md` | Superseded by the aspect-on-persist-signals direction; archived 2026-08-17 |
| `surrogate_keys.md` | Not started (Phase 1 only) — tracked by issue #199 |
| `temporal_condition_sources.md` | Fully implemented by #224; archived 2026-08-17 |
| `temporal_event_segmentation.md` | Partially implemented — accurately self-described ("Draft... first executable slice landed") |

---

## Verified accurate (no action needed)

- `docs/contributing/databricks_execution_contract.md` — every file path, test name, function signature, and job-config key checked and matches code exactly, including the `tests/system/extensions/databricks/` → `tests/system/core/` test-file move.
- `docs/reference/cli_reference.md` — command groups/flags for `config set`, `runner register/status/delete/invoke`, `agent setup` all cross-checked against `cli.py` and match. Minor: the deprecated `app add executor` command has no doc entry, but it's deprecated, so low priority.
- CSV, EventHub (aside from `provider.transport`), Memory, and Delta provider tag docs in `config_reference.md` all cross-checked line-by-line against provider source and match.

---

## Follow-up

The proposal-status reconciliation is recorded above. The unrelated stale and
missing documentation findings remain a tracking record rather than a claim
that those fixes have shipped. The three "Now tracked" proposal gaps have
GitHub issues #245, #246, and #247; the other documentation gaps should be
re-verified before being closed.
