# Obsolete proposals

Proposals in this folder are archived because the work they specified has since shipped or been superseded. They're kept in-tree rather than deleted for historical reference — use them to understand the design thinking behind features that are now in the codebase.

| Proposal | Status | What satisfies it today |
|---|---|---|
| [app_settings_overlay_model.md](./app_settings_overlay_model.md) | Implemented | App-local `settings.yaml`/platform/environment overlays are canonical; legacy `app.<platform|env>.yaml` reads remain only as an intentional compatibility shim |
| [blinker_events_implementation_plan.md](./blinker_events_implementation_plan.md) | Implemented | [packages/kindling/signaling.py](../../../packages/kindling/signaling.py) — `SignalPayload`, `BlinkerSignalProvider` |
| [blue_green_table_migrations.md](./blue_green_table_migrations.md) | Superseded | [packages/kindling/migration.py](../../../packages/kindling/migration.py) — `MigrationService` plan/apply/rollback/cleanup shipped instead of the dual-write/active-slot design |
| [config_based_entity_providers.md](./config_based_entity_providers.md) | Implemented | [packages/kindling/entity_resolution.py](../../../packages/kindling/entity_resolution.py) — `ConfigDrivenEntityNameMapper`, `ConfigDrivenEntityPathLocator` |
| [comprehensive_tracing_instrumentation.md](./comprehensive_tracing_instrumentation.md) | Implemented | PR #213; built-in bootstrap, execution, provider, migration, streaming, deploy, and CLI spans with tree-level tests |
| [config_driven_execution_options.md](./config_driven_execution_options.md) | Implemented | `generation_executor.py` resolves parallelism, timeout, caching, retry, and `skip_dependents` from `kindling.execution.*` config |
| [csv_provider_write_support.md](./csv_provider_write_support.md) | Implemented | [packages/kindling/entity_provider_csv.py](../../../packages/kindling/entity_provider_csv.py) — `CSVEntityProvider.write_to_entity`/`append_to_entity`. Streaming write deferred: no use case identified |
| [dag_execution_implementation_plan.md](./dag_execution_implementation_plan.md) | Implemented | [pipe_graph.py](../../../packages/kindling/pipe_graph.py), [execution_strategy.py](../../../packages/kindling/execution_strategy.py), [generation_executor.py](../../../packages/kindling/generation_executor.py) |
| [databricks_uc_vs_classic_capability_plan.md](./databricks_uc_vs_classic_capability_plan.md) | Implemented | [packages/kindling/features.py](../../../packages/kindling/features.py) — `databricks.uc_enabled`, `databricks.volumes_enabled` feature flags |
| [databricks_workspace_role_model.md](./databricks_workspace_role_model.md) | Implemented | [iac/databricks/workspace/variables.tf](../../../iac/databricks/workspace/variables.tf), [unity_catalog.tf](../../../iac/databricks/workspace/unity_catalog.tf) — `workspace_role` + `enable_kindling_*` toggles. `enable_kindling_platform_support` gates no resources yet |
| [domain_package_development.md](./domain_package_development.md) | Implemented | `kindling new` CLI (PR #57, shipped v0.9.0) |
| [entity_provider_roadmap.md](./entity_provider_roadmap.md) | Implemented | ADX API, Cosmos DB, and Parquet providers all shipped with their capability-specific behavior |
| [kindling_patterns_cli.md](./kindling_patterns_cli.md) | Superseded | The CLI adopted the narrower `app init --pattern` scaffold direction instead of a separate `kindling pattern` command group |
| [local_bootstrap_plan.md](./local_bootstrap_plan.md) | Implemented | Standalone as first-class platform in [bootstrap.py](../../../packages/kindling/bootstrap.py) |
| [local_code_first_development.md](./local_code_first_development.md) | Implemented | `kindling new` scaffold templates + unit/component/integration test tiers |
| [load_workspace_packages_rename.md](./load_workspace_packages_rename.md) | Implemented | `load_workspace_packages` is canonical throughout bootstrap, CLI templates, and docs; deprecated aliases remain temporarily for compatibility |
| [memory_provider_seed_rows.md](./memory_provider_seed_rows.md) | Implemented | `entity_provider_memory.py` materializes `provider.seed.rows`; usage is documented in the config reference and local Python-first guide |
| [pre_post_transform_analysis.md](./pre_post_transform_analysis.md) | Superseded | Signals framework provides the hook points; document itself notes "FULLY CONGRUENT" with signal_dag_streaming |
| [read_only_entities.md](./read_only_entities.md) | Implemented | [packages/kindling/entity_provider_delta.py](../../../packages/kindling/entity_provider_delta.py) — `ReadOnlyEntityError`, `EntityPathConflictError`, `_ensure_external_registration`, write guards |
| [secret_provider_service.md](./secret_provider_service.md) | Implemented | [platform_provider.py](../../../packages/kindling/platform_provider.py) — `SecretProvider`, `PlatformServiceSecretProvider` |
| [signal_dag_streaming_evaluation.md](./signal_dag_streaming_evaluation.md) | Historical | Evaluation doc for the proposal below; both superseded by shipped code |
| [signal_dag_streaming_meta_evaluation.md](./signal_dag_streaming_meta_evaluation.md) | Historical | Meta-review of the evaluation above; no ongoing value |
| [signal_dag_streaming_proposal.md](./signal_dag_streaming_proposal.md) | Implemented | Signals, DAG execution, and streaming orchestrator all shipped — see individual files under [packages/kindling/](../../../packages/kindling/) |
| [simplified_bootstrap.md](./simplified_bootstrap.md) | Implemented | v0.9.0/v0.9.1 refactor (single wheel + platform extras + entry-point loader + lazy platform imports) |
| [single_notebook_bootstrap.md](./single_notebook_bootstrap.md) | Rejected | Evaluation concluded wheel distribution remains the right approach; single-notebook embedding impractical |
| [scd_type2_support.md](./scd_type2_support.md) | Implemented | [entity_provider_delta.py](../../../packages/kindling/entity_provider_delta.py) — `DeltaMergeStrategies`, `SCD1/2MergeStrategy`, `_execute_scd2_merge`, `read_entity_as_of`; [data_entities.py](../../../packages/kindling/data_entities.py) — `SCDConfig`, `scd_config_from_tags`; [entity_provider_current_view.py](../../../packages/kindling/entity_provider_current_view.py) — `CurrentViewEntityProvider`. Phase 4 items deferred: #83, #84 |
| [scd_type2_implementation_plan.md](./scd_type2_implementation_plan.md) | Implemented | Task artifact for TASK-20260429-001; executed in PRs #77 and #82 |
| [sdp_engine_phase1_notes.md](./sdp_engine_phase1_notes.md) | Implemented | Phase-1 declaration planning, capability gating, bootstrap ordering, and provider write-inertness shipped in `kindling_ext_sdp`; later SDP phases are tracked by the active parent proposal |
| [stage_processor_signal_inversion.md](./stage_processor_signal_inversion.md) | Superseded | The framework retained direct orchestration and adopted aspects on persist signals rather than moving orchestration into signal subscribers |
| [temporal_condition_sources.md](./temporal_condition_sources.md) | Implemented | PR #224; `DataConditions.register(...)` and table/registry `condition_source` execution are implemented in `kindling_ext_temporal` |

Still-relevant proposals live one level up in [docs/proposals/](..).
