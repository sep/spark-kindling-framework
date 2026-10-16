# Documentation and issue reconciliation — 2026-10-16

Baseline: `origin/main` at `694e585` (0.14.0rc1 source), reviewed in an isolated
`docs/full-reconciliation-2026-10-16` worktree. This is a source reconciliation,
not a certification of cloud behavior or external vendor support.

## Coverage and method

The baseline has 222 tracked Markdown files, including guides, reference pages,
all READMEs, agent guidance, shipped agent-reference/template docs, proposals,
release notes and earlier audits. The companion [inventory](2026-10-16-document-inventory.csv)
accounts for every baseline file and its treatment. Repository-wide scans checked
local Markdown destinations, Python example import names and stale source/test
paths; focused source/test inspection checked lifecycle, provider, configuration,
CLI, deployment, temporal and proposal-status claims. Static scans cannot prove
that every example executes or every sentence is correct. Historical releases,
closed issue descriptions and archived designs retain their original context;
only navigational corrections are made where needed.

GitHub inventory: 79 issues (24 open, 55 closed), retrieved with
`gh issue list --state all --limit 1000` on 2026-10-16. The result is below the
limit. Each issue has a disposition below. Closed issue bodies often describe
pre-fix behavior; those are not current product documentation. Issue comments
and live cloud deployments were not exhaustively replayed. No issue is closed
merely because a matching symbol exists.

## Corrections in this change

- Archive seven delivered proposal records and repair their Markdown links.
- Record CLI proposal acceptance gaps, current streaming lowering, Event Hub
  correctness priorities, current uv/PyPI workflow and pending config controls.
- Add missing CLI diagnostic/bundle coverage and execution/retry, Parquet,
  ADX API, SQL entity, Event Hub transport and Delta ensure configuration docs.
- Fix `provider.type` versus `provider_type`, the nonexistent `sql` selector,
  removed `EntityProvider` imports, the SDK `FabricAPI` import and injector import.
- Correct temporal multi-generation limitations and one-time versus registered jobs.
- Correct obsolete test paths, broken relative links and current test commands.
- Mark historical plans/evaluations as such rather than silently presenting
  proposed APIs, old dependency workflows or future features as current behavior.

## Acceptance review: CLI proposal (#260)

| Criterion | Evidence and disposition |
|---|---|
| Exact bootstrap-equivalent config | **Not met.** `_load_effective_raw_config` reads only base/platform/environment YAML in the app settings directory. Shared/workspace overlays, runtime environment and SparkConf injection, dynamic values and resolved secrets are outside that path. Existing tests verify YAML merges, not a real-bootstrap equality assertion. |
| Entity tag secret redaction/reveal warning | **Partially met.** `_build_tag_view` / `entity_tags` cover YAML-derived redaction, but the source documents that already-resolved secrets from code-declared tags can lose provenance and escape redaction. Config show/diff operate on raw references, and omit reveal warnings in JSON mode; do not describe them as resolved-secret inspection. |
| Additive entity/pipe commands | Commands and targeted tests exist. No evidence here requires removing existing inspect/list commands. |
| Health-check exit codes | `test_cli_health_checks.py` covers healthy/import/graph/build failures and JSON output. Package builds now use uv; unavailable version skew is a note. |
| Trace equivalence/help | `test_cli_trace_flags.py` covers parameter precedence, local environment mapping and help. |

Keep the proposal active. Close the code-declared secret provenance/redaction gap. Resolve whether exact runtime parity is still desired
and implement/test it, or explicitly accept a narrower raw-YAML contract. Do not
archive solely on the basis that PR #250 added the command names.

## Proposal dispositions

All 29 proposals active at baseline are accounted for here. Previously archived
records remain historical; outstanding work referenced by them has separate
trackers (#32, #66, #67, #155, #168, #202, #203), so an archive does not mean every
idea in a design sketch shipped.

| Proposal | Disposition | Evidence / residual |
|---|---|---|
| [autoloader_file_ingestion](../proposals/obsolete/autoloader_file_ingestion.md) | Archived: implemented | Auto Loader extension and per-entry discovery; #228. |
| [collector_driving_inputs](../proposals/collector_driving_inputs.md) | Retain: partial | `driving_entity_ids`, batch skip/watermark and temporal multi-source chains exist; collector sugar remains proposed. |
| [cosmos_streaming_and_throughput_controls](../proposals/obsolete/cosmos_streaming_and_throughput_controls.md) | Archived: implemented | Cosmos change-feed, merge and throughput options; #201. Connector value/default deviations documented. |
| [databricks_bundle_deployment](../proposals/databricks_bundle_deployment.md) | Retain: partial | `bundle.py` renders bundles and inline settings. Wrapper commands, wheel building/dependency closure and remote deletion protections remain. |
| [dataproc_platform_evaluation](../proposals/dataproc_platform_evaluation.md) | Retain: parked | No implementation; #69 requires architecture refresh if scheduled. |
| [declarable_streaming_sources](../proposals/obsolete/declarable_streaming_sources.md) | Archived: implemented scope | Databricks lowering shipped; OSS capability gate is intentional, not a promise of OSS support. |
| [declaration_derivations](../proposals/obsolete/declaration_derivations.md) | Archived: implemented | Entity/pipe derivation and tests in test_declaration_derivations.py. |
| [declarative_pipelines_engine](../proposals/declarative_pipelines_engine.md) | Update: partial | Provider streams and temporal strata shipped in addition to original phases. Generic non-source streaming and parity work remain. |
| [domain_devcontainer_contract](../proposals/domain_devcontainer_contract.md) | Update: partial | uv workspaces/PyPI pins/version reporting supersede old Poetry/URL-only language; candidate/image/CI-parity contract remains incomplete. |
| [episode_analytical_facts](../proposals/episode_analytical_facts.md) | Retain: proposed | Membership foundation remains #256; do not depend on fan-in #257. |
| [event_condition_episode_ontology](../proposals/event_condition_episode_ontology.md) | Retain: conceptual | Ontology is not an implementation checklist. Temporal package implements only a subset; #206. |
| [event_hub_kafka_transport](../proposals/event_hub_kafka_transport.md) | Update: partial | Kafka correctness precedes platform-default flip; #198. |
| [fan_in_upsert_pipes](../proposals/fan_in_upsert_pipes.md) | Retain: deferred | No owned-column patch-upsert contract; #257. |
| [governed_artifact_platform](../proposals/governed_artifact_platform.md) | Retain: concept | No governed artifact lifecycle implementation or dedicated issue in this snapshot; product decision needed. |
| [great_expectations_validation](../proposals/great_expectations_validation.md) | Retain: proposed | Provider-neutral foundation first, optional GX adapter later; #245. |
| [iceberg_entity_provider](../proposals/iceberg_entity_provider.md) | Retain: proposed | No Iceberg provider or dedicated issue in snapshot. Platform preview claims are historical, not revalidated here. |
| [json_column_processing](../proposals/json_column_processing.md) | Retain: proposed | Spark-native helpers first; provider out of first delivery, #64. |
| [kindling_cli_devex_gaps](../proposals/kindling_cli_devex_gaps.md) | Update: partial | Commands implemented; exact runtime parity acceptance not met, #260. |
| [kindling_core_runner_split](../proposals/kindling_core_runner_split.md) | Retain: deferred | No runner split; #246. |
| [migration_fit_for_purpose_findings](../proposals/migration_fit_for_purpose_findings.md) | Retain: partial findings/proposed extensions | Guardrails shipped; rename/default/backfill, durability and verification remain #247. |
| [package_config_architecture](../proposals/package_config_architecture.md) | Update: partial | Overlay/patterns and CLI introspection exist; underscore control semantics remain #32. |
| [poetry_to_uv_migration](../proposals/obsolete/poetry_to_uv_migration.md) | Archived: implemented | Root/CLI/SDK uv workspace, uv_build metadata, migration script, generated scaffolds and CI. |
| [rest_api_entity_provider](../proposals/rest_api_entity_provider.md) | Retain: proposed | No provider implementation; #200. |
| [sdp_auto_clustering](../proposals/sdp_auto_clustering.md) | Retain: proposed | Emitter still forwards auto literally. Rejection stopgap and full support remain #258. |
| [settings_discovery_by_convention](../proposals/obsolete/settings_discovery_by_convention.md) | Archived: implemented | config_dir/app_dir hierarchy and deprecation tests. Core config_files compatibility intentionally remains. |
| [surrogate_keys](../proposals/surrogate_keys.md) | Retain: proposed | No generic surrogate declaration/aspect; #199. |
| [temporal_event_segmentation](../proposals/temporal_event_segmentation.md) | Retain: partial | MVP, registry conditions and iterative chains shipped; #206 residuals remain. |
| [temporal_lakeflow_execution_mode](../proposals/obsolete/temporal_lakeflow_execution_mode.md) | Archived: implementation delivered | Mode parser/config bridge, batch/streaming lowerer and local unit/integration tests exist. Cloud acceptance was not rerun; no new live-support claim. |
| [unified_otlp_telemetry_provider](../proposals/unified_otlp_telemetry_provider.md) | Retain: proposed | Existing Azure Monitor distro extension remains; no unified OTLP/Zerobus adapter or dedicated issue. Preview claims need fresh vendor review before scheduling. |

## GitHub issue dispositions

State is the retrieved state. Issue bodies #260, #206, #168, #202, #203 and
#155 were updated during this reconciliation: acceptance gaps, shipped temporal
behavior, the superseded inversion dependency and stale proposal/README paths.
Each update was read back and verified; all issue states were preserved.
Documentation changes are proposed in [PR #358](https://github.com/sep/spark-kindling-framework/pull/358).

| Issue | State | Disposition and source evidence |
|---|---|---|
| [#6](https://github.com/sep/spark-kindling-framework/issues/6) [Feature]: DAG-Based Pipeline Ordering with Parallelization and Caching | closed | `execution_orchestrator.py`, `pipe_graph.py`, `execution_strategy.py`, `generation_executor.py`, `cache_optimizer.py` and matching tests. Keep closed; resume remains #203. |
| [#7](https://github.com/sep/spark-kindling-framework/issues/7) [Feature]: Support for Views - Hybrid Entity/DataPipe Pattern | closed | `DataPipes.view` and SQL entities exist with their implemented APIs; original standalone decorators are historical design, not promised names. Keep closed. |
| [#8](https://github.com/sep/spark-kindling-framework/issues/8) [Feature]: Column-Level Lineage Analyzer | open | **Keep deferred** — No ColumnLineage API; entity/pipe DAG and visualization are not column-expression lineage. Any design must respect the JVM boundary. |
| [#9](https://github.com/sep/spark-kindling-framework/issues/9) [Feature]: Python-First Local Development Support | closed | CLI local workflow, scaffold and DI integration paths exist; `test_cli_local_dev_dx.py`, `test_scaffold.py`, `test_di_wiring_standalone.py`. Historical scope/duplicates; retain closed state, do not claim every original sketch shipped verbatim. |
| [#10](https://github.com/sep/spark-kindling-framework/issues/10) [Feature]: Python-First Local Development Support | closed | CLI local workflow, scaffold and DI integration paths exist; `test_cli_local_dev_dx.py`, `test_scaffold.py`, `test_di_wiring_standalone.py`. Historical scope/duplicates; retain closed state, do not claim every original sketch shipped verbatim. |
| [#11](https://github.com/sep/spark-kindling-framework/issues/11) [Bug]: No indication when running a fileingestor that references a non-existent destination entity | closed | File ingestion reports missing destination declarations; `test_file_ingestion.py`. Keep closed. |
| [#13](https://github.com/sep/spark-kindling-framework/issues/13) [Feature]: Azure Monitor OpenTelemetry Integration for Enhanced Telemetry | closed | OTel Azure extension/providers shipped; delivery assertions remain #155 and platform test matrix #357. Keep closed. |
| [#14](https://github.com/sep/spark-kindling-framework/issues/14) [Capability] Production Streaming Essentials | closed | Streaming manager, health monitor, recovery and event queue implementations/tests. Keep closed; dimension restart remains #67. |
| [#15](https://github.com/sep/spark-kindling-framework/issues/15) [Capability] Unified DAG Orchestrator | closed | `execution_orchestrator.py`, `pipe_graph.py`, `execution_strategy.py`, `generation_executor.py`, `cache_optimizer.py` and matching tests. Keep closed; resume remains #203. |
| [#16](https://github.com/sep/spark-kindling-framework/issues/16) [Capability] Flexible Entity Providers | closed | Provider registry and Parquet/memory/CSV capability implementations with provider tests. Keep closed. |
| [#17](https://github.com/sep/spark-kindling-framework/issues/17) [Capability] Configuration Enhancements | closed | Config patterns and post-registration overlays exist; config pattern/overlay tests. Keep closed; tag controls remain #32. |
| [#18](https://github.com/sep/spark-kindling-framework/issues/18) [Feature] Streaming Query Manager | closed | Streaming manager, health monitor, recovery and event queue implementations/tests. Keep closed; dimension restart remains #67. |
| [#19](https://github.com/sep/spark-kindling-framework/issues/19) [Feature] Streaming Health Watchdog | closed | Streaming manager, health monitor, recovery and event queue implementations/tests. Keep closed; dimension restart remains #67. |
| [#20](https://github.com/sep/spark-kindling-framework/issues/20) [Feature] Auto-Recovery Manager | closed | Streaming manager, health monitor, recovery and event queue implementations/tests. Keep closed; dimension restart remains #67. |
| [#21](https://github.com/sep/spark-kindling-framework/issues/21) [Feature] Queue-Based Event Processing | closed | Streaming manager, health monitor, recovery and event queue implementations/tests. Keep closed; dimension restart remains #67. |
| [#22](https://github.com/sep/spark-kindling-framework/issues/22) [Feature] Dependency Graph Builder | closed | `execution_orchestrator.py`, `pipe_graph.py`, `execution_strategy.py`, `generation_executor.py`, `cache_optimizer.py` and matching tests. Keep closed; resume remains #203. |
| [#23](https://github.com/sep/spark-kindling-framework/issues/23) [Feature] Execution Strategies | closed | `execution_orchestrator.py`, `pipe_graph.py`, `execution_strategy.py`, `generation_executor.py`, `cache_optimizer.py` and matching tests. Keep closed; resume remains #203. |
| [#24](https://github.com/sep/spark-kindling-framework/issues/24) [Feature] Generation Executor | closed | `execution_orchestrator.py`, `pipe_graph.py`, `execution_strategy.py`, `generation_executor.py`, `cache_optimizer.py` and matching tests. Keep closed; resume remains #203. |
| [#25](https://github.com/sep/spark-kindling-framework/issues/25) [Feature] Cache Optimization | closed | `execution_orchestrator.py`, `pipe_graph.py`, `execution_strategy.py`, `generation_executor.py`, `cache_optimizer.py` and matching tests. Keep closed; resume remains #203. |
| [#26](https://github.com/sep/spark-kindling-framework/issues/26) [Feature] Provider Registry & Adapter | closed | Provider registry and Parquet/memory/CSV capability implementations with provider tests. Keep closed. |
| [#27](https://github.com/sep/spark-kindling-framework/issues/27) [Feature] CSV Entity Provider | closed | Provider registry and Parquet/memory/CSV capability implementations with provider tests. Keep closed. |
| [#28](https://github.com/sep/spark-kindling-framework/issues/28) [Feature] In-Memory Provider | closed | Provider registry and Parquet/memory/CSV capability implementations with provider tests. Keep closed. |
| [#29](https://github.com/sep/spark-kindling-framework/issues/29) [Feature] Parquet Provider | closed | Provider registry and Parquet/memory/CSV capability implementations with provider tests. Keep closed. |
| [#30](https://github.com/sep/spark-kindling-framework/issues/30) [Feature] Config Override Overlay (formerly Two-Phase Registration) | closed | Config patterns and post-registration overlays exist; config pattern/overlay tests. Keep closed; tag controls remain #32. |
| [#31](https://github.com/sep/spark-kindling-framework/issues/31) [Feature] Wildcard Config Patterns | closed | Config patterns and post-registration overlays exist; config pattern/overlay tests. Keep closed; tag controls remain #32. |
| [#32](https://github.com/sep/spark-kindling-framework/issues/32) [Feature] Tag Management | open | **Keep open** — `config_patterns.py` explicitly treats underscore keys as ordinary merged metadata; no disable/remove semantics. |
| [#42](https://github.com/sep/spark-kindling-framework/issues/42) [Feature] Simplified Bootstrap & Dynamic Loading | closed | Single bootstrap script and initialize entrypoint exist; bootstrap tests. Keep closed. |
| [#53](https://github.com/sep/spark-kindling-framework/issues/53) Finalize #15: ExecutionOrchestrator facade + DataPipesExecuter DAG entrypoint wiring | closed | `execution_orchestrator.py`, `pipe_graph.py`, `execution_strategy.py`, `generation_executor.py`, `cache_optimizer.py` and matching tests. Keep closed; resume remains #203. |
| [#62](https://github.com/sep/spark-kindling-framework/issues/62) [Feature] Blue/Green Table Migrations | closed | Desired-state `migration.py` supersedes the blue/green sketch; do not reopen to implement the rejected slot design. Remaining migration work is #247. |
| [#63](https://github.com/sep/spark-kindling-framework/issues/63) [Feature] SCD Type 2 Entity Support | closed | SCD configuration, Delta merge and registration guards implemented; `test_scd_config.py`, `test_scd2_registration.py`, `test_scd2_as_of.py`. Keep closed. |
| [#64](https://github.com/sep/spark-kindling-framework/issues/64) [Feature] JSON Column Processing | open | **Keep open** — No json_transforms module or generic JSON-processing API; ad-hoc Spark JSON use is not the proposed capability. |
| [#65](https://github.com/sep/spark-kindling-framework/issues/65) [Feature] Pre/Post Transform Interceptors | closed | Read/persist DataFrame intercepts in `simple_read_persist_strategy.py` and signal tests. Keep closed; streaming hooks remain #168. |
| [#66](https://github.com/sep/spark-kindling-framework/issues/66) [Feature] Typed Signal Payloads & Introspection | open | **Keep open** — `signaling.py` provides SignalPayload and get_emitted_signals; concrete typed families and generated reference/discovery tooling remain. |
| [#67](https://github.com/sep/spark-kindling-framework/issues/67) [Feature] Dimension-Driven Stream Restarts | open | **Keep deferred** — No DimensionChangeDetector or StreamRestartController. Streaming recovery is not dimension-refresh orchestration. |
| [#68](https://github.com/sep/spark-kindling-framework/issues/68) [Feature] Secret Provider Contract Completion | closed | `secret_provider.py` / `secret_provider_loader.py` and loader tests. Keep closed. |
| [#69](https://github.com/sep/spark-kindling-framework/issues/69) [Feature] Google Cloud Dataproc Platform Support | open | **Keep parked** — No Dataproc platform registration or SDK module. Rewrite the old architecture baseline if scheduled. |
| [#70](https://github.com/sep/spark-kindling-framework/issues/70) [Feature] Single-Notebook Bootstrap Delivery Decision | closed | Single bootstrap script and initialize entrypoint exist; bootstrap tests. Keep closed. |
| [#78](https://github.com/sep/spark-kindling-framework/issues/78) fix(scd2): point_in_time type inconsistency in read_entity_as_of | closed | SCD configuration, Delta merge and registration guards implemented; `test_scd_config.py`, `test_scd2_registration.py`, `test_scd2_as_of.py`. Keep closed. |
| [#79](https://github.com/sep/spark-kindling-framework/issues/79) fix(scd2): validate __merge_key is not reserved in entity schema | closed | SCD configuration, Delta merge and registration guards implemented; `test_scd_config.py`, `test_scd2_registration.py`, `test_scd2_as_of.py`. Keep closed. |
| [#80](https://github.com/sep/spark-kindling-framework/issues/80) fix(scd2): concat routing key drops nulls causing incorrect merge matches | closed | SCD configuration, Delta merge and registration guards implemented; `test_scd_config.py`, `test_scd2_registration.py`, `test_scd2_as_of.py`. Keep closed. |
| [#81](https://github.com/sep/spark-kindling-framework/issues/81) fix(scd2): validate scd.current_entity_id differs from base entity id | closed | SCD configuration, Delta merge and registration guards implemented; `test_scd_config.py`, `test_scd2_registration.py`, `test_scd2_as_of.py`. Keep closed. |
| [#83](https://github.com/sep/spark-kindling-framework/issues/83) [Feature] SCD2 close_on_missing — close current rows absent from source batch | closed | SCD configuration, Delta merge and registration guards implemented; `test_scd_config.py`, `test_scd2_registration.py`, `test_scd2_as_of.py`. Keep closed. |
| [#84](https://github.com/sep/spark-kindling-framework/issues/84) [Feature] SCD2 optimize_unchanged — skip hash-unchanged rows before merge | closed | SCD configuration, Delta merge and registration guards implemented; `test_scd_config.py`, `test_scd2_registration.py`, `test_scd2_as_of.py`. Keep closed. |
| [#85](https://github.com/sep/spark-kindling-framework/issues/85) Local Python-first DX gaps: no run command, ABFSS-first scaffold, missing validate/introspect | closed | CLI local workflow, scaffold and DI integration paths exist; `test_cli_local_dev_dx.py`, `test_scaffold.py`, `test_di_wiring_standalone.py`. Historical scope/duplicates; retain closed state, do not claim every original sketch shipped verbatim. |
| [#87](https://github.com/sep/spark-kindling-framework/issues/87) [DX] Post-#85 eval: 2 blocking gaps + 3 high gaps in local dev flow | closed | CLI local workflow, scaffold and DI integration paths exist; `test_cli_local_dev_dx.py`, `test_scaffold.py`, `test_di_wiring_standalone.py`. Historical scope/duplicates; retain closed state, do not claim every original sketch shipped verbatim. |
| [#88](https://github.com/sep/spark-kindling-framework/issues/88) [Testing] CLI component tests mock DI boundary — real DI wiring failures invisible | closed | CLI local workflow, scaffold and DI integration paths exist; `test_cli_local_dev_dx.py`, `test_scaffold.py`, `test_di_wiring_standalone.py`. Historical scope/duplicates; retain closed state, do not claim every original sketch shipped verbatim. |
| [#90](https://github.com/sep/spark-kindling-framework/issues/90) [DX Eval] Full developer experience gaps — post-#85/#87/#88 evaluation (G1–G13) | closed | CLI local workflow, scaffold and DI integration paths exist; `test_cli_local_dev_dx.py`, `test_scaffold.py`, `test_di_wiring_standalone.py`. Historical scope/duplicates; retain closed state, do not claim every original sketch shipped verbatim. |
| [#155](https://github.com/sep/spark-kindling-framework/issues/155) [Feature] Assert Azure Monitor telemetry delivery end-to-end in system test | open | **Fix README path; keep open** — `tests/system/extensions/azure-monitor/test_azure_monitor_extension.py` does not assert telemetry ingestion through a query API. |
| [#168](https://github.com/sep/spark-kindling-framework/issues/168) [Feature] Streaming-native staged signals for SimplePipeStreamStarter | open | **Correct superseded dependency; keep open** — `pipe_streaming.py` does not provide the proposed staged read/persist hooks. Stage-processor signal inversion is archived/superseded; do not make it a prerequisite. |
| [#196](https://github.com/sep/spark-kindling-framework/issues/196) Sandbox: agent harness pilot (throwaway) | closed | Throwaway sandbox issue; not a framework capability commitment. Preserve closed state. |
| [#198](https://github.com/sep/spark-kindling-framework/issues/198) [Feature] Event Hub Kafka correctness first; cross-platform auto transport afterward | open | **Keep open, correctness first** — `entity_provider_eventhub.py` still uses failOnDataLoss=false, a shaded login module and consumer-group-to-kafka.group.id mapping. Cross-platform auto remains distinct. |
| [#199](https://github.com/sep/spark-kindling-framework/issues/199) [Feature] Declarative surrogate keys — Phase 1 (hash strategy + persist aspect) | open | **Keep open** — No generic SurrogateKeyAspect or surrogate declaration sugar. Temporal event hashes and Delta routing keys are narrower facilities. |
| [#200](https://github.com/sep/spark-kindling-framework/issues/200) [Feature] REST API entity provider — Phase 1 (read-only, OpenAPI-typed, incremental cursor) | open | **Keep open** — No rest-api provider registered in `entity_provider_registry.py`; no OpenAPI provider implementation. |
| [#201](https://github.com/sep/spark-kindling-framework/issues/201) [Feature] Cosmos DB change-feed streaming source + formal merge_to_entity | closed | Cosmos change-feed/merge/throughput support and provider unit tests. Keep closed; archive implemented record. |
| [#202](https://github.com/sep/spark-kindling-framework/issues/202) [Feature] Remove deprecated load_local config aliases | open | **Fix proposal path; keep open** — Deprecated load_local aliases still occur in bootstrap/config/shim compatibility paths; archived rename record does not imply alias removal. |
| [#203](https://github.com/sep/spark-kindling-framework/issues/203) [Feature] ExecutionOrchestrator.resume() for failed-pipe re-execution | open | **Fix proposal path; keep open** — `execution_orchestrator.py` has no resume method. Retry/skip_dependents in `generation_executor.py` do not resume a prior result. |
| [#206](https://github.com/sep/spark-kindling-framework/issues/206) [Feature] Temporal extension post-MVP: declarative conditions/events/episodes next slice | open | **Update docs; keep open** — `chain.py` already iterates to quiescence/max_generations; remove the README multi-generation limitation. Late closed-episode revision, streaming driver and other residuals remain. |
| [#207](https://github.com/sep/spark-kindling-framework/issues/207) [Feature] Artifact store abstraction: support UC Volumes (and local paths) as the artifacts location in CLI/SDK | closed | `kindling_sdk/artifact_store.py` supports Azure/Volumes/local stores; `test_artifact_store.py`. Keep closed. |
| [#210](https://github.com/sep/spark-kindling-framework/issues/210) [Feature] Comprehensive built-in tracing instrumentation across kindling | closed | `trace_ops.py`, bootstrap/provider/execution spans and tracing-tree tests. Keep closed. |
| [#211](https://github.com/sep/spark-kindling-framework/issues/211) [Bug] Databricks pre-flight demands DATABRICKS_TOKEN even though az-cli and service-principal auth are supported | closed | SDK Databricks artifact resolution, cluster access mode, jobs.submit and CLI auth alternatives exist; platform SDK/preflight unit tests. Keep closed; live cloud not rerun. |
| [#214](https://github.com/sep/spark-kindling-framework/issues/214) [Bug] DatabricksAPI._submit_one_time_run crashes: self.client.jobs.runs.submit does not exist | closed | SDK Databricks artifact resolution, cluster access mode, jobs.submit and CLI auth alternatives exist; platform SDK/preflight unit tests. Keep closed; live cloud not rerun. |
| [#216](https://github.com/sep/spark-kindling-framework/issues/216) [Bug] DatabricksAPI job submission ignores KINDLING_ARTIFACTS_STORAGE_PATH — produces invalid python_file for Volumes-only workspaces | closed | SDK Databricks artifact resolution, cluster access mode, jobs.submit and CLI auth alternatives exist; platform SDK/preflight unit tests. Keep closed; live cloud not rerun. |
| [#217](https://github.com/sep/spark-kindling-framework/issues/217) [Bug] Databricks job clusters default to non-UC access mode, cannot read python_file from Volumes | closed | SDK Databricks artifact resolution, cluster access mode, jobs.submit and CLI auth alternatives exist; platform SDK/preflight unit tests. Keep closed; live cloud not rerun. |
| [#220](https://github.com/sep/spark-kindling-framework/issues/220) Add merge_to_entity (SCD2) support to MemoryEntityProvider | closed | Temporal conditions registry/merge guard and memory SCD2 merge exist; temporal condition tests and `test_entity_provider_memory_scd2.py`. Keep closed. |
| [#221](https://github.com/sep/spark-kindling-framework/issues/221) Guard ingest_conditions() merge_to_entity call with a clear error | closed | Temporal conditions registry/merge guard and memory SCD2 merge exist; temporal condition tests and `test_entity_provider_memory_scd2.py`. Keep closed. |
| [#222](https://github.com/sep/spark-kindling-framework/issues/222) Registry-declared temporal conditions (alongside table-backed rules-as-data) | closed | Temporal conditions registry/merge guard and memory SCD2 merge exist; temporal condition tests and `test_entity_provider_memory_scd2.py`. Keep closed. |
| [#228](https://github.com/sep/spark-kindling-framework/issues/228) [Feature] Wire Databricks Auto Loader (cloudFiles) into FileIngestionProcessor, scoped per entry | closed | Auto Loader extension and per-entry ingestion dispatch; `test_databricks_autoloader_file_ingestion.py`. Keep closed. |
| [#245](https://github.com/sep/spark-kindling-framework/issues/245) [Feature] First-class data validation with a Great Expectations adapter | open | **Keep open** — No `DataValidations` registry or optional GX adapter package. Databricks expectations are engine-specific, not this provider-neutral contract. |
| [#246](https://github.com/sep/spark-kindling-framework/issues/246) [Feature] Split kindling core (declaration framework) from a kindling_runner execution engine | open | **Keep deferred** — Executor/strategy/streaming still reside in `packages/kindling`; no separate `kindling_runner` package. |
| [#247](https://github.com/sep/spark-kindling-framework/issues/247) [Feature] Migration: unambiguous convergence for renames, defaults/backfills, and durable run records | open | **Keep open, staged** — `migration.py` provides desired-state schema convergence; rename/default/backfill declarations and durable locked run records remain outside its implemented contract. |
| [#256](https://github.com/sep/spark-kindling-framework/issues/256) [Feature] Episode membership foundation for analytical facts | open | **Keep open** — Temporal events/episodes and deterministic identity exist; no reusable episode-membership declaration/derivation API in the temporal package. |
| [#257](https://github.com/sep/spark-kindling-framework/issues/257) [Feature] Keyed fan-in upserts into one entity | open | **Keep deferred** — No partial-column ownership/null-update contract or keyed multi-producer upsert API in `data_pipes.py` / `pipe_graph.py`. |
| [#258](https://github.com/sep/spark-kindling-framework/issues/258) [Feature] Auto liquid clustering support in the SDP/Lakeflow declarative path | open | **Keep open** — `kindling_ext_sdp/oss_engine.py::_declaration_kwargs` forwards cluster columns as `cluster_by`; no auto-sentinel capability handling/rejection. |
| [#259](https://github.com/sep/spark-kindling-framework/issues/259) [Feature] Declarable streaming sources for Lakeflow (Event Hub over Kafka) | closed | DeclarableStreamingSource capability, provider source specs and Databricks append-flow lowering; `test_sdp_streaming_source_lowering.py`. Keep closed; archive implemented proposal. |
| [#260](https://github.com/sep/spark-kindling-framework/issues/260) [Docs] Acceptance-criteria review for kindling_cli_devex_gaps.md before reclassifying status | open | **Update scope; keep open** — Seven diagnostic commands and trace flags exist. `_load_effective_raw_config` only merges app-local YAML; no real-bootstrap equality test. See acceptance review below. |
| [#298](https://github.com/sep/spark-kindling-framework/issues/298) Decide: remove the deprecated kindling.lakeflow.config_files alias surface (post-#281) | closed | Removed alias is recognized only to warn/drop in `lakeflow_app_selector.py`; inline settings is canonical. Keep closed. |
| [#333](https://github.com/sep/spark-kindling-framework/issues/333) [Feature] Databricks serverless job execution for app run and runner register | open | **Keep open** — `kindling_sdk/platform_databricks.py` and CLI expose classic cluster configuration. Bundle serverless is a separate execution path. |
| [#334](https://github.com/sep/spark-kindling-framework/issues/334) [Bug] Warn when Databricks bootstrap staging falls back to DBFS root | open | **Keep open** — `runtime/scripts/kindling_bootstrap.py` and `bootstrap.py` stage before downloaded settings. Diagnostics requested by the issue are still missing; clarify settings-only temp_path in docs. |
| [#357](https://github.com/sep/spark-kindling-framework/issues/357) [Feature] Run extension system tests on Fabric and Synapse in CI | open | **Keep open** — `.github/workflows/ci.yml` resolves extension dependencies; only `databricks-system-tests.yml` dispatches extension system tests. Fabric/Synapse install/import coverage remains. |

## Remaining follow-up

1. #260: decide and test configuration parity; the raw-YAML contract is now
   documented accurately. Exact bootstrap parity must not be inferred from shared
   merge rules alone.
2. #198 and #258 remain correctness gaps; documentation reconciliation does not
   change Kafka data-loss/offset semantics or implement auto clustering.
3. #206, #247, #245 and #256 remain scoped capability work. Preserve the current
   desired-state migration architecture and avoid inventing new versioned scripts.
4. Bundle and domain-devcontainer proposals remain partial. The concept-only
   governed-artifact, Iceberg and unified-OTLP proposals have no dedicated issue
   in this inventory. Create scoped implementation trackers when scheduled;
   do not infer product approval from a proposal file.
5. Historical analysis contains illustrative commands and support claims. The
   current testing guide and implementation take precedence. External cloud
   preview/status claims and all historical release claims were not revalidated
   against vendor sites or redeployed to cloud platforms.
6. A literal line-by-line semantic proof of all 222 files and execution of every
   example is outside the evidence produced here. The inventory distinguishes
   focused reconciliations from static/historical review so subsequent reviews
   can deepen coverage without assuming a clean scan proves correctness.

## Validation

Targeted repository task: `poe test-unit`, with `PYTEST_ADDOPTS` selecting CLI
config/entity/health/trace, settings discovery, temporal Lakeflow mode,
derivations, Cosmos, Auto Loader, declarable streaming sources and uv migration.
Result: **291 passed, 1 skipped, 3208 deselected**, one upstream pytest warning.
No cloud integration/system suite was run. Markdown link/import checks and
`git diff --check` passed. All local Markdown link destinations resolve; the
static Python-example import scan found no remaining missing Kindling module or
symbol names in current non-proposal/non-release docs. These are static checks,
not execution of the examples. No runtime code was changed.
