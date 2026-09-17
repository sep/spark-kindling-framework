# AWS Glue as a Kindling Execution Target

**Status:** Proposed (feasibility evaluation). No extension code exists; nothing
in this document is implemented.
**Created:** 2026-09-17
**Baseline:** `main` @ `c917ac3` (release 0.12.47). Every claim about Kindling
below cites a `file:line` in that tree. Glue facts were checked against the AWS
documentation on 2026-09-17 (sources at the end).
**Related:** `dataproc_platform_evaluation.md` (prior fourth-platform evaluation,
architecturally stale), `databricks_bundle_deployment.md` (proposed, not
implemented), `declarative_pipelines_engine.md`, `kindling_core_runner_split.md`,
`iceberg_entity_provider.md`, `great_expectations_validation.md`,
`contributing/platform_api_architecture.md`.

## Motivation

Kindling targets Synapse, Fabric and Databricks. A recurring client pattern on
AWS is Airflow (MWAA) orchestrating Glue PySpark jobs. Supporting Glue as a
fourth target would let Kindling (a) enforce project structure and keep
transformation code portable on AWS, and (b) provide a credible later migration
path to Databricks with minimal rewrite. This document evaluates whether that is
feasible, what it would take, and where the portability claim is weaker than it
sounds.

## 1. Recommendation

**Feasible with caveats.** Glue 5.1 is a credible fourth *runner* target: the
platform seams Kindling already has (`PlatformService`, `PlatformAPI`,
`ArtifactStore`, `provider_type` registry, engine extensions) cover most of the
surface, the devcontainer already pins pyspark 3.5.5 + delta-spark 3.3.2
(`.devcontainer/Dockerfile:37-38`), which is the Glue 5.1 combination, and the
standalone service already routes `s3://`/`s3a://` through Hadoop FS
(`packages/kindling/platform_standalone.py:299-300`). The caveats are real but
bounded: four core assumptions must change (platform-detection whitelist,
notebook-utility-only config transport, no streaming trigger control,
Databricks-default feature flags), Glue Data Catalog's two-level naming collides
with Kindling's three-part name resolution, and Glue job bookmarks are a poor
substitute for Kindling's own portable watermark mechanism and should be
confined to raw-file/JDBC ingestion adapters. The "write once on Glue, retarget
Databricks" claim holds for declaration code and vanilla PySpark transformations
and for Kindling-owned incremental state when Delta table history is preserved;
it does **not** hold for bookmark state, any Glue Data Quality rules, catalog
identity, or orchestration, and retargeting onto Lakeflow (rather than Databricks
runner Jobs) inherits every runner-to-LDP divergence Kindling already documents.
Glue 6.0 SDP is the true Lakeflow analog and the better long-term migration
story, but it is four weeks old, has no local Docker image, lacks expectations
and AUTO CDC, and requires Spark 4.x support core does not declare; treat it as
a second, later track.

## 2. Where Glue fits in Kindling's architecture

### 2.1 Kindling has two execution paradigms and two platform layers

Kindling is one package with two execution paradigms
(`docs/proposals/kindling_core_runner_split.md:12-57`): an **imperative runner**
(generation executor, watermark aspect, SCD merge strategies, streaming stack)
that is ambient in core and runs when `initialize()` is called with no
`engine=`, and **declarative engine extensions** that translate the same
declarations into a platform's pipeline vocabulary.

Every target also has two platform layers (`docs/contributing/platform_api_architecture.md:5-16`):
a runtime `PlatformService` that runs on the cluster and a design-time
`PlatformAPI` in `kindling_sdk` that deploys and submits from a dev machine or
CI. Submission transport differs per platform and is not part of the runtime:
Synapse uses Livy batches (`packages/kindling_sdk/kindling_sdk/platform_synapse.py:464-498`),
Fabric creates ephemeral SparkJobDefinitions (`platform_fabric.py:326-363`),
Databricks uses the Jobs API (`platform_databricks.py:623-709`). All three
implement the same job-shaped contract
(`packages/kindling_sdk/kindling_sdk/platform_provider.py:80-213`).

Databricks is served by **both** paradigms, through unrelated code paths:

- The runner, submitted as a Databricks Job whose Python entry point is the
  bootstrap shim (`runtime/scripts/kindling_bootstrap.py`).
- The Lakeflow lowering: `kindling_ext_databricks` is a declaration-time adapter
  that emits *in-process* `dp.*` calls inside a Lakeflow pipeline's source
  evaluation (`packages/extensions/kindling_ext_databricks/kindling_ext_databricks/engine.py:190-412`).
  Its only dependencies are `spark-kindling` and `spark-kindling-ext-sdp`
  (`packages/extensions/kindling_ext_databricks/pyproject.toml:17-23`); it makes
  no API calls and writes no files. Pipeline objects are provisioned by
  Terraform (`iac/databricks/workspace/pipelines.tf:5-49`) or hand-written YAML.
  Bundle emission is a proposal marked "Proposed; commands below are not
  implemented" (`docs/proposals/databricks_bundle_deployment.md:3`); no
  `kindling bundle` CLI group exists (`packages/kindling_cli/kindling_cli/cli.py`
  groups at `:1381-8423`).

### 2.2 What a Glue target is, structurally

Glue 5.x has no declarative pipeline engine, so there is nothing to lower
Kindling's vocabulary onto. Kindling's runner runs *inside* the Glue job
unchanged, exactly as it does inside a Databricks Job or a Synapse batch. What a
Glue target adds is:

1. a fourth `PlatformService` + `PlatformAPI` pair (the shape
   `dataproc_platform_evaluation.md` sketched for GCP), and
2. a design-time **orchestration emitter** producing Glue job definitions,
   Airflow DAGs and an S3 layout. Because DAB emission is unimplemented, this
   would be the first orchestration emitter in the repo, and should be designed
   to be shared with the bundle proposal.

Glue 6.0 changes the picture: it ships OSS Spark Declarative Pipelines (SDP),
the API `kindling_ext_sdp` already emits. That makes a genuine declarative
lowering possible on Glue, as a second track (§10, Track B).

### 2.3 Glue version matrix against Kindling's floors

Core declares `pyspark >=3.4.0,<4.0.0` and `delta-spark >=2.4.0,<4.0.0`
(`pyproject.toml:74-75`), Python `^3.10` (`:18`).

| Glue | Spark | Python | Java | Delta | Iceberg | Fit |
|---|---|---|---|---|---|---|
| 4.0 | 3.3.0 | 3.10 | 8 | 2.1.0 | 1.0.0 | **Not viable**: below both floors; `current_catalog()` absent; Delta lacks `withSchemaEvolution` and `whenNotMatchedBySourceUpdate` |
| 5.0 | 3.5.4 | 3.11 | 17 | 3.3.0 | 1.7.1 | Inside declared ranges |
| 5.1 (default for new jobs) | 3.5.6 | 3.11 | 17 | 3.3.2 | 1.10.0 | **Runner target.** Equals the devcontainer pins apart from Java 21 vs 17 |
| 6.0 (GA 2026-08-21) | 4.1.1 | 3.13 | 17 | 4.2.0 | 1.11.0 | **Declarative target, later.** Exceeds `pyspark <4.0.0`; SDP, Spark Connect for interactive sessions, S3A only, ANSI on by default; no Docker image yet |

## 3. The extension contract a target implements

**Runtime `PlatformService`** (`packages/kindling/notebook_framework.py:630-766`).
It is a plain class, not an ABC, so the `@abstractmethod` markers are inert and
`StandaloneService` legally omits the job-control and notebook methods. Methods
core actually consumes:

| Concern | Methods | Consumed at |
|---|---|---|
| Identity | `get_platform_name` | overlay selection `bootstrap.py:667-669`, app-file filtering |
| Secrets | `get_secret`, `secret_exists`, `list_secrets` | `PlatformServiceSecretProvider` `platform_provider.py:138-177`; `@secret` loader `config_loaders.py:26-194` |
| Storage | `exists`, `copy`, `read`, `write`, `move`, `delete`, `list` | app code load `data_apps.py:1350-1361`; file ingestion listing `file_ingestion.py:414`; app discovery `data_apps.py:448` |
| Session | `get_spark_session`, `get_config`, `set_config` | `spark_session.py:130-131` prefers `__main__.spark` |
| Introspection | `is_interactive_session`, `get_workspace_info`, `get_cluster_info` | bootstrap/logging |
| Notebooks | `list_notebooks`, `get_notebook`, ... | `NotebookLoader`, eagerly DI-bound (`notebook_framework.py:1074-1083`, `data_apps.py:412-414`); must return `[]`, not raise |
| Job control (in-cluster) | `deploy_spark_job`, `run_spark_job`, `get_job_status`, `cancel_job` | `DataAppDeployer` `job_deployment.py:175-545`, which calls SDK-only methods and is effectively non-functional legacy; do not implement to it |

Registration: `@PlatformServices.register(name=...)` on a
`create_platform_service(config, logger)` factory (`notebook_framework.py:1912-1974`),
discovered through the `spark_kindling.platforms` entry-point group
(`bootstrap.py:1665-1738`, declared `pyproject.toml:103-109`). Third-party
platforms **must** ship the entry point because `kindling` is not a namespace
package (`bootstrap.py:1689-1692`). The unit contract
`tests/unit/test_platform_entry_points.py:20` pins the expected set.

**Design-time `PlatformAPI`** (`packages/kindling_sdk/kindling_sdk/platform_provider.py:80-213`):
`deploy_app`, `cleanup_app`, `create_job`, `run_job`, `get_job_status`,
`cancel_job`, `delete_job`, `get_job_logs`, `stream_stdout_logs`, `set_secret`,
`delete_secret`, `find_job_by_name`, `submit_app_run`, `register_app_job`, plus a
`from_env()` classmethod. Registration is `@PlatformAPIRegistry.register("<name>")`
but discovery is a hard-coded `importlib.import_module(f"kindling_sdk.platform_{name}")`
(`:235-240`) and `list_platforms()` hard-codes three names (`:249`). Artifact
transport is `ArtifactStore` (`kindling_sdk/artifact_store.py:193-227`) with
`artifact_store_for()` dispatching on `abfss://`, `/Volumes/`, `file://` and
rejecting any other scheme (`:466-496`).

**Engine extension** (`packages/kindling/__init__.py:119-152`): duck-typed. A
module `kindling_ext_<name>` exposes `engine_extension()` returning an object with
`activate()`, optional `owns_incrementality: bool` (suppresses the watermark
aspect, `bootstrap.py:2313-2323`) and optional `declare_pipeline(pipe_ids=None)`.
Engine extensions do **not** implement `ExecutionStrategy`/`ExecutionOrchestrator`;
those are runner internals with zero references under `packages/extensions/`.

**Other registration seams:** `register_provider(provider_type, cls)`
(`entity_provider_registry.py:79`); `AutoLoaderFileIngestionRunner` ABC
(`file_ingestion.py:170-195`); `EntityNameMapper`/`EntityPathLocator` DI bindings
(`data_entities.py:134,140`); `WatermarkEntityFinder` (`watermarking.py:37`);
telemetry provider rebind at import time (`kindling_ext_otel_azure/__init__.py:14-50`).

## 4. The Lakeflow lowering as precedent

Two modules do the translation: `kindling_ext_sdp/declaration_engine.py`
(registries → validated, Spark-free `DeclarationPlan`) and
`kindling_ext_sdp/oss_engine.py` (plan → `dp.*` calls, `pyspark.pipelines`
injected lazily at `:40-51`). `kindling_ext_databricks/engine.py:57` subclasses the
OSS engine and adds the Databricks-only features gated by `capabilities.py:61-71`.

| Kindling construct | LDP construct | Where |
|---|---|---|
| `@DataPipes.pipe` → table entity | `dp.materialized_view(name, comment, table_properties, partition_cols, cluster_by, schema)` | `oss_engine.py:183-201` |
| Input produced in-pipeline | `spark.table(<local dataset>)` (graph edge inferred) | `oss_engine.py:237,248`; classification `declaration_engine.py:298-333` |
| Input produced elsewhere | `spark.table(EntityNameMapper.get_table_name(...))` | `oss_engine.py:148-170` |
| Entity id `silver.orders` | single-part dataset `silver_orders` (or `orders` with `dataset_naming: leaf`) | `declaration_plan.py:19-52` |
| `engine.databricks_sdp.expectations{,_drop,_fail}` | `@dp.expect_all{,_or_drop,_or_fail}` | `engine.py:50-54,397-422` |
| Provider-owned stream / `streaming_inputs` | `dp.create_streaming_table` + `dp.append_flow` | `engine.py:218-245` |
| SCD2 (`scd.*` tags + `merge_columns`) | temp view + streaming table + `dp.create_auto_cdc_flow` / `create_auto_cdc_from_snapshot_flow` | `engine.py:321-395`, `auto_cdc.py:56-120` |
| `use_watermark` | **dropped**: `owns_incrementality=True`, aspect never registered | `engine_extension.py:21`; `bootstrap.py:2313-2323` |
| Provider writes | **refused** by `SdpWriteGuardProvider` | `guard_provider.py:26-92` |
| Temporal chain events/episodes | stratified streaming tables / MVs + snapshot AUTO CDC | `temporal_lowering.py:420-618` |

Entry point in the pipeline source: `declare_from_pipeline_config()`
(`lakeflow_app_selector.py:290-361`) → `kindling.initialize(engine="databricks_sdp",
declaration_only=True)` → `register_all()` → `kindling.declare_pipeline()`. Config
reaches the pipeline via `spark.kindling.bootstrap.config_files` point lookups
(`lakeflow_app_selector.py:89-137`). This precedent is the template for a Glue
6.0 SDP engine (§10, Track B); it has no counterpart on Glue 5.x.

## 5. Contracts that carry semantics vs plumbing

| Area | Semantics a target must reproduce | Plumbing that can be swapped |
|---|---|---|
| Entity vocabulary | `EntityMetadata` (`data_entities.py:224-243`) plus tag conventions: `provider_type` (default `delta`, `entity_provider_registry.py:175`), `provider.*`-only option extraction (`entity_provider.py:110-124`), `scd.*`, `write.mode`, `schema.drift`, `read_only`, `dataset.kind: derived` | provider implementations |
| Merge | SCD1 update-all/insert-all with additive schema evolution (`entity_provider_delta.py:95-141`), insert-only replay safety (`:145`), full SCD2 staged-update contract (`:223-441`: sequence ordering, sentinel routing key, `delete_when`, `close_on_missing` via `whenNotMatchedBySourceUpdate` `:423`) | `DeltaMergeBuilder` API |
| Incremental read | opaque provider-owned cursor covering exactly the returned frame (`entity_provider.py:399-435`); at-least-once tolerance; cursor scoped `(source_entity, pipe_id)` in `system.watermarks`, a Delta entity (`watermarking.py:72-77`); persist-then-advance via `WatermarkAspect` (`:478-673`); skip only when all driving reads are empty (`data_pipes.py:770-777`); reset = delete the row | Delta CDF as the change source (`entity_provider_delta.py:2085-2126`), blinker signals |
| Streaming | checkpoint identity `<checkpoint_root>/<pipeid>` (`pipe_streaming.py:218-223`); sinks-first ordering (`execution_strategy.py:441`); driving=stream, non-driving=static; micro-batch merge = batch merge | orchestrator/recovery/health supervision (`streaming_*.py`) |
| File ingestion | regex named groups → destination template → enrichment; run-now-drain-stop contract (`file_ingestion.py:181-195`); entry-scoped state paths (`:602-603`) | discovery engine (`AutoLoaderFileIngestionRunner` seam) |
| Data quality | **none in core vocabulary**; only the `persist.before_persist` gate (`simple_read_persist_strategy.py:264-279`); expectations are Databricks-SDP passthrough only (`engine.py:404-410`); GX runner is a proposal (#245) | everything |
| Table format | behaviour of merge/replace/CDF; `system.watermarks` hard-tagged Delta (`watermarking.py:77`); `DeltaTable` imported at module scope in `data_entities.py:9`, `data_pipes.py:10`, `file_ingestion.py:13`, `watermarking.py:9` (unused in those four), `simple_stage_processor.py:6`; **zero** Iceberg/Hudi code | `DeltaTableReference`, strategy registry |
| Catalog identity | precedence chain in `entity_resolution.py:221-309`: `provider.table_name` > `leaf` strategy > no-namespace-config means the entity id *is* the qualified name (3 parts kept, 2 parts promoted via `current_catalog()` unless `spark_catalog`, `:233-246`) > configured catalog/schema flatten | UC detection heuristics (`features.py:73-98`) |
| Migration | additive DDL in place; destructive = blue/green with `ALTER TABLE RENAME TO` (`migration.py:755-807`) in catalog mode, in-place overwrite in storage mode; typed to `DeltaEntityProvider` (`:563`) | SQL text |

## 6. Batch-job porting hazards in core

Core was designed around long-lived notebook sessions on managed clusters. The
assumptions below were verified with a throwaway script against this tree:
`detect_platform({"platform": "glue"})` raises `RuntimeError: Unable to detect
platform`, `_get_storage_utils()` returns `None` outside a notebook, and
`download_config_files(...)` then raises `Storage utilities not available for
config loading`. Detection also boots a Spark session as a side effect
(`bootstrap.py:1050`).

| # | Assumption | Location | Hazard on Glue |
|---|---|---|---|
| H1 | Platform whitelist `["databricks","fabric","synapse","standalone"]`; unknown ⇒ `RuntimeError` | `bootstrap.py:1040`, `:1084` | Hard failure; `platform: glue` silently ignored then rejected |
| H2 | Config and extension-wheel download require `dbutils`/`mssparkutils` | `bootstrap.py:620-623`, `:728-756`, `:1501-1528`; shim `runtime/scripts/kindling_bootstrap.py:189-262` | No artifacts-storage config discovery on Glue; only explicit `config_files` works, with a warning (`:2097-2104`) |
| H3 | Spark session via `__main__.spark` or `SparkSession.builder.getOrCreate()`; `spark_configs` pushed with `spark.conf.set` on a live session | `spark_session.py:130-131`, `bootstrap.py:1789` | `GlueContext` owns the session; most `spark.sql.catalog.*`/extension keys are immutable after start and will be "Could not apply" warnings. Must move to `--conf` at submit |
| H4 | Feature discovery runs live `EXPLAIN ALTER TABLE ... CLUSTER BY`, `SHOW CATALOGS`, `dbutils.fs.ls("/Volumes")`; with discovery off, defaults assume a modern Databricks runtime | `features.py:121-224`, `MODERN_RUNTIME_DEFAULTS` `:107-118` | Misleading probes; must set `kindling.features.discovery: false` **and** override every `databricks.*`/`delta.*` flag statically |
| H5 | Streaming queries get no trigger unless the caller passes `options["trigger"]`; `StreamingOrchestrator.run()` blocks on `await_termination` | `entity_provider_delta.py:1992-1994`, `append_as_stream` `:1803`, `streaming_orchestrator.py:179-194` | A Glue *batch* job running a streaming plan never terminates (billed until timeout). No config key exists for `availableNow` |
| H6 | `platform != "standalone"` turns on watermarking and bootstrap dependency installs by default | `bootstrap.py:2276`, `:2317` | A `glue` platform inherits managed-platform defaults: `system.watermarks` must be writable on first run; pip-at-init should be disabled in favour of Glue job parameters |
| H7 | pip installs at framework init, driver only, default index, no `--index-url` plumbing anywhere | `bootstrap.py:1328-1363`, `data_apps.py:1073-1097`, `pip_manager.py:95-137` (only `find_links`) | Cold-start cost per run; executors never see these packages; private PyPI impossible without new plumbing |
| H8 | Concurrent same-pipe execution unsupported; one provider decorator per process | `watermarking.py:513-524`, `entity_provider_registry.py:65` | Glue `MaxConcurrentRuns` must stay 1 per Kindling app job |
| H9 | Notebook manager eagerly constructed; `load_workspace_packages` compiles notebook cells | `notebook_framework.py:1074-1083`, `bootstrap.py:1741-1763` | Harmless if `list_notebooks()` returns `[]` and the flag stays false (default `:2388`) |
| H10 | Log4j MDC and Fabric `ComponentSparkEvent` via `_jvm`, guarded | `spark_log.py:20`, `spark_trace.py:86-134` | Degrade to stdlib logging/print; stdlib logging reaches CloudWatch anyway |
| H11 | Databricks-specific temp/staging path logic | `bootstrap.py:443-586` | Dead weight; `tempfile.mkdtemp()` branch is fine |
| H12 | In-cluster `DataAppDeployer` calls SDK-only methods | `job_deployment.py:241-242`, `:314`, `:372` | Non-functional legacy; a Glue service should not implement it |

Notably **absent** hazards: the JVM boundary test (`tests/unit/test_architecture_jvm_boundary.py:16-30`)
already forbids `_jvm` outside an allowlist, so core is safe on Glue 5.x Lake
Formation FGAC mode and on Spark Connect alike; the `.kda` format is text-only
and storage-agnostic (`app_files.py:6-60`); the `@secret` two-pass loader is
backend-agnostic (`config_loaders.py:26-254`, `bootstrap.py:1823-1919`).

## 7. Kindling concepts mapped onto Glue

Fidelity: **clean** (same semantics), **lossy** (works with documented
differences), **absent** (no Glue equivalent or no Kindling equivalent).

| Kindling concern | Glue-native equivalent | Fidelity | Implications |
|---|---|---|---|
| Job submission | `CreateJob`/`UpdateJob` (`glueetl`, `GlueVersion 5.1`, `WorkerType`/`NumberOfWorkers`, `DefaultArguments`, `ExecutionProperty.MaxConcurrentRuns`) + `StartJobRun(Arguments)`; `GetJobRun`; `BatchStopJobRun` | clean | Maps onto `PlatformAPI`: `register_app_job` → `CreateJob`, `submit_app_run` → `StartJobRun` (Glue collapses create+run like Dataproc did, `dataproc_platform_evaluation.md:63-82`); `get_job_logs`/`stream_stdout_logs` → CloudWatch `GetLogEvents` on `/aws-glue/jobs/output`. Script args stay `config:k=v` (`platform_databricks.py:548-556`) plus `--conf spark.kindling.*`. Run-level `Arguments` override `DefaultArguments`; `NonOverridableArguments` win. 260 KB argument cap |
| Deployment / packaging | S3 artifacts prefix; `--additional-python-modules` (S3 wheels, `*.gluewheels.zip` + `--python-modules-installer-option --no-index`, `-r requirements.txt`, or `--index-url` for private PyPI); `--python-virtual-env` (Glue ≥5.0, replaces the env entirely); `--extra-py-files` (driver path); `--extra-files` (copied to driver cwd: the natural transport for config YAML) | lossy | Kindling's runtime pip-at-init model (H7) is redundant and should be disabled; framework + extensions + `lake-reqs.txt` wheels become a zip-of-wheels built in CI, which is stricter and more deterministic than today. Private PyPI is a Glue job parameter, and also the first place Kindling would need `--index-url` support it lacks. Pre-installed Glue 5.x modules (boto3 1.34, pandas 2.2.2, pyarrow 17; no PyYAML on 5.0/5.1) must be respected; a `[glue]` extra with no Spark deps matches the existing packaging rationale (`pyproject.toml:67-71`) |
| Orchestration | Generated Airflow DAG using `GlueJobOperator` (creates/updates job, uploads script, returns run id; `update_config`, `wait_for_completion`/`deferrable`, `verbose`, `stop_job_run_on_kill`) + `GlueJobSensor`. Glue Workflows (triggers/jobs/crawlers, EventBridge start) as a lesser alternative | lossy | Recommended granularity: **one Glue job per Kindling app**; Kindling's generation executor runs the pipe DAG in-process, Airflow provides schedule, retries, sensors and cross-app edges. Per-pipe Glue jobs would pay a cold start per pipe, lose `CacheOptimizer` sharing and multiply `system.watermarks` writers; keep as an opt-in for large apps. Glue Workflows lack parameters beyond run properties, cross-workflow dependencies and first-class retry policy; not recommended as primary |
| Catalog | Glue Data Catalog as Hive metastore (`--enable-glue-datacatalog`), two-level `database.table`; three-part only via a configured Iceberg `spark.sql.catalog.glue_catalog`; Lake Formation for permissions | lossy | `ConfigDrivenEntityNameMapper` produces three-part names by default (`entity_resolution.py:233-246`, `:277`). Glue target must set `kindling.storage.table_schema` (flatten to `schema.leaf`) and leave `table_catalog` unset, or bind a `GlueEntityNameMapper`. `current_catalog()` returns `spark_catalog` on Glue, which Kindling already treats as "absent" (`:239-245`). Delta tables register via `saveAsTable`/`CREATE TABLE ... USING DELTA LOCATION` (`entity_provider_delta.py:1396-1400`); Glue crawlers are not needed. `spark.catalog.tableExists` becomes an AWS API call; permission errors read as "missing" (`:1286-1289`) |
| Incremental reads | Glue job bookmarks: DynamicFrame-only (`create_dynamic_frame.from_catalog/from_options` with `transformation_ctx`), sources S3 (JSON/CSV/Avro/XML/Parquet/ORC by object mtime), JDBC (monotonic keys), Relationalize; state keyed by job name + `transformation_ctx`, opaque, committed atomically at `job.commit()`, deleted with the job; reset is job-wide; rewind via `job-bookmark-pause` + `from`/`to` run ids; **not** for Delta/Iceberg sources, not in streaming jobs, not in the local Docker image | lossy (see §7.1) | Kindling's own mechanism (per-pipe cursor in `system.watermarks`, Delta CDF, persist-then-advance) works on Glue unchanged and is portable. Bookmarks add value only for raw S3 file and JDBC *sources* Kindling reads via `spark.read` today |
| Data quality | `awsgluedq.transforms.EvaluateDataQuality.apply(frame=DynamicFrame, ruleset=DQDL, publishing_options)` → results DynamicFrame + CloudWatch metrics; standalone rulesets on catalog tables (Airflow `GlueDataQualityRuleSetEvaluationRunOperator`); not in the local image | absent on both sides today | Core has no DQ vocabulary; Lakeflow expectations are engine-scoped config. A `engine.glue.*` block lowering to DQDL is the symmetric design, but the portable form is the proposed validation runner (#245) firing at `persist.before_persist` |
| Table formats | `--datalake-formats delta,iceberg,hudi`; Delta 3.3.x with `DeltaSparkSessionExtension` + `DeltaCatalog` + `S3SingleDriverLogStore` via `--conf`; `spark.read.format("delta")`, `DeltaTable.forPath`, `saveAsTable` all supported; Iceberg via `GlueCatalog` | clean for Delta on 5.x | Delta 3.3 has `withSchemaEvolution` and `whenNotMatchedBySourceUpdate`, so SCD1/SCD2 paths match the devcontainer. `S3SingleDriverLogStore` means one writer per Delta table (multi-job writes need `S3DynamoDBLogStore`), reinforcing `MaxConcurrentRuns = 1`. Liquid clustering / `CLUSTER BY` unsupported: already self-disabling (`entity_provider_delta.py:816-826`). Iceberg would be a new provider **and** a core change (watermark store tag, cursor encoding, `replaceWhere`, migration typing) |
| Spark version matrix | see §2.3 | 5.x clean; 4.0 absent; 6.0 lossy | Only `kindling_ext_sdp` is exercised on Spark 4.1 today, in an isolated venv (`scripts/ensure_sdp_runtime.py`) with **no kindling-core import** (`tests/integration/test_sdp_dry_run_real.py:14-17`) |
| Dev loop | Official image `public.ecr.aws/glue/aws-glue-libs:5` (Spark 3.5.4, Delta/Iceberg/Hudi preloaded, x86_64+arm64; no bookmarks, no DQ, no LF vending locally); Interactive Sessions (same runtime, `%additional_python_modules` etc., Spark Connect in 6.0); no 6.0 image yet | clean | Today's local story is `platform: standalone` + local Spark/Delta via `kindling app run` (`cli.py:5274-5388`); nothing Glue-specific is needed for transformation development. The Glue image becomes the *pre-deploy* check (catalog + S3 + pip parity), analogous to how Databricks system tests are used. Standalone needs `hadoop-aws` jars and AWS credential wiring analogous to `_configure_abfss_local_auth` (`platform_standalone.py:15-73`) |
| Secrets / config | Secrets Manager `GetSecretValue`, SSM `GetParameter` via boto3 under the job IAM role; config via `--extra-files` (driver cwd) or S3 reads; env vars only through `--customer-driver-env-vars` with a mandatory `CUSTOMER_` prefix | clean | `GlueService.get_secret` is the only seam needed (`platform_provider.py:138-177`); add `kindling.secrets.aws.*` keys beside `secret_scope`/`key_vault_url`. `KINDLING_*` Dynaconf env overrides cannot be set directly on Glue (prefix restriction), so overrides go through `spark.kindling.*` `--conf` or `config:k=v` args |
| Logging / metrics | CloudWatch Logs (`--enable-continuous-cloudwatch-log`), Glue job metrics, observability metrics, Spark UI event logs to S3 | clean for logs, absent for metrics | Plain-python telemetry providers (`plain_telemetry.py`) bind when the JVM bridge probe says so (`bootstrap.py:2178-2184`); stdlib logging lands in CloudWatch. Core has no metrics abstraction (`unified_otlp_telemetry_provider.md:137-150`) |

### 7.1 Job bookmarks vs Kindling watermarks vs Lakeflow

| Dimension | Kindling watermark (`watermarking.py`) | Glue job bookmark | Lakeflow / Auto Loader |
|---|---|---|---|
| State owner | Kindling, in `system.watermarks` (Delta, user-visible, mergeable) | Glue service DB, opaque | Pipeline checkpoints / `cloudFiles.schemaLocation`, platform-owned |
| Scope | `(source_entity_id, pipe_id)` row | `(job name, transformation_ctx)` | per streaming table / flow |
| Advance point | after each pipe's durable persist (`:644-673`) | once, at `job.commit()` for all contexts | per micro-batch commit |
| Failure behaviour | discard capture, re-read same slice next run (at-least-once, idempotent merge) | no commit ⇒ whole job replays; partial writes before failure are re-applied | checkpoint replay |
| Reset | delete one row (or `no_watermark` for a one-off) | `ResetJobBookmark` resets **all** contexts in the job; targets untouched | full refresh per dataset |
| Rewind | none built in | `job-bookmark-pause` + `from`/`to` run ids | none |
| Sources | any `IncrementalReadableEntityProvider` (Delta CDF today) | S3 objects by mtime, JDBC monotonic keys | Delta streaming source, cloud files |
| Transfer between platforms | yes if Delta history preserved (cursor = table version) | no; deleted with the job | no |
| Concurrency | unsupported same-pipe concurrency | `MaxConcurrentRuns` | pipeline-level |

Consequence: bookmarks should never replace the watermark aspect for
Delta-to-Delta pipes. They are worth wrapping only as a *source adapter* for raw
S3 landing zones and JDBC extracts, where today Kindling has no incremental
source at all (`file_ingestion.py` lists the whole directory per run in `batch`
discovery, `:414`, and `spark_jdbc.py` is a full read).

## 8. Boundary design: DynamicFrame confined to the edges

**Proposed boundary.** DynamicFrame is used only inside extension-owned
*source* adapters (to obtain job bookmarks and catalog integration), converted
with an immediate `.toDF()`; all user transformation code is vanilla PySpark
DataFrame API; `GlueContext` and `awsglue` imports are forbidden in user code
and enforced by lint. Three refinements:

1. **Sinks do not need DynamicFrame at all.** Kindling writes through the Delta
   provider's DataFrame API (`saveAsTable`, `DeltaTable.merge`), which registers
   tables in Glue Data Catalog directly. `write_dynamic_frame`/`write_data_frame.from_catalog`
   adds nothing except bookmark "sink" contexts Kindling does not need.
2. **Bookmark sources are a narrow adapter.** Two adapters cover the useful
   cases: (a) a `provider_type: "glue-bookmark"` read-only provider implementing
   `BaseEntityProvider` + `IncrementalReadableEntityProvider` whose
   `read_entity_changes` calls
   `create_dynamic_frame.from_catalog/from_options(transformation_ctx=<entity id>)`
   and returns `(dyf.toDF(), "<glue-managed>")`, and (b) a `FileIngestionProcessor`
   variant (or a `discovery="glue-bookmark"` mode) for landing zones. The opaque
   cursor contract explicitly permits provider-owned state
   (`entity_provider.py:404-412`), but the *at-least-once* clause is only honoured
   if the extension-owned entry script calls `job.init` before the runner and
   `job.commit` after the whole app succeeds. Because commit is job-wide, a
   failure in pipe B replays pipe A's bookmarked input; that is safe under
   Kindling's idempotent-merge rule but coarser than the per-pipe cursor, and
   must be documented.
3. **The lint rule must be broader than `awsglue`.** Portability also requires:
   no `spark.conf.set` for session-immutable keys (H3), no `dbutils`/`mssparkutils`
   /`notebookutils`, no `spark._jvm` (already enforced for core by
   `test_architecture_jvm_boundary.py`, and mandatory anyway under Glue 5.x Lake
   Formation FGAC, which blocks RDDs, custom classes and extra jars), and no
   `boto3` in transformation code (secrets go through `@secret`). This is
   enforceable: `kindling app validate` already AST-inspects apps without a
   SparkSession (`cli.py:1343-1380`), and scaffolded packages get their own
   tests (`docs/guide/local_python_first.md`); add a banned-imports test to the
   scaffold and a `--strict-portability` check to `app validate`.

**Do the existing reader/writer abstractions absorb this?** Yes for sources
and sinks: the `provider_type` registry plus the split capability interfaces are
exactly the seam, and `EntityNameMapper`/`EntityPathLocator` DI bindings absorb
catalog naming. **Two new extension points are needed:** (a) a streaming trigger
policy that the runner honours (`pipe_streaming.py`, `entity_provider_delta.py:1992`)
so a Glue batch job can drain-and-stop, and (b) a job-lifecycle hook so the
extension can run `job.init`/`job.commit` around the app without users writing
Glue code (the `app_run` phase at `bootstrap.py:2448-2458` is the natural place;
today the `.kda` entry point is `exec`'d source text, `data_apps.py:1350-1403`).

## 9. Migration path to Databricks: where the claim weakens

The two Databricks destinations differ and are listed separately.

| # | Weakness | Retarget to Databricks **runner** Jobs | Retarget to **Lakeflow** SDP |
|---|---|---|---|
| M1 | Kindling watermark cursors are Delta table versions. Copying tables (deep clone, `COPY INTO`, Athena CTAS) resets version history; cursors become invalid. Recovery is "delete the watermark rows ⇒ initial full load + `remove_duplicates`" (`entity_provider_delta.py:2101-2121`): correct, but a full reload of every source | Holds only if tables move with their `_delta_log` intact (same S3/ADLS objects, or external tables pointing at the original bucket) | Irrelevant: `owns_incrementality=True` drops watermarks; every streaming table starts from a full refresh |
| M2 | Bookmark state (if any adapter uses it) is Glue-owned and deleted with the job | Lost; the source adapter has no Databricks counterpart, so JDBC/S3 landing-zone pipes need re-declaration onto Auto Loader (`kindling_ext_databricks_autoloader`) or Kindling batch discovery | Lost; same |
| M3 | Auto Loader vs bookmarks: different discovery semantics (file notification / directory listing with `schemaLocation` vs object mtime) and different schema-evolution behaviour (`autoloader_file_ingestion.py:43-44`) | Reprocess or manual cutover marker required | Same |
| M4 | Expectations: Glue has none in Kindling today; if Glue Data Quality DQDL is adopted, it does not lower to `dp.expect*` and vice versa. Only a core validation contract (#245) would be portable | Runner has no expectations either, so DQDL rules are simply lost | `engine.databricks_sdp.expectations` must be authored fresh |
| M5 | Catalog identity: Glue two-level `db.table` (Kindling configured to flatten) vs UC three-level. Table names, `provider.table_name` overrides, and every downstream consumer (Athena/Redshift views vs UC grants) change | Config overlay change (`settings.databricks.yaml`), plus consumer re-pointing | Same, plus `DatasetNameMapper` single-part naming rules (`declaration_plan.py:19-52`) |
| M6 | Orchestration: Airflow DAG (external scheduler, per-run job, Airflow retries/SLAs, sensors) vs Databricks Jobs (workflow tasks) vs Lakeflow (engine-managed graph, triggered/continuous). Airflow-side logic (branching, backfills, cross-DAG sensors) is not Kindling vocabulary and does not migrate | DAG must be rewritten as Jobs workflow or kept in Airflow with `DatabricksRunNowOperator` | Pipeline update replaces the DAG entirely; MVs recompute fully (`declarative_pipelines_engine.md`), so cost profile changes |
| M7 | Runner-to-LDP semantic divergences Kindling already documents: SCD2 columns `__START_AT`/`__END_AT` instead of Kindling effective columns, schema withheld from streaming tables, no CDF forced on outputs (`auto_cdc.py:12-30`, `engine.py:243,364`, `declaration_plan.py:139-143`), watermark-driven incremental MVs become full recompute unless `streaming_inputs` is declared | Not applicable | Every SCD2 consumer and every `use_watermark` pipe changes behaviour |
| M8 | Physical layout: `cluster_columns` are no-ops on Glue (self-disabled) and activate on Databricks; `partition_columns` are skipped when clustering is preferred (`entity_provider_delta.py:731-740`) | Benign but surprising: tables get re-laid-out on first Databricks write | Same |
| M9 | Session-immutable Spark config moves from Glue `--conf` back to runtime `spark_configs`; secrets move from Secrets Manager ARNs to secret scopes; storage roots from `s3://` to `abfss://`/Volumes | Pure config-overlay work if the app kept everything in `settings.*.yaml`; a real rewrite if users hard-coded paths | Same |
| M10 | Glue 5.x is classic py4j; Databricks UC shared clusters are Spark Connect. Anything in user code that slipped past the portability lint (RDDs, `_jvm`, `toPandas` on huge frames) breaks only after migration (`docs/contributing/databricks_execution_contract.md`) | Real risk; mitigated by the lint rule in §8 and by running system tests on a Shared-mode cluster | Same |
| M11 | Version skew: Glue 5.1 Delta 3.3.2 vs DBR Delta; Python 3.11 on both today, but Glue 6.0 moves to 3.13 and ANSI-on-by-default | Low | Low |

Summary suitable for a client conversation: **declaration code and DataFrame
transformations migrate; Kindling-owned incremental state migrates when table
history moves intact; platform-owned state, data-quality rules, catalog names
and orchestration do not.** The strong version of the claim is "retarget to
Databricks runner Jobs with the same Kindling app"; the Lakeflow version adds
the runner-to-LDP divergences Kindling already lists, and should be presented as
a re-declaration exercise with tooling support, not a no-op.

## 10. Extension architecture

Two tracks. Track A is the recommendation for the Airflow-plus-Glue use case;
Track B is the true declarative analog and should follow once Glue 6.0 matures.

### Track A: `spark-kindling-ext-glue` (runner on Glue 5.1, Airflow-orchestrated)

```text
packages/kindling/platform_glue.py                 # runtime PlatformService (in-tree, like the other four)
  GlueService(PlatformService)
    get_platform_name -> "glue"
    get_secret / secret_exists / list_secrets       # boto3 Secrets Manager + SSM; kindling.secrets.aws.*
    exists/copy/read/write/move/delete/list         # boto3 S3 (driver-side), s3:// and s3a://
    get_spark_session                               # GlueContext.spark_session if present, else getOrCreate
    is_interactive_session -> False; notebook CRUD -> []; job control omitted
  _bind_default_entity_services                     # kindling.delta.access_mode default "catalog"
  @PlatformServices.register(name="glue")           # + spark_kindling.platforms entry point + [glue] extra

packages/kindling_sdk/kindling_sdk/platform_glue.py # design-time PlatformAPI
  GlueAPI(PlatformAPI)                              # boto3 glue/logs/s3
    deploy_app        -> S3ArtifactStore.upload data-apps/<app>/
    register_app_job  -> CreateJob/UpdateJob (idempotent by name)
    submit_app_run    -> StartJobRun(Arguments={"--config:app_name": ...})  [or one-time job + run]
    get_job_status    -> GetJobRun state map (STARTING/RUNNING/STOPPING/STOPPED/SUCCEEDED/FAILED/TIMEOUT/ERROR)
    stream_stdout_logs-> CloudWatch GetLogEvents /aws-glue/jobs/output/<run_id>
    set_secret        -> Secrets Manager PutSecretValue
  S3ArtifactStore(ArtifactStore)                    # + s3:// branch in artifact_store_for()

packages/extensions/kindling_ext_glue/kindling_ext_glue/
  __init__.py                 # import-time register_provider("glue-bookmark", ...); binds GlueFileDiscovery
  entry.py                    # the Glue script Kindling uploads: SparkContext -> GlueContext -> job.init ->
                              #   __main__.spark = glueContext.spark_session -> kindling.initialize(config from
                              #   getResolvedOptions + spark.kindling.*) -> run app -> job.commit
  bookmark_source_provider.py # DynamicFrame source adapter, immediate .toDF(), IncrementalReadableEntityProvider
  file_discovery.py           # FileIngestion discovery variant on bookmarks (optional, later)
  data_quality.py             # engine.glue.expectations -> DQDL EvaluateDataQuality at persist.before_persist (later)
  telemetry.py                # optional CloudWatch EMF / OTLP-to-ADOT providers (later)

packages/kindling_cli/kindling_cli/orchestration/   # NEW design-time emitter, shared shape with the DAB proposal
  inventory.py                # apps, packages, wheels, config files (Spark-free; reuse _resolve_by_convention)
  render_glue.py              # glue/jobs/<app>.json (CreateJob payload), s3 layout, zip-of-wheels manifest
  render_airflow.py           # airflow/dags/<solution>_<app>.py using GlueJobOperator/GlueJobSensor
  manifest.py                 # provenance: source rev, generator version, wheel hashes, deployment inputs
  cli: kindling orchestration build --platform glue --target dev  (name to be agreed with the bundle proposal)
```

**Emission pipeline (Track A).** Inputs: `apps/<app>/app.yaml`, `settings*.yaml`,
`lake-reqs.txt`, built wheels, deployment inputs from CLI/env
(`KINDLING_ORCH_*`, same contract shape as `databricks_bundle_deployment.md:283-303`:
never read from runtime settings). Steps: inventory → resolve wheel closure →
build `<solution>-<ver>.gluewheels.zip` → render Glue job JSON per app → render
one Airflow DAG per solution (one `GlueJobOperator` per app, edges from
`app.yaml` dependencies or explicit inputs; Kindling's `PipeGraph`
(`pipe_graph.py:73`) runs *inside* the job) → write manifest. Output is
disposable, regenerated from inputs, never edited.

**Emitted artifacts and S3 layout.**

```text
s3://<bucket>/kindling/<solution>/<target>/
  scripts/kindling_glue_entry.py            # extension-owned entry (replaces runtime/scripts/kindling_bootstrap.py role)
  packages/<solution>-<ver>.gluewheels.zip  # spark-kindling[glue], spark-kindling-ext-glue, domain wheels, lake-reqs closure
  config/settings.yaml, settings.glue.yaml, settings.<env>.yaml
  data-apps/<app>/app.py, settings.yaml, settings.glue.yaml, settings.<env>.yaml, lake-reqs.txt
  checkpoints/<app>/...                     # kindling.storage.checkpoint_root
  tmp/                                      # --TempDir
dist/orchestration/glue/                    # local, disposable
  glue/jobs/<app>.json                      # CreateJob: Command{glueetl, ScriptLocation}, GlueVersion "5.1",
                                            #   WorkerType, NumberOfWorkers, ExecutionProperty{MaxConcurrentRuns:1},
                                            #   DefaultArguments{--additional-python-modules, --python-modules-installer-option --no-index,
                                            #   --extra-files <config paths>, --enable-glue-datacatalog, --datalake-formats delta,
                                            #   --conf spark.sql.extensions=... --conf spark.kindling.bootstrap.config_files=[...],
                                            #   --job-bookmark-option job-bookmark-disable (enable only when bookmark sources declared),
                                            #   --enable-continuous-cloudwatch-log true, --TempDir}
  airflow/dags/<solution>.py                # GlueJobOperator(job_name, script_location, create_job_kwargs, script_args={"--config:environment": ...},
                                            #   update_config=True, wait_for_completion=True or deferrable) per app; sensors for cross-DAG
  manifest.json
```

Runtime config precedence is unchanged: `--conf spark.kindling.*` maps through
`bootstrap.py:231-289`; explicit `spark.kindling.bootstrap.config_files` pointing
at `--extra-files` copies bypasses the storage-utils download (H2) until that
path is refactored; `settings.glue.yaml` is selected purely by the platform name
(`bootstrap.py:667-669`) once H1 is fixed.

### Track B: Glue 6.0 SDP (declarative)

Glue 6.0 runs OSS SDP: a `spark-pipeline.yml` (name, catalog, database, S3
`storage`, `libraries` globs, `configuration`) plus Python/SQL transformation
files zipped to S3, launched by a `glueetl` job with
`--enable-spark-declarative-pipeline true`, controlled by
`spark.glue.sdp.jobMode RUN|VALIDATE` and `--refresh/--full-refresh/--full-refresh-all`.
`kindling_ext_sdp` already emits this API and already writes a
`spark-pipeline.yml` for dry runs (`kindling_ext_sdp/dry_run.py:44-85`). A
`GlueSdpEngine(OssSdpEngine)` would be small; the emitter would zip a
definitions file that calls `kindling.initialize(engine="glue_sdp",
declaration_only=True)` + `register_all()` + `declare_pipeline()`, mirroring
`lakeflow_app_selector.py`. Blockers today: no expectations and no AUTO CDC on
Glue SDP (so SCD2 has no lowering; `capabilities.py` would mark Glue SDP as
`OSS_SDP` minus nothing), materialized views always full-recompute, Iceberg is
the recommended format for cross-run streaming-table state, core does not
declare pyspark 4.x, Python 3.13 compatibility of `dynaconf <3.3.2` and friends
is unverified, and there is no local Docker image. Revisit in one to two quarters.

## 11. Gap register

Severity: **High** blocks the MVP or silently corrupts data; **Medium** blocks a
later phase or degrades semantics; **Low** cosmetic or documented limitation.

| # | Contract / assumption | Location | Severity | Handling |
|---|---|---|---|---|
| G1 | Platform whitelist rejects `glue`; no Glue detection probes | `bootstrap.py:1040,1044-1084`; shim `runtime/scripts/kindling_bootstrap.py:234-262` | High | **Core change**: accept any registered platform name; add probes (`GLUE_VERSION`/`--JOB_NAME` args, `awsglue` importable, `spark.glue.*` conf) |
| G2 | Config/wheel download requires notebook storage utils | `bootstrap.py:620-623,728-756,1501-1528` | High | **Core change**: route through `PlatformService.read/copy/list`; interim: `--extra-files` + explicit `config_files` |
| G3 | Streaming queries have no trigger control; batch job never exits | `entity_provider_delta.py:1992-1994,1803`; `pipe_streaming.py`; `streaming_orchestrator.py:179-194` | High (for any `processing_mode: streaming` pipe) | **Core change**: `kindling.streaming.trigger` (default none; Glue overlay sets `availableNow`) honoured by starter and providers |
| G4 | Feature defaults assume Databricks when discovery is off | `features.py:107-118` | High | **Core change**: per-platform default sets, or `GlueService` supplies static features; interim: explicit `kindling.features.*` overrides in `settings.glue.yaml` |
| G5 | Three-part table names by default | `entity_resolution.py:233-246,277` | High | Adapter: `GlueEntityNameMapper` or documented `kindling.storage.table_schema` posture; system test asserting `db.table` |
| G6 | `spark_configs` pushed at runtime | `bootstrap.py:1785-1792` | Medium | Documented limitation: emitter places all `spark.*` into job `--conf`; runtime push stays for mutable keys |
| G7 | Managed-platform defaults turn on pip-at-init and watermarking for any non-standalone name | `bootstrap.py:2276,2317` | Medium | Glue overlay sets `install_bootstrap_dependencies: false`, `use_lake_packages: false`; watermarking stays on (desired) with `system.watermarks` created on first run |
| G8 | No `--index-url`/credential plumbing for pip | `pip_manager.py:95-137`; `bootstrap.py:1328-1363`; `data_apps.py:1073-1097` | Medium | Not needed if all deps ship via zip-of-wheels; **core change** if runtime private-index installs are wanted anywhere |
| G9 | `PlatformAPIRegistry` discovery is module-name only; CLI `click.Choice` lists; `known` set; `EXPECTED_PLATFORMS` test | `kindling_sdk/platform_provider.py:235-249`; `kindling_cli/test_runner.py:15`; `cli.py:43,1076-1089`; `tests/unit/test_platform_entry_points.py:20` | Medium | **Core change**: entry-point discovery for SDK APIs; widen choices; add `GLUE_*` env detection |
| G10 | `artifact_store_for` rejects `s3://` | `artifact_store.py:466-496,499-546` | Medium | New `S3ArtifactStore` + branch (SDK change) |
| G11 | Watermark store hard-tagged `provider_type: delta`; cursor = Delta version + CDF | `watermarking.py:77`; `entity_provider_delta.py:2085-2126` | Low on Delta; High if Iceberg | Delta-on-Glue: none. Iceberg: **core change** (configurable store provider, snapshot-id cursor) plus the proposed Iceberg provider |
| G12 | CDF must be enabled on every incremental source; externally created tables (crawlers, Athena) lack it | `entity_provider_delta.py:1063-1100,2123` | Medium | Documented limitation + `kindling migrate` `TAG_UPDATE` to set `delta.enableChangeDataFeed` |
| G13 | Concurrent same-pipe runs unsupported; single-writer Delta log store on S3 | `watermarking.py:513-524`; Glue `S3SingleDriverLogStore` | High if violated | Emitter sets `MaxConcurrentRuns: 1`; Airflow `max_active_runs=1`; document `S3DynamoDBLogStore` for multi-job writers |
| G14 | Blue/green migration uses `ALTER TABLE ... RENAME TO`; `CREATE OR REPLACE VIEW` for SQL entities | `migration.py:755-807`, `:292` | Medium | Spike; likely restrict Glue to storage-mode in-place rewrite and validate Glue view support for `sql_entity` |
| G15 | `DESCRIBE DETAIL`/`spark.catalog.tableExists` semantics against Glue Data Catalog | `entity_provider_delta.py:526-536,1171,1285-1289`; `migration.py:447,476` | Medium | Spike; permission errors must not read as "table missing" |
| G16 | No DQ vocabulary; expectations are Databricks-only | `engine.py:404-410`; `capabilities.py:65` | Medium (later phase) | Implement #245 validation runner in core; Glue DQDL as one adapter |
| G17 | Bookmark semantics differ from watermark contract (job-wide commit/reset) | see §7.1 | Medium | Adapter confined to raw S3/JDBC sources; documented at-least-once behaviour |
| G18 | File ingestion `autoloader` discovery has no Glue runner; `batch` discovery lists S3 per run | `file_ingestion.py:566-631,414` | Medium | Bind a Glue `AutoLoaderFileIngestionRunner` on Spark file source + `availableNow` (needs explicit schema), or bookmark discovery |
| G19 | `KINDLING_*` env overrides impossible (Glue requires `CUSTOMER_` prefix) | `spark_config.py:211`; Glue `--customer-driver-env-vars` | Low | Document `--conf spark.kindling.*` as the override channel |
| G20 | `workspace_id` config layer has no Glue analogue | `bootstrap.py:849-933` | Low | Pass explicitly (account/region) or leave unused |
| G21 | Unused module-scope `DeltaTable` imports make core require `delta-spark` | `data_entities.py:9`, `data_pipes.py:10`, `file_ingestion.py:13`, `watermarking.py:9` | Low | Remove (cheap hygiene); required only for a non-Delta path |
| G22 | Notebook manager eagerly bound | `notebook_framework.py:1074-1083` | Low | `GlueService` returns `[]` from notebook listing |
| G23 | Metrics have no core abstraction | `unified_otlp_telemetry_provider.md:137-150` | Low | Out of scope; Glue job metrics via console/CloudWatch |
| G24 | Extensions absent from CI release classification; system tests know only Azure clouds + Databricks | `.github/workflows/ci.yml`; `tests/system/conftest.py:18-19` | Medium (delivery) | Add `glue` to `ALL_PLATFORMS`, AWS credentials to CI, `tests/system/extensions/glue/` |

## 12. Effort estimate

Assumptions: one senior engineer familiar with Kindling internals; an AWS account
with Glue, S3, Secrets Manager, CloudWatch, MWAA (or the MWAA local runner) and
IAM already provisioned; Delta as the lake format; Glue 5.1; Track A only.
Numbers are engineer-weeks of focused work, excluding review latency. For scale,
the Dataproc evaluation estimated 5 to 7 weeks for a platform *without*
orchestration emission (`dataproc_platform_evaluation.md:527`).

| Phase | Scope | Estimate | Key assumptions |
|---|---|---|---|
| MVP-1 Runtime | `GlueService` (S3, secrets, session), G1/G2/G4 core changes, Glue overlay defaults, `[glue]` extra, unit tests, JVM-boundary allowlist entry | 2.5 wk | Config transport refactor through `PlatformService` is done properly, not patched with a third `if is_databricks` branch |
| MVP-2 Design-time | `GlueAPI`, `S3ArtifactStore`, registry/CLI widening (G9/G10), `kindling app deploy/run/status/logs` on Glue, CloudWatch log streaming, extension entry script | 2 wk | Reuse `_build_job_spec` shape; no in-cluster `DataAppDeployer` work |
| MVP-3 Orchestration emitter | inventory + wheel-closure resolver + Glue job JSON + Airflow DAG renderer + manifest; one job per app; CLI command; deterministic-output tests | 2.5 to 3 wk | Greenfield: the DAB proposal's inventory/config/artifact resolvers do not exist yet, and building them here should be designed to be shared with the bundle proposal. Airflow rendering itself is small |
| MVP-4 Delivery | AWS system-test infra (IaC under `iac/aws/`), CI job, `tests/system/extensions/glue/`, Glue Docker pre-deploy check, docs (`docs/guide/glue_*.md`, config reference) | 2 wk | An AWS account usable for CI |
| **MVP total** | batch jobs + Airflow DAG emission + deploy | **9 to 10 wk** (5 to 6 calendar weeks with two engineers) | Spikes in §13 resolved first |
| Later-1 Incremental sources | `glue-bookmark` source provider (DynamicFrame at the edge), job.init/commit lifecycle hook, bookmark-based file discovery, documentation of at-least-once | 2.5 wk | Watermark/CDF path needs **no** work; this is only for raw S3/JDBC |
| Later-2 Streaming | G3 trigger policy in core (1 wk), Glue streaming job type in emitter, Kinesis/MSK `DeclarableStreamingSource` provider (EventHub provider is the template, `entity_provider_eventhub.py`), checkpoint layout on S3, recovery via job retry | 4 wk | Continuous jobs are billed hourly; most pipelines will use `availableNow` batch drains instead |
| Later-3 Data quality | core validation runner per #245 (3 wk, benefits all targets) + Glue DQDL adapter (1 wk) | 4 wk | Portable form first; DQDL is an adapter, never the contract |
| Later-4 Iceberg | proposed `IcebergEntityProvider` with a Glue strategy, watermark-store provider config, snapshot cursor, migration typing | 5 to 6 wk | Only if Iceberg is mandated; Delta on Glue is the low-risk path |
| Later-5 Track B (Glue 6.0 SDP) | `GlueSdpEngine`, SDP zip emitter, pyspark 4.x support in core, Python 3.13 verification, system tests | 4 to 5 wk | After AWS publishes a Glue 6.0 image and SDP gains expectations/CDC, or their absence is accepted |

## 13. Spike plan

The three riskiest unknowns, each with a minimal experiment and pass criteria.
All can run in the official `public.ecr.aws/glue/aws-glue-libs:5` image against a
real Glue Data Catalog and S3 bucket, except where noted.

1. **Delta identity against Glue Data Catalog (G5, G14, G15).** Install
   `spark-kindling[standalone]` from the local wheel into the Glue 5 image, run a
   two-entity app (`bronze.orders` → `silver.orders` with `merge_columns` and
   `scd.type: "2"`) in catalog access mode with `kindling.storage.table_schema`
   set, `--enable-glue-datacatalog`, `--datalake-formats delta`. Observe:
   `saveAsTable`/`CREATE TABLE ... USING DELTA LOCATION` registration;
   `DeltaTable.forName("db.table")` and the `DESCRIBE DETAIL` fallback;
   `spark.catalog.tableExists` on missing table vs missing database vs denied
   permission; `readChangeFeed` incremental second run advancing
   `system.watermarks`; `ALTER TABLE ... RENAME TO`, `ALTER TABLE ADD COLUMNS`,
   `CREATE OR REPLACE VIEW` (for `sql_entity`). **Pass:** two consecutive runs
   produce correct SCD2 rows with one cursor row per `(source, pipe)`; every
   DDL either works or degrades along an existing Kindling fallback. Two days.
2. **Real Glue job bootstrap (G1, G2, G3, G6, G7).** Create one Glue 5.1 job
   with a hand-written entry script implementing the §10 sequence (`GlueContext`
   → `__main__.spark` → `kindling.initialize` with `config_files` from
   `--extra-files` and `--conf spark.kindling.*` → run app → `job.commit`),
   dependencies via a `*.gluewheels.zip` with `--no-index`. Observe cold-start
   time, whether Kindling's pip-at-init can be fully disabled, whether the
   `spark.jvm_bridge` probe binds the right telemetry providers, how CloudWatch
   receives stdlib logs, behaviour of a `processing_mode: streaming` pipe with
   and without a hand-injected `availableNow` trigger, and what happens on a
   second concurrent `StartJobRun`. **Pass:** the app runs end to end with no
   Kindling code changes beyond a monkey-patched platform whitelist; the list of
   unavoidable core changes matches §14. Two to three days.
3. **Airflow DAG emission shape (MVP-3 design risk).** Hand-write the DAG the
   emitter would produce for a two-app solution and run it in the MWAA local
   runner (or `airflow standalone`) against the job from spike 2: `GlueJobOperator`
   with `update_config=True` and `create_job_kwargs` carrying the full job
   definition, `script_args` carrying `--config:environment`, one
   `GlueJobSensor` cross-app edge, `max_active_runs=1`. Observe whether the
   operator's job-update semantics let the DAG own the job definition (removing
   the need for a separate `CreateJob` step), how `ConcurrentRunsExceededException`
   surfaces, and how Kindling's app-level failure maps to task failure and retry.
   **Pass:** a schedule-driven run updates the job, waits, streams logs, fails
   correctly on a forced pipe failure, and reruns without watermark corruption.
   Two days.

Optional fourth spike for Track B: run `spark-pipelines dry-run` on a
`kindling_ext_sdp`-declared graph inside a Glue 6.0 job with
`spark.glue.sdp.jobMode VALIDATE`, and separately `pip install spark-kindling`
on Python 3.13 to surface dependency breakage. One day each.

## 14. Core changes required (affect all targets)

Ordered by necessity for the Track A MVP.

1. **Platform detection accepts registered names** and gains Glue probes
   (`bootstrap.py:1036-1084`; `runtime/scripts/kindling_bootstrap.py:234-262`).
   Also stop detection from creating a Spark session as a side effect
   (`:1050`) where possible.
2. **Config and wheel transport through `PlatformService`** instead of notebook
   storage utilities (`bootstrap.py:589-812`, `:1368-1625`). Benefits every
   target: it removes the `"DBUtils" in type(...).__name__` branching and makes
   the standalone service a first-class transport too.
3. **Streaming trigger policy** as configuration honoured by
   `SimplePipeStreamStarter` and the Delta provider (`pipe_streaming.py`,
   `entity_provider_delta.py:1803,1992`). Needed by any batch-job target running
   streaming pipes; today only callers passing `options["trigger"]` get one.
4. **Platform-neutral feature defaults**: replace the single
   `MODERN_RUNTIME_DEFAULTS` (`features.py:107-118`) with per-platform default
   sets supplied by the platform service, so `discovery: false` is safe off
   Databricks.
5. **SDK registry discovery and CLI choices**: entry-point discovery in
   `PlatformAPIRegistry` (`kindling_sdk/platform_provider.py:229-250`), an
   `s3://` branch and `S3ArtifactStore` in `artifact_store.py:466-546`, wider
   `SUPPORTED_PLATFORMS`/`APP_RUN_PLATFORMS` (`test_runner.py:15`, `cli.py:43`),
   `GLUE_*` env detection (`cli.py:1076-1089`), `EXPECTED_PLATFORMS`
   (`tests/unit/test_platform_entry_points.py:20`), `ALL_PLATFORMS`
   (`tests/system/conftest.py:18`).
6. **Job lifecycle hooks around app execution** (`bootstrap.py:2448-2458`,
   `data_apps.py:460-507`): a signal or extension hook before/after the app so a
   platform can wrap `job.init`/`job.commit` (Glue) or equivalent, without users
   writing platform code.
7. **Design-time orchestration emitter scaffolding** (inventory, config
   resolver, wheel-closure resolver, provenance manifest) in `kindling_cli`,
   designed once for both the DAB proposal and Glue. Not core runtime, but a
   shared design decision that should be recorded in
   `databricks_bundle_deployment.md` before either emitter is built.
8. Optional / later: `--index-url` plumbing in `PipManager` and its two
   bypassing call sites (G8); configurable watermark-store provider and
   format-neutral cursor (G11, Iceberg only); remove the four unused
   `DeltaTable` imports (G21); a core validation contract (#245) so data quality
   is portable rather than engine-specific (G16).

Nothing in this list changes Kindling's declaration vocabulary. The changes are
confined to bootstrap transport, defaults, one streaming knob, and design-time
tooling, which is consistent with the core/runner split direction in
`kindling_core_runner_split.md:88-111`.

## Sources (AWS and Airflow, checked 2026-09-17)

- Glue versions and runtime matrix: <https://docs.aws.amazon.com/glue/latest/dg/release-notes.html>
- Glue 6.0 migration, breaking changes, OTF versions: <https://docs.aws.amazon.com/glue/latest/dg/migrating-version-60.html>
- Glue 6.0 announcement (GA 2026-08-21): <https://aws.amazon.com/blogs/big-data/introducing-aws-glue-6-0-for-apache-spark/>
- Spark Declarative Pipelines on Glue 6.0: <https://docs.aws.amazon.com/glue/latest/dg/spark-declarative-pipelines.html>
- Version support policy: <https://docs.aws.amazon.com/glue/latest/dg/glue-version-support-policy.html>
- Job parameters reference: <https://docs.aws.amazon.com/glue/latest/dg/aws-glue-programming-etl-glue-arguments.html>
- Python libraries, zip-of-wheels, requirements.txt, virtual envs: <https://docs.aws.amazon.com/glue/latest/dg/aws-glue-programming-python-libraries.html>
- Job bookmarks (operational details, reset/rewind): <https://docs.aws.amazon.com/glue/latest/dg/monitor-continuations.html> and <https://docs.aws.amazon.com/glue/latest/dg/programming-etl-connect-bookmarks.html>
- Delta Lake on Glue: <https://docs.aws.amazon.com/glue/latest/dg/aws-glue-programming-etl-format-delta-lake.html>
- Lake Formation fine-grained access considerations: <https://docs.aws.amazon.com/glue/latest/dg/security-lf-enable-considerations.html>
- Streaming ETL jobs: <https://docs.aws.amazon.com/glue/latest/dg/add-job-streaming.html>
- Data Quality `EvaluateDataQuality`: <https://docs.aws.amazon.com/glue/latest/dg/aws-glue-api-crawler-pyspark-transforms-EvaluateDataQuality.html>
- Local development Docker image: <https://docs.aws.amazon.com/glue/latest/dg/develop-local-docker-image.html>; Glue 6.0 image not yet published: <https://github.com/awslabs/aws-glue-libs/issues/254>
- Glue Workflows: <https://docs.aws.amazon.com/glue/latest/dg/orchestrate-using-workflows.html>
- Airflow Amazon provider Glue operators: <https://airflow.apache.org/docs/apache-airflow-providers-amazon/stable/_api/airflow/providers/amazon/aws/operators/glue/index.html>
