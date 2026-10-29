# Runtime Manifest

**Status:** Phases 1 (`kindling.runtime_manifest`, with `run_id` in
provenance stamps) and 2 (`kindling.supported_runtimes`, the untested-runtime
warning) implemented. Phase 3 (`system.runs`) not started.
**Created:** 2026-10-16
**Related:** `docs/reference/config_reference.md`
(`kindling.runtime.features.*`, including the Databricks runtime version),
`kindling_cli_devex_gaps.md` (planned "runtime version skew" check),
`migration_fit_for_purpose_findings.md` and issue #247 (durable run records;
`system.runs` could be their parent), `provenance_stamping.md` (rows link to
the run by `run_id`), `scripts/check_extension_runtime_packages.py` (the
supported-runtime package matrix).

## Problem

For a given run, Kindling can't say exactly what it ran on: platform runtime,
Spark, Delta, Python, the Kindling and extension versions, the app version,
and which packages an install changed. Most of this is gathered during
bootstrap and then discarded:

- Installed package versions are read before each extension install
  (`_installed_distribution_snapshot`, `bootstrap.py:1406`); afterwards
  `_report_replaced_distributions` only logs what changed.
- The 14 key runtime packages (`_RUNTIME_KEY_DISTRIBUTIONS`) are pinned at
  their installed versions, which are never recorded.
- Feature discovery records `spark.version` and, on Databricks only, the DBR
  version string (`features.py:151-194`). Fabric and Synapse get no runtime
  version.
- The shim defaults `kindling_version` to `"latest"`, so the version that
  actually ran is often not recorded.
- App dependency installs run a plain `pip install` with no key-package pins
  or change report: a second, unrecorded source of drift.
- There is no initialization-level id and nothing durable links ids
  together. Executor runs do have ids (`generation_executor.py:321`,
  `data_pipes.py:821`): a new uuid per `execute` / `run_datapipes` call. But
  one initialization can make several such calls, nothing ties them to the
  bootstrap or to a record, and `system.watermarks.last_execution_id` is
  another fresh uuid per save, so it links to nothing.
- The bootstrap trace is only flushed at the end of the run, so it is lost if
  the process is killed, and its root span carries no versions.

## Proposal

### The manifest

Built once per `initialize_framework`, after dependency install, and extended
after app dependency installs:

| Group | Fields |
|---|---|
| Identity | `run_id` (new, per initialization; the run record's key), the executor run ids made under it, Spark application id, platform job/run ids where available |
| Platform | name, runtime version (DBR, Fabric, Synapse, standalone), serverless / Spark Connect flag |
| Engine | Spark and PySpark versions, Python, Java/Scala (only when the JVM bridge is available), Delta |
| Kindling | `spark-kindling` and every loaded extension: version requested and version resolved |
| App | name, version, deploy time |
| Packages | key-package versions; every change an install made (`name`, before, after, cause) |
| Fingerprints | `runtime_fingerprint`, `package_fingerprint` (below) |

Held in a `RuntimeManifest` with a `get_runtime_manifest()` accessor; no new
cross-module config keys. Cost: one more versions-only pass over installed
distributions (to be measured on DBR images) and no Spark statements, so it
runs even with feature discovery off.

### Where it is recorded

1. **Log:** one INFO line with the main versions and fingerprints, on every
   platform.
2. **Trace:** a small `runtime_manifest` span emitted right after bootstrap,
   not only at the end-of-run flush, with scalar fields only (span attribute
   size limits).
3. **Run record (opt-in):** a `system.runs` entity, defined like the watermarks
   entity, with run id, start/end, status, app and version, platform runtime,
   Kindling version, both fingerprints and the full manifest as JSON. It can be
   the parent record for migration runs (#247).
4. **Rows:** output rows carry `run_id` and the executor's `execution_id`
   (and maybe the runtime fingerprint) through provenance stamping and join
   to the run record on `run_id`; the manifest is not embedded per row. The
   run record lists the executor run ids made under it, so either id finds the
   run. Also pass the executor run id as the watermark `last_execution_id`.

### Drift and untested runtimes

- **Fingerprints:** `runtime_fingerprint` hashes platform, runtime version,
  Python major.minor and key-package versions, so it changes when the vendor
  updates the runtime. `package_fingerprint` hashes the full installed set
  after all installs, so it changes when any install replaces something.
- **Untested-runtime warning:** ship the supported-runtime matrix (today only
  in the CI check script) inside the wheel, with the script importing it. At
  bootstrap, an exact match logs the tested runtime; a partial match warns and
  lists the differing packages; an unknown runtime warns as untested. Versions
  must be normalized (Fabric's vendor PySpark builds compare by base release;
  conda-versioned packages are skipped) or the warnings become noise.
- Today the matrix means "extension installs don't replace key packages", not
  "Kindling is system-tested here". Deciding whether it becomes the declared
  supported-runtime matrix is part of this.

## Phases

1. Manifest, log line, early span, change capture, fingerprints. **S–M**
2. Packaged matrix and the untested-runtime warning. **S**
3. `system.runs`, and `run_id` passed to watermarks and provenance. **M**

## Decisions (2026-10-22)

1. **`system.runs` in core, opt-in** (phase 3), defined like
   `system.watermarks` and off by default; rows join to provenance stamps on
   `run_id`.
2. **Fingerprints: both** the key packages (`runtime_fingerprint`) and the
   full installed set (`package_fingerprint`). A configuration fingerprint
   stays deferred (secrets).
3. **Untested runtime: warn** (phase 2), with the matrix shipped in the wheel
   and the CI check script importing it.
4. **A process-level `run_id`** per initialization is the parent of the
   executor run ids (listed in the manifest) and is carried by batch and
   streaming provenance stamps (declared SDP stamps keep it null). Passing
   it to the watermark `last_execution_id` belongs to phase 3.
5. **App dependency installs: report now, pins opt-in.** Every change an
   app install makes is recorded and logged; pinning the key packages there
   is opt-in with `kindling.apps.pin_runtime_packages: true`, since an app
   may need a newer one.

Phase 1 as built: the manifest is begun before the dependency install (so
every change is captured, with its cause), completed in a
`runtime_manifest` bootstrap phase from installed distribution metadata,
the session's `spark.version` / `spark.conf` and discovered features, and
logged on one INFO line. The `runtime_manifest` span is emitted right after
initialization, before an auto-run app, not with the bootstrap span tree
that is flushed when the app ends. Executor run ids come from
`orchestrator.execution_started`, `datapipes.before_run` and
`streaming.orchestrator_started`.

## Decisions (2026-10-28, phase 2)

1. **The matrix is the system-tested runtimes**: `kindling.supported_runtimes`
   lists the runtimes the system tests run on (DBR 17.3 LTS, Fabric 1.3,
   Synapse 3.5) and the key packages each ships. The CI extension check loads
   it by file path, so it never imports Kindling or Spark.
2. **Match by platform and packages.** Fabric and Synapse report no runtime
   version, so the candidates are the platform's entries (on Databricks, only
   those of the reported DBR version) and the closest wins: fewest
   major.minor differences, then fewest differences. Only the packages an
   entry ships are compared; a missing package is skipped; a vendor build
   compares by its base release and a conda date-versioned build is skipped.
3. **Warn at major.minor.** `tested` (patch differences recorded only),
   `partial` (warns, naming the packages), `untested` (warns), `unchecked`
   (standalone: recorded, never warned). The result is in the manifest
   (`runtime_match`), the INFO line and the span. The provenance system test
   asserts every CI platform is recognized (`tested` or `partial`).

## Unverified

How to read the runtime version on Fabric and Synapse, the Databricks
serverless environment version, Databricks job and run ids, and the Delta
version where no `delta-spark` distribution is installed (Databricks). These
need checking on real clusters. Calls through py4j must be guarded on Spark
Connect and Unity Catalog shared clusters.
