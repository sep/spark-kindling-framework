# Runtime Manifest

**Status:** Proposed. Design findings against the current bootstrap; no code
changes yet.
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

## Decisions needed

1. `system.runs` in core (opt-in), or telemetry only.
2. Fingerprint scope: key packages, the full set, or both (recommended).
   A configuration fingerprint is deferred because of the risk of leaking
   secrets.
3. Untested runtime: warn (recommended) or fail; ship the matrix in the wheel.
4. A process-level `run_id` as the parent of pipe-run ids, passed to watermarks
   and provenance.
5. Whether app dependency installs get the same key-package pins and change
   reporting (a behaviour change).

## Unverified

How to read the runtime version on Fabric and Synapse, the Databricks
serverless environment version, Databricks job and run ids, and the Delta
version where no `delta-spark` distribution is installed (Databricks). These
need checking on real clusters. Calls through py4j must be guarded on Spark
Connect and Unity Catalog shared clusters.
