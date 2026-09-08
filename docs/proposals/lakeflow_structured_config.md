# Lakeflow Structured Configuration

**Status:** Superseded.
**Created:** 2026-09-08
**Superseded:** 2026-09-08 by the Lakeflow canonical-configuration
correction (`fix/lakeflow-canonical-config`).
**Related:** `package_config_architecture.md`,
`docs/guide/dab_config_promotion.md`,
`docs/guide/lakeflow_app_selection.md`,
`docs/contributing/databricks_execution_contract.md`.

## Superseding Correction

This proposal correctly identified the need to carry structured
`dataentities:` / `datapipes:` configuration into Lakeflow and the evidence that
literal dotted entity IDs must survive configuration loading. Its implementation
conclusion was wrong: Lakeflow should not own a separate config key, list
parser, YAML pre-parser, section validator, or file-access path.

Current guidance is to use Kindling's canonical configuration lifecycle:
`spark.kindling.bootstrap.config_files` for explicit settings files, or
`spark.kindling.bootstrap.artifacts_storage_path` with
`spark.kindling.bootstrap.environment` and
`spark.kindling.bootstrap.workspace_id` for discovered hierarchy loading. The
selector sets `declaration_only=true`, supplies the selected `app_name` to
`kindling.initialize()`, and leaves parsing, validation, precedence, and
structured overlays to shared bootstrap/Dynaconf code.

Probe A below is now treated as the shared `_merge_dotted_key()` defect: mapping
payload keys must stay literal, while top-level dotted bootstrap paths still
split. Probes B-D remain useful evidence, but no longer justify a
Lakeflow-specific transport or parser.

## Problem

`declare_from_pipeline_config()` is the declaration-time adapter for a Databricks
Lakeflow pipeline. Its lifecycle is fixed in
`packages/extensions/kindling_ext_databricks/kindling_ext_databricks/lakeflow_app_selector.py:345-415`:
read the selected app, authorize it, initialize Kindling with bridged pipeline
configuration, import/register the app, then declare the graph.

The bridge that feeds Kindling is flat by construction
(`lakeflow_app_selector.py:150-192`). It enumerates or point-looks-up Spark
configuration, copies scalar `kindling.*` and `datapipes.*` keys, rewrites
`spark.kindling.*`, pins the selected app and allowlist, and defaults
`platform` to `standalone`. It never sets the top-level bootstrap key
`config_files`, so a Lakeflow pipeline currently loads zero settings files
through this path.

That means a Databricks Asset Bundle can set scalar keys such as
`kindling.sdp.dataset_naming`, but cannot express Kindling's structured sections:
`dataentities:`, `dataentities-bytag:`, `datapipes:`, or `datapipes-bytag:`.
The blocking case is `dataentities:` keyed by dotted logical entity IDs such as
`bronze.device_telemetry`. Encoding those IDs into flat Spark configuration keys
is ambiguous because the entity ID dots and configuration-path dots are
indistinguishable.

## Evidence

Four probes drove the decision.

### Probe A: `initial_config` Splits Dotted Mapping Keys

`DynaconfConfig.initialize()` first loads settings files and then translates
bootstrap input (`packages/kindling/spark_config.py:142-173`). The bootstrap
translation builds nested trees through `_merge_dotted_key()`
(`spark_config.py:212-248`), and `_merge_dotted_key()` recursively splits dotted
keys inside dictionary values too (`spark_config.py:250-275`).

That means `_merge_dotted_key({}, "dataentities",
{"bronze.device_telemetry": {...}})` becomes:

```python
{"dataentities": {"bronze": {"device_telemetry": {...}}}}
```

Any structured configuration handed to `initialize(config=...)` as dictionary
data violates the dotted-ID requirement before it reaches the config pattern
matcher.

### Probe B: YAML Settings Files Preserve Dotted Keys

Dynaconf settings files built the way Kindling builds them preserve
`dataentities:` keys such as `bronze.device_telemetry` and
`silver.device_telemetry` as single keys. They also keep `dataentities-bytag:`
intact. This is the critical behavior absent from the direct dictionary path.

### Probe C: Existing Precedence Is Flat Last

Because settings files load before `_translate_bootstrap_to_nested()`, the flat
bridged pipeline configuration is applied after the YAML. In a probe where YAML
set `kindling.sdp.dataset_naming: leaf` and the bridged configuration set
`kindling.sdp.dataset_naming` to `normalized`, the resolved value was
`normalized`.

The same probe confirmed that unrelated YAML namespaces survive the bridged
namespace merge: a YAML-only `kindling.sdp.dataset_naming` and sibling
`kindling.some_yaml_only_key` survive setting the bridged `kindling` namespace,
and a bridged `datapipes.other.pipe.enabled` coexists with YAML
`datapipes: {some.pipe: ...}`.

### Probe D: Dynaconf Diagnostics Are Not Sufficient

A missing settings file is silently ignored by Dynaconf, so a typo would make
the feature appear to do nothing. Invalid YAML raises a raw
`dynaconf.vendor.ruamel.yaml.parser.ParserError` from inside Dynaconf without
naming the Lakeflow configuration key. A non-mapping `dataentities:` section can
reach `ConfigPatternMatcher._compile()` and raise a bare shape error; the
matcher otherwise tolerates non-mapping per-pattern override values by warning
and skipping them (`packages/kindling/config_patterns.py:159-171`).

The supported Lakeflow surface therefore needs keyed source validation before
handing paths to Kindling. That validation is diagnostic only; it does not add a
new merge path.

## Options

| Option | Mechanism | Evidence | Decision |
| --- | --- | --- | --- |
| OPT-001 | `kindling.lakeflow.config_files`: a comma-separated, order-preserving list of deployed YAML paths, passed to `kindling.initialize(config={"config_files": [...]})` | Probe B preserves dotted entity IDs. Probe C keeps structured sections and gives deterministic precedence. `packages/kindling/bootstrap.py:1922-1928` already normalizes top-level `config_files`. `docs/guide/dab_config_promotion.md:1-84` documents the existing DAB workspace-file transport for normal Kindling config files. | Selected |
| OPT-002 | `kindling.lakeflow.config_json`: an inline JSON object deep-merged into the bridged configuration | Probe A is fatal: direct dictionary data splits dotted entity IDs. Salvaging it would require materializing a temporary YAML file and passing that as `config_files`, adding a temp-file lifecycle and unstable path strings for the re-init guard. | Rejected |
| OPT-003 | `kindling.lakeflow.config_resource`: a package resource such as `my_app:config/lakeflow.yaml` | App wheel readability is structurally likely because `register_all()` already requires the package to import, but resource values are baked into the wheel and still must become filesystem paths, likely through `importlib.resources.as_file()`, before reaching the same `config_files` plumbing. | Deferred follow-up |
| OPT-004 | Flat escaped-key encoding for entity IDs | Decoding would still have to feed the result through settings files to avoid Probe A. This adds a proprietary escaping convention without structural benefit. | Rejected |

## Superseded Decision

The superseded v0.12.35 decision shipped one Lakeflow-specific surface:
`kindling.lakeflow.config_files`. This is historical context, not current
guidance; the key now exists only as a deprecated compatibility alias whose
removal is eligible at 0.13.0.

The key is read by direct `spark.conf.get(key, None)` point lookup from a module
constant in the selector. It is not listed in `kindling.lakeflow.config_keys`.
Its value is a comma-separated list of YAML paths. Paths are split, stripped,
empties are dropped, order is preserved, and every path is normalized with
`os.path.abspath` before validation and emission.

Only `.yaml` and `.yml` are accepted. Each file is checked before Kindling sees
it: missing files, blank set values, unsupported suffixes, invalid YAML,
non-mapping documents, non-mapping structured sections, and non-mapping
per-entity `dataentities:` or `datapipes:` values are raised as
`LakeflowConfigSourceError`, a subclass of `LakeflowAppSelectionError`.

Validated paths are passed through the existing public bootstrap contract:

```python
kindling.initialize(config={"config_files": ["/absolute/path/to/file.yaml"]})
```

This is the only selected option whose source reaches the one verified code path
that preserves dotted logical entity IDs. It reuses Kindling's existing
configuration loading and overlay machinery rather than inventing selector-local
merge semantics.

## Precedence

The selected order is:

1. Structured YAML files from `kindling.lakeflow.config_files`, in listed order.
2. Flat bridged Lakeflow pipeline-configuration keys, applied as Kindling
   bootstrap `initial_config`.
3. Selector-authoritative values in the bridged dict: `kindling.data_app`,
   `kindling.lakeflow.allowed_apps`, and the declaration-time `platform` default.

Flat bridged keys therefore win over structured YAML when both set the same
scalar configuration path. This deliberately inverts the requirements
artifact's tentative OQ-002 assumption. Inverting it would require hand-merging
outside the supported machinery, because `DynaconfConfig.initialize()` loads
`settings_files` first and only then applies `initial_config`
(`spark_config.py:142-173`).

That order is also defensible operationally: the structured YAML is the shared
app or environment layer, while the pipeline `configuration:` block is the
deployment-specific override layer. Structured sections such as `dataentities:`
and `dataentities-bytag:` have no flat-key equivalent, so this mainly affects
scalar paths such as `kindling.sdp.dataset_naming`. The `datapipes:` namespace is
the exception because the existing bridge already carries flat `datapipes.*`
keys; the namespace merge was verified as additive for non-colliding entries.

One accepted limitation follows from the same order: `kindling.platform.environment`
inside structured YAML is inert for this selector. Platform defaulting inspects
only the bridged dict and the bridged dict wins.

## Deviations From Pure Machinery Reuse

The implementation reuses Kindling's supported `config_files` loading and
overlay path, but two validation steps intentionally sit in front of it.

### YAML Pre-Parse

The selector pre-parses each configured YAML file with `yaml.safe_load` before
passing the path to Dynaconf. This is required because Probe D showed that a
missing settings file is silently ignored and invalid YAML raises a raw parser
exception with no mention of `kindling.lakeflow.config_files`. Those diagnostics
do not satisfy the Lakeflow selector contract.

This is a second parser, not the parser Dynaconf uses. Dynaconf loads YAML with
its vendored ruamel parser, so a document accepted by PyYAML but rejected by
Dynaconf can still fail later inside `initialize()`. The pre-parse catches the
common missing/invalid-source cases with a keyed error; it does not claim total
parser equivalence.

### Per-Entity Shape Strictness

The selector rejects non-mapping per-entity override values inside
`dataentities:` and `datapipes:`. This is stricter than the existing
`ConfigPatternMatcher._compile()` behavior, which warns and skips a non-mapping
override value (`config_patterns.py:159-171`). The stricter Lakeflow behavior is
required because a Bundle-declared structured source with a wrong-shaped entity
mapping would otherwise look like it loaded while silently dropping placement.

The consequence is intentional and documented: identical YAML can be rejected
through `kindling.lakeflow.config_files` but only warned about through other
direct `config_files` routes.

## What Was Verified

Verified in the repository:

- The selector lifecycle, flat bridge, restricted-runtime fallback, and
  declaration call order in `lakeflow_app_selector.py:99-192` and
  `lakeflow_app_selector.py:345-415`.
- Top-level `config_files` normalization in `bootstrap.py:1922-1928`.
- Re-init equality checking for requested `environment` and `config_files` in
  `bootstrap.py:1820-1852`; deterministic absolute paths keep repeated
  Lakeflow source evaluation stable.
- Dynaconf settings-file-before-bootstrap ordering in `spark_config.py:142-173`
  and nested bootstrap translation in `spark_config.py:212-275`.
- Overlay persistence for registered and later-registered entities through
  `DataEntityManager` raw params and persisted matchers in
  `packages/kindling/data_entities.py:810-831` and
  `data_entities.py:876-905`.
- Existing naming-boundary documentation:
  `docs/contributing/databricks_execution_contract.md:187-225` and
  `packages/extensions/kindling_ext_sdp/README.md:108-130`.
- Existing DAB config-file transport for normal jobs and notebooks in
  `docs/guide/dab_config_promotion.md:1-84`.
- Existing restricted-runtime point-lookup facts in
  `docs/guide/lakeflow_app_selection.md:68-78`.

Verified by planning probes:

- Probe A: direct `initial_config` dictionary data splits dotted nested keys.
- Probe B: YAML settings files preserve dotted `dataentities:` keys and
  `dataentities-bytag:`.
- Probe C: flat bridged values win, while unrelated YAML `kindling.*` and
  `datapipes:` siblings survive namespace merging.
- Probe D: missing files are silent in Dynaconf, invalid YAML surfaces as a raw
  vendored-ruamel parser error, and wrong-shaped structured sections can reach
  bare matcher failures.

Assumed but not shipped:

- Dynaconf's non-YAML loader suffixes behave as documented. Because `.json` and
  `.toml` diagnostics were not implemented or tested, the Lakeflow surface
  accepts only `.yaml` and `.yml` and records the other suffixes as follow-up.

Unverified:

- A Lakeflow serverless pipeline environment reading a companion file from
  `/Workspace/...`. The repository documents `/Workspace/...` readability for UC
  shared/standard-access clusters and jobs, not for Lakeflow serverless
  pipelines. The selected loader is path-agnostic, so a Unity Catalog volume path
  is the conservative alternative when workspace-file readability has not been
  confirmed in a target pipeline environment.

## Follow-Ups

- Add `kindling.lakeflow.config_resource` as a pointer resolver over the same
  `config_files` path, if teams need configuration bundled with an app wheel and
  accept rebuilds for configuration changes.
- Add `.json` and `.toml` support only with parser-specific diagnostics and
  tests equivalent to the YAML coverage.

## Coverage

| ID | Status |
| --- | --- |
| REQ-006 | covered |
| US-004 | covered |
| TS-006 | covered |
| OPT-001 | covered |
| OPT-002 | covered |
| OPT-003 | covered |
| OPT-004 | covered |
| AC-014 | covered |
| AC-017 | covered |
| OQ-001 | covered |
| OQ-002 | covered |
| OQ-003 | covered |
