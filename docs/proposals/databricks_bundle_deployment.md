# Databricks Bundle Build and Deployment

**Status:** Proposed; commands below are not implemented.
**Created:** 2026-09-09

## Recommendation

Use Kindling CLI to materialize a disposable Databricks bundle from the existing
domain project, reuse its built Python wheels, and use Databricks CLI to
validate, deploy, and run it. All bundle artifacts must be either generated
output or existing domain-project assets. Developers do not maintain a second
set of bundle YAML, source wrappers, or package definitions.
Start with Lakeflow pipelines. Keep the existing KDA and
durable-runner deployment commands as their own execution path.

Generate `databricks.yml` and included `resources/<app>.pipeline.yml` files.
There is no need for a separate Kindling `lakeflow.yml` deployment language.
An included file could be named `lakeflow.yml`, but its contents would still
be ordinary Databricks bundle resource configuration.

Databricks requires one root `databricks.yml` and supports splitting resource
definitions into included YAML files. Its current name for DABs is
Declarative Automation Bundles (formerly Databricks Asset Bundles).
See [bundle configuration](https://docs.databricks.com/aws/en/dev-tools/bundles/settings).

## Existing decisions and implementation

- [Config promotion](../guide/dab_config_promotion.md) already assigns promotion
  to `databricks bundle deploy`. Kindling owns configuration semantics and
  environment overlays; bundle targets own workspace deployment settings.
- [Lakeflow app selection](../guide/lakeflow_app_selection.md) defines an
  installed wheel entry point in `spark_kindling.data_apps`, pointing to a
  module with declaration-only `register_all()`. A generic source calls
  `declare_from_pipeline_config()`; one pipeline selects one app.
- [Structured configuration](lakeflow_structured_config.md) specifies
  `kindling.lakeflow.config_files`. The selector implementation now reads and
  validates these paths and passes them into initialization. Its proposal's
  historical problem statement should not be read as current behavior.
- [CLI conventions](../../packages/kindling_cli/README.md) separate buildable
  packages under `packages/` from apps under `apps/`. `kindling app package`
  creates a KDA; `kindling app deploy` delegates remote app storage to the SDK.
  Neither command currently creates a bundle or a Lakeflow pipeline.
- The current app scaffold uses `app.py`, `app.yaml`, and `lake-reqs.txt`.
  It does not produce the Lakeflow wheel entry-point contract. Existing
  Lakeflow test apps demonstrate that contract separately.

The missing feature is a supported bridge between these conventions, rather
than a new deployment backend inside the runtime framework.

## Ownership and layout

Default to one logical bundle per domain solution, with one pipeline resource
per selected app. Materialize it beneath an ignored build directory; its local
location is not its deployment identity. Independently released solutions can
use separate logical bundles later.

```text
domain-repo/
  config/
    settings.yaml
    settings.dev.yaml
    settings.prod.yaml
  apps/
    orders/
      app.yaml
      lake-reqs.txt
  packages/
    sales/
      pyproject.toml
      src/sales/...
  dist/                     # ignored, disposable build outputs
    bundles/databricks/
      databricks.yml
      resources/orders.pipeline.yml
      pipelines/kindling_lakeflow.py
      config/...            # staged existing configuration
      wheels/...            # staged existing wheels, generated adapter if needed
      manifest.json         # input versions/hashes, no credentials
```

Regenerate the complete output from maintained inputs. Generated files are
reviewable but never edited as source. Stage copies within the output root so
the bundle is self-contained and independent of source-tree-relative paths.
Only replace the designated generated directory, never existing project files.

Use existing app names, package metadata, `lake-reqs.txt`, and runtime YAML as
project inputs. Deployment choices are maintained separately, for example in
GitHub repository/environment variables, and passed through CLI options or
process environment variables. Do not add deployment keys to `settings*.yaml`
or read deployment values from runtime overlays. No maintained bundle manifest
is required.

The generated `databricks.yml` translates deployment inputs into bundle identity,
includes, sync rules, targets, permissions, and execution identity. Generated
pipeline resources combine these inputs with app selection and built artifact
references. Runtime settings are staged as runtime settings. The selected
runtime environment determines which overlays the pipeline reads; the deployment
target identifies where resources are deployed. These names may differ.

## App packaging contract

A Lakeflow app is installed as a wheel, not executed from a KDA. Reuse an
existing `spark_kindling.data_apps` entry point when available. Otherwise,
generate a small adapter wheel containing the entry point and registration
module from the app's existing package selection. Its source, packaging
metadata, and wheel are disposable output; no additional maintained app
package is required. Keep `apps/<name>/` as the authoring/configuration location.

The module's `register_all()` registers the selected packages' entities and
pipes. It must not execute `app.py`, invoke the runner, install packages, or
perform writes. Existing imperative apps require an explicit adaptation;
bundle generation must not claim that any KDA can become a Lakeflow pipeline.
Read metadata during generation without importing arbitrary app code. Reuse
the existing registration/discovery contract; verify its declaration-only
behavior before relying on it in the adapter. Reject apps whose behavior
cannot be represented through that contract. Custom declaration logic, when
necessary, belongs in the domain package and is reusable outside bundling.

Consume the selected package wheels and their local dependency closure from
the existing Poetry/poe build outputs or retained release artifacts. Report
missing or stale artifacts; an explicit build option may invoke existing build
tasks, but bundle generation must not silently rebuild release wheels.
Supply pinned Kindling core, SDP, and
Databricks extension dependencies through the pipeline environment. Exclude
CLI/SDK development dependencies and local Spark installation extras from the
pipeline dependency set. Verify this against the current package metadata.

Databricks supports bundle artifact build commands and pipeline environment
dependencies; use its supported mechanisms rather than implementing a package
installer in Kindling. See [library dependencies](https://docs.databricks.com/aws/en/dev-tools/bundles/library-dependencies).

For promotion, build versioned wheels once, retain their hashes, and deploy
those exact artifacts to each target without rebuilding. Include any generated
adapter wheel in that immutable release set; its version must change when its
registration inputs change. Bundle manifests record the source/config revision,
generator version, and artifact hashes so output can be reproduced and audited.
Verify the chosen
Databricks CLI's local-wheel path rewriting for pipeline environments before
fixing the template. Changed code must receive a new wheel version: the
existing Lakeflow guide records caching of unchanged wheel requirements.

## Configuration contract

Ship the complete non-secret config directory. Each target selects an ordered
file list through `kindling.lakeflow.config_files`: base, applicable platform
and workspace layers, environment, then app overrides. Preserve Kindling's
existing ordering; do not flatten structured entity IDs into Spark keys.
The starter can use base plus environment, expanding only when layers exist.

Conceptual fragment within the pipeline resource:

```yaml
configuration:
  kindling.data_app: orders
  kindling.lakeflow.allowed_apps: orders
  kindling.lakeflow.config_files: >-
    ${workspace.file_path}/config/settings.yaml,${workspace.file_path}/config/settings.${bundle.target}.yaml
```

This illustrates the default target-name-to-environment mapping. Permit an
explicit mapping when target and environment names differ. Selecting a file
list is transport wiring; its contents remain the source of runtime behavior.
Do not assume the bundle target automatically sets Kindling's environment.

Use pipeline catalog/schema for managed outputs. Preserve external table
overrides and the [dataset naming contract](../contributing/databricks_execution_contract.md).
Do not inject platform initialization settings merely because the bundle runs
on Databricks: the selector intentionally defaults declaration-time platform
services to `standalone`. Additional scalar settings needed on restricted
runtimes must be named in `kindling.lakeflow.config_keys`.

Workspace-file config reads in Lakeflow serverless remain unverified in the
existing guide. Make an end-to-end readability test a release gate for this
template. If it fails, use the documented UC-volume config location and define
an explicit upload step before deploying the pipeline; ordinary bundle file
sync must not be assumed to upload config to a volume.

Use isolated paths and destinations for individual development targets, and
stable solution/target paths for shared deployments. Retain stable bundle
resource keys and deployment state across releases. Secret values stay out of
synced files; preserve secret references and the appropriate runtime identity.

## Proposed CLI experience

Add a `kindling bundle` group for assembly, initially supporting Databricks:

```bash
# Proposed commands, not available today:
kindling bundle build --platform databricks --target dev --env dev
# Remaining deployment inputs supplied through KINDLING_BUNDLE_* variables.

# Run in the generated bundle directory after reviewing the output:
cd dist/bundles/databricks
databricks bundle validate -t dev
databricks bundle deploy -t dev
databricks bundle run -t dev orders_pipeline
```

Build resolves apps, package artifacts, and configuration using existing
Kindling conventions. Here `build` means assemble the deployment output; it
does not mean rebuild domain wheels. An explicit app set must be stable for a
logical bundle: reject accidental omission of previously managed apps rather
than treating a partial selection as permission to delete pipeline resources.
Allow an explicit output directory and multiple environment selections for CI.
Same inputs and generator version should produce the same file contents.

The Databricks CLI owns authentication, resource state, deployment, and runs.
Do not add a second implementation of those operations to Kindling SDK.
Convenience `kindling bundle validate/deploy/run` wrappers can follow if useful;
they should call the installed Databricks CLI and preserve its diagnostics and
exit status. They are not necessary for the first version. Existing
`kindling app deploy/run` semantics stay explicit and unchanged.

Databricks documents the native validate/deploy/run workflow for pipelines in
its [pipeline bundle tutorial](https://docs.databricks.com/aws/en/dev-tools/bundles/pipelines-tutorial).

## Delivery and validation

### Required implementation components

The disposable-output workflow requires the following work. These are proposed
implementation boundaries, not claims that the capabilities already exist.

| Component | Existing foundation | Required change / completion evidence |
| --- | --- | --- |
| Project/app inventory | CLI `_resolve_by_convention`, app metadata, `lake-reqs.txt` | Spark-free inventory of the complete deployed app set, package roots, and stable app IDs; report ambiguity instead of selecting the first match. |
| Configuration resolver | CLI `_load_effective_raw_config` loads `settings.yaml`, `settings.<platform>.yaml`, `settings.<env>.yaml` | Extract a reusable design-time resolver; explicitly support shared config and app overrides, return ordered runtime source paths, and test parity with runtime overlays. The existing helper does not resolve the full workspace hierarchy. |
| Deployment input resolver | CLI option/environment conventions | New typed CLI/environment-only input contract; no runtime settings lookup, Spark bootstrap, or GitHub API dependency. Test precedence, missing inputs, and target/runtime-environment separation. |
| Artifact resolver | Package builds; SDK `ArtifactStore` supports local, ABFSS, and UC-volume files | Resolve requirement constraints to exact wheel metadata and hashes, including transitive dependencies and target Python compatibility; consume retained build provenance. Storage access alone is not dependency resolution. |
| Declaration registration API | Bootstrap `_import_local_package_registrations` and CLI requirement-name parser | Extract a supported API that imports only explicitly selected registration namespaces after initialization, with no ambient local-package environment expansion or dependency installation. Preserve bootstrap behavior for existing callers. |
| Adapter wheel builder | Lakeflow `spark_kindling.data_apps` selector contract | Generate metadata and `register_all()` calling that API; reuse explicit existing app entry points; reject duplicate app entry points and unresolved import roots. |
| Bundle renderer | Selector source and documented config-file transport | New CLI module renders YAML, source wrapper, staged files, and provenance manifest from validated inputs. Output has no references to files outside the generated root. |
| Deployment integration | Native Databricks CLI; SDK volume upload support | Pin/test CLI compatibility, validate generated output, document auth/run identity prerequisites, and wire any required volume staging step. Preserve native diagnostics. |
| Lifecycle and CI | Existing poe tests and package builds | Test clean regeneration, immutable promotion, stable remote identity, app-set deletion protection, and real serverless execution. |

Keep Click handlers thin; place the new inventory/config/artifact/rendering
logic in modules under `packages/kindling_cli/kindling_cli/`. Runtime changes
are limited to the supported registration API and its bootstrap integration.
The generated adapter depends on runtime core, never on CLI or SDK.

### Deployment inputs: separate from runtime settings

Deployment parameters belong to the delivery system. GitHub variables are one
supported source: a workflow maps repository/environment variables into the
process environment. Other CI systems and local shells use the same contract.
Kindling does not fetch GitHub variables itself or depend on GitHub APIs.

Proposed input interface (not implemented):

| CLI option | Environment variable | Meaning |
| --- | --- | --- |
| `--name` | `KINDLING_BUNDLE_NAME` | Stable logical solution identity. |
| `--target` | `KINDLING_BUNDLE_TARGET` | Deployment target, such as `dev`. |
| repeated `--app` | `KINDLING_BUNDLE_APPS` | Complete managed app set; environment form is a JSON string array. |
| `--workspace-host` | `KINDLING_BUNDLE_WORKSPACE_HOST` | Destination workspace; authentication remains native Databricks CLI configuration. |
| `--workspace-root` | `KINDLING_BUNDLE_WORKSPACE_ROOT` | Stable remote root for bundle files/artifacts/state. |
| `--run-as-service-principal` | `KINDLING_BUNDLE_RUN_AS_SERVICE_PRINCIPAL` | Runtime execution identity, not an authentication credential. |
| `--catalog`, `--schema` | `KINDLING_BUNDLE_CATALOG`, `KINDLING_BUNDLE_SCHEMA` | Default managed pipeline destinations. |
| `--continuous` / `--no-continuous` | `KINDLING_BUNDLE_CONTINUOUS` | Pipeline update mode; defaults to false. |
| `--env` | `KINDLING_BUNDLE_RUNTIME_ENV` | Existing Kindling runtime overlay selection; defaults to target. |
| `--app-options-json` | `KINDLING_BUNDLE_APP_OPTIONS` | Optional per-app deployment overrides keyed by app name, limited initially to catalog, schema, and continuous mode. |
| `--permissions-json` | `KINDLING_BUNDLE_PERMISSIONS` | Optional typed principal/permission entries for generated resources. |

Explicit options override corresponding environment values, then documented
defaults apply. Collections replace rather than merge. Parse booleans strictly;
validate JSON shape, unknown app names, required fields, and unsupported keys.
Do not use Dynaconf runtime `KINDLING_*` resolution to load these inputs. Missing
values name the relevant CLI option/environment variable, not a settings file.
Avoid passing the deployment environment into runtime pipeline configuration.

For example, CI could supply the following process environment from its
maintained variables (illustrative resolved values, not a GitHub workflow):

```text
KINDLING_BUNDLE_NAME=sales-domain
KINDLING_BUNDLE_TARGET=dev
KINDLING_BUNDLE_APPS=["orders"]
KINDLING_BUNDLE_WORKSPACE_HOST=https://example.azuredatabricks.net
KINDLING_BUNDLE_WORKSPACE_ROOT=/Workspace/Shared/kindling/sales-domain/dev
KINDLING_BUNDLE_RUN_AS_SERVICE_PRINCIPAL=example-principal
KINDLING_BUNDLE_CATALOG=dev_sales
KINDLING_BUNDLE_SCHEMA=orders
```

The first implementation supports serverless pipelines only. Unsupported compute
choices fail explicitly. Development may use the invoking identity; shared
production requires an explicit execution identity. Credentials remain in the
Databricks authentication mechanism, sourced from CI secrets or federation as
appropriate; no credentials enter generated YAML or provenance manifests.

Record the effective non-secret deployment inputs in the generated manifest,
alongside runtime-config revision and artifact hashes. Reproduction needs this
snapshot because CI variables can change independently of Git. Retain release
manifests in CI artifact storage; they are generated records, not another
hand-maintained configuration file.

Pipeline destination catalog/schema come from deployment inputs. External
entity references and runtime execution options remain in runtime settings.
When those references must align with deployed destinations, validate the
alignment; do not rewrite maintained runtime overlays or silently treat one
source as the other.

### Runtime configuration staging and app-set safety

Use project-level `config/` when present (or explicit `--config`), then app-local
settings, with base/platform/environment order within each scope. Shared
workspace overlays require an explicit resolver contract and parity tests
before being advertised. Exclude `settings.local.*`, credentials, and caches
from deployment. The earlier config fragment is illustrative: actual file
lists come from this resolver, including distinct per-app paths. Copy raw
runtime YAML with secret references intact; never serialize a secret-resolved
configuration tree into the bundle. Deployment inputs are resolved separately without starting Spark or importing
app code; runtime settings are never a fallback for missing deployment values.

The effective app list from CLI/environment is the complete managed set;
`--app` is not a partial deployment filter. Reducing that set requires an explicit
resource-removal workflow with a deployment diff. A regenerated directory has
no prior local history, so comparison must use the retained release manifest or
remote deployment state. Reject an attempted removal when that comparison is
unavailable. This prevents disposable local output from bypassing deletion
protection.

### Registration and wheel resolution details

The current requirement-name helper normalizes distribution names to import
names, which is valid for scaffolded Kindling packages but not arbitrary Python
distributions. Resolve scaffolded roots from project/wheel metadata; require an
explicit registration entry point for ambiguous external packages. Do not
generate imports by blindly replacing hyphens with underscores.

The current walker also includes `KINDLING_LOCAL_PACKAGE_MODULES` and tolerates
missing namespaces. The new explicit registration API must avoid ambient
package discovery, distinguish missing required packages from optional empty
namespaces, and propagate errors inside registration modules. Initialization
must occur before registration, and Kindling config overrides must apply to
the registered declarations before `declare_pipeline()` emits the graph.
Test equivalent entity/pipe selection for the same package-only app through
local registration and the generated adapter.

Python imports can have arbitrary side effects; static inspection cannot prove
an app is declaration-only. Default adapter support to package-only apps with
the documented registration contract. Apps with custom imperative entrypoints
must explicitly supply a declaration entry point rather than silently losing
their `app.py` behavior. Verify custom declaration modules in isolated tests.

Resolve all wheels before writing final output. Reject incompatible constraints,
multiple candidates without an exact release selection, missing dependencies,
and target-incompatible Python requirements. Stage runtime dependencies only;
do not remove dependencies from an existing wheel to hide its incompatible
`standalone` extras. Correct the maintained package metadata or supply a
compatible existing build. Broad extension version ranges must be resolved to
exact artifacts for the release.

A wheel filename/version cannot establish source freshness. Require build
provenance with source revision/input hashes for reproducible CI releases;
without provenance, report freshness as unknown rather than claiming a stale
artifact check passed. The generated adapter uses deterministic contents and a
version derived from its registration inputs and generator version. Retain its
wheel with the release so environment promotion consumes identical bytes.

### Platform prerequisites and acceptance gates

Provision the workspace, catalog/schema, deployment identity permissions,
pipeline execution privileges, and any required artifact volume outside the
bundle generator. Confirm the runtime can access pinned dependency artifacts
and config/secret references. Bundle generation itself is offline when all
inputs are local; deployment requires configured Databricks authentication.

The release gate must demonstrate all of the following with a domain fixture:

- Build a bundle without Spark, app imports, or new maintained domain files.
- Supply deployment values solely through CLI/environment; prove runtime
  settings cannot override them and that deployment variables are not emitted
  as runtime configuration. Test a target whose runtime environment differs.
- Delete output and regenerate identical content from the same inputs.
- Validate and run on serverless using existing domain wheels plus the adapter.
- Read structured per-app/environment config and preserve dotted entity IDs.
- Load changed versioned wheels and promote the same bytes to another target.
- Redeploy from a different local directory without creating a new pipeline ID.
- Reject incomplete app sets, invalid config, dependency conflicts, and missing
  required artifacts before deployment.

Serverless workspace-file readability and pipeline wheel path rewriting remain
empirical gates. If volume transport is needed, implement its explicit staging
step using `VolumesArtifactStore`, with release-specific config paths and
permissions checked before pipeline deployment. Do not call the feature ready
while either transport path remains untested.

### Implementation order

Resolve the registration API, separate deployment input contract, and runtime
config staging contract first. Build artifact
resolution and the adapter against those contracts, then implement rendering
and CLI assembly. Finish with native CLI validation, platform transport probes,
and regeneration/promotion tests. The components above define the implementation
scope; task status belongs in beads rather than this proposal.

First generate a reference bundle from an existing domain-project fixture using
the existing selector. Framework-owned templates and test expectations can be
versioned in Kindling; domain projects need no maintained bundle files.
Exercise wheel installation, structured overlays, app
discovery, and declaration in a real serverless pipeline. Pin a tested
Databricks CLI version/range; choose it from those results rather than assuming
all current bundle features are available in older CLIs.

Then implement the generator and generated registration adapter. Test convention
lookup, entry-point metadata, multiple apps/packages, missing/stale artifacts,
deterministic output, and preservation of existing user files. Validate generated
YAML using the pinned Databricks CLI. Run the relevant poe unit tasks and
formatting for implementation changes.

Finally document CI promotion of the same wheels and config revision across
targets. Delete all local generated output, regenerate it in a different
directory, and verify that redeploy still updates the same pipeline ID.
Derive remote state paths, bundle identity, and resource keys from stable
project/target/app identities, never temporary paths, wheel versions, or build
timestamps. Remote deployment state, pipeline state, and checkpoints are
durable even though local bundle output is disposable. Verify dev/prod tables
remain isolated, and a changed wheel is actually loaded. Deployment should not
implicitly start a pipeline update. Keep infrastructure provisioning in IaC;
do not let IaC and bundles both manage the same pipeline. Existing pipelines
need a deliberate ownership migration before adopting bundles.

Bundle-backed classic jobs and convenience command wrappers are follow-up
scope. A classic job adapter must call a supported Kindling runner entrypoint;
it cannot reuse Lakeflow's declaration-only source.
