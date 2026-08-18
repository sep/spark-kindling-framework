# Domain Devcontainer Contract

**Date:** 2026-08-18
**Status:** Partially implemented — see Implementation Update
**Scope:** The published Kindling devcontainer used to develop separate domain
projects. This does not define the contributor container for the
`spark-kindling` framework repository or the images used by Kindling CI.

---

## Implementation Update (2026-08-18)

The core problem this proposal targets — project state, not container state,
must be the authority for which Kindling release is in use — is implemented,
but via a simpler mechanism than the "immutable wheelhouse + manifest.json"
design below. That mechanism turned out to be unnecessary: Poetry already has
a native way to pin a dependency to an exact, content-hashed artifact (a
`url` source dependency), and `poetry.lock` already records its SHA-256 hash.
Building a parallel `manifest.json`/wheelhouse system to provide the same
guarantee was redundant once that was recognized.

**What shipped:**

- `kindling repo init`/`kindling package init` generate Kindling dependencies
  pinned directly to a release wheel URL (`spark-kindling = { url =
  "https://github.com/sep/spark-kindling-framework/releases/download/v<version>/
  spark_kindling-<version>-py3-none-any.whl", extras = [...] }`) instead of an
  open-ended `>=<version>` constraint against a local Poetry source. No
  `[[tool.poetry.source]]` block, no `/opt/kindling-packages`, no local PEP
  503 index — `poetry.lock`'s recorded URL and hash are the entire
  reproducibility contract.
- `kindling env update` re-points every `spark-kindling`/`spark-kindling-*`
  dependency already declared in a project (framework, SDK, CLI, and any
  extensions) at its matching wheel in a target release, in one step, via
  `poetry add <release wheel URL>`. It never mutates `/opt` (there is no
  `/opt` to mutate) and never runs `poetry update`.
- `kindling env add <package>` adds a single Kindling package the same way,
  preserving an existing dependency's group/extras when re-adding it (Poetry
  does not preserve either automatically).
- `kindling env bootstrap` is the new `postCreateCommand`: it checks whether
  the project declares any Kindling dependency at all, adds the framework/SDK/
  CLI (pinned to latest) only if none is declared, then always runs
  `poetry install --sync`. It never upgrades an already-declared dependency —
  satisfying "container creation installs; it does not upgrade" without
  needing a separate migration command for projects that predate this model
  (an old `>=`/local-index-style declaration is a declared dependency, so
  `bootstrap` leaves it alone; running `env update` on it converts it to a
  URL dependency on its next explicit upgrade, which doubles as the described
  migration path without a dedicated `env migrate` command existing yet).
- The devcontainer image no longer builds or installs the Kindling
  framework/SDK from source at all (previously `poetry build` against
  whatever commit triggered the image build, then `pip install`ed into
  system Python — the actual root cause of a live bug: that build only
  matches a real, fetchable GitHub release when built from a release tag,
  and a `workflow_dispatch` dev build is not, so every project scaffolded
  against that image would 404 on `poetry install`). The image now installs
  only the Kindling CLI, resolved from the *latest published GitHub release*
  at build time — never built from source — so `kindling env bootstrap` is
  always available regardless of what commit produced the image.

**Where this intentionally diverges from the design below:**

- **No wheelhouse, no `manifest.json`, no `kindling-release` Poetry source.**
  The entire "Coherent release wheelhouse" section is superseded — GitHub
  Releases already is the coherent, checksummed, immutable artifact store;
  `poetry.lock` already records which artifact and hash a project uses.
  Nothing in `/opt` needs to describe the image's Kindling release because
  the image no longer has one.
- **The devcontainer image is no longer version-coupled to a Kindling
  release at all.** It still publishes `:latest` (not a Kindling version
  tag), because the image now only provides OS/Java/Python/Poetry/JARs/the
  bootstrap CLI — none of which are Kindling-release-specific. The
  `devcontainer:<kindling-version>` tagging scheme, the `kindling --version`
  "SDK image" / "Image wheelhouse" reporting lines, and the acceptance
  criterion requiring a non-`latest` image reference do not apply under this
  simplification and should be considered superseded rather than pending.
- **`env add`/`env update` resolve directly against GitHub releases**, not
  against "the release manifest selected by the project image" — there is no
  project image manifest to select from. `--version` defaults to `latest`
  and can target any specific release explicitly; nothing scopes it to a
  project's currently-pinned version, since GitHub Releases (not the image)
  is the single source of truth for what versions exist.

**Not implemented / still open:**

- `kindling env check`'s image/wheelhouse contract reporting (moot for the
  wheelhouse parts; a project/CLI version-alignment report is still useful
  and not yet built).
- `kindling env use-candidate` / `clear-candidate` for testing unreleased
  builds.
- A dedicated `kindling env migrate --to-pinned-sdk` command (see the
  `bootstrap`/`update` note above for why this may not be needed).
- Generated CI template changes, SBOM/provenance/digest-scanning for the
  image, and refreshing generated agent-reference material as part of
  `env update`.
- Image tag/digest pinning in generated `devcontainer.json` — moot for
  Kindling-version purposes per the above, but the underlying question of
  pinning the *tooling* image (Java/Python/Poetry versions) for
  reproducibility is still open and unaddressed.

---

## Executive Summary

Kindling should continue to publish a domain-development container. Spark,
Java, Delta, Azure authentication, Hadoop Azure integration, and the Kindling
toolchain create enough setup friction that a prebuilt environment is a useful
part of the product.

The container must, however, become a **versioned development SDK image**, not
a mutable package manager. The checked-out project must be the authority for
its Python dependencies. A project commit must answer, without consulting a
developer's existing container, which Kindling release and development image
it uses.

The target contract is:

```text
versioned domain SDK image
  owns: OS, Python, Java, Spark, Delta, cloud CLIs, Hadoop JARs,
        bootstrap Kindling CLI, release wheelhouse
                         |
                         v
checked-in domain project
  owns: image tag/digest, pyproject constraints, poetry.lock,
        declared extensions and application dependencies
                         |
                         v
project Poetry environment
  owns: the authoritative kindling/CLI/SDK imports and commands
```

The release wheelhouse in the image is immutable and contains one coherent
Kindling release set. It is an installation source, not durable mutable state.
Opening a project installs its committed lockfile; it never performs an
implicit dependency update. Upgrading Kindling is an explicit repository
mutation that updates the image reference, dependency constraints, lockfile,
and generated Kindling reference material together.

## Context

Today `kindling repo init` scaffolds a domain repository whose devcontainer
uses:

```json
"image": "ghcr.io/sep/spark-kindling-framework/devcontainer:latest"
```

The published image contains:

- Python 3.11 and Java 21;
- the local PySpark 3.5 and Delta runtime selected by
  `spark-kindling[standalone]`;
- Poetry and Poe;
- Kindling runtime, CLI, and SDK wheels installed into system Python;
- those wheels again under `/opt/kindling-packages/wheels/`;
- a PEP 503 index under `/opt/kindling-packages/simple/`;
- Hadoop Azure JARs under `/opt/hadoop-jars/`;
- Kindling's agent reference document; and
- editor-facing defaults supplied by the scaffolded `devcontainer.json`.

A generated package adds the image-local index as a supplemental Poetry
source and declares open-ended Kindling dependencies such as:

```toml
spark-kindling = { version = ">=0.12.29", extras = ["standalone"] }
spark-kindling-cli = ">=0.12.29"
spark-kindling-sdk = ">=0.12.29"
```

The generated post-create command may run `poetry update` before
`poetry install`. `kindling env update` can later download another release,
copy its wheels into `/opt`, rebuild the local index, upgrade the system
Python installation, update the Poetry project, and synchronize its virtual
environment.

This is convenient but leaves ownership ambiguous. The image tag, global
installation, wheel cache, `pyproject.toml`, and `poetry.lock` may each name
or resolve a different Kindling release.

## Goals

1. A fresh clone of a domain project produces the same Kindling environment
   for every developer and in CI.
2. Opening or rebuilding a devcontainer never upgrades project dependencies
   implicitly.
3. The project repository, not mutable container state, records the selected
   Kindling release.
4. A developer can upgrade Kindling with one explicit command and review the
   complete change in Git.
5. Domain developers retain a low-friction environment containing a verified
   local Spark/Delta/Azure toolchain.
6. Kindling extensions are ordinary, explicit project dependencies.
7. The bootstrap CLI remains available before the project environment exists,
   while project commands use the project environment afterward.
8. Projects can reproduce the same environment in CI without depending on a
   developer's modified container filesystem.
9. Published images remain multi-architecture, auditable, and safe to update.

## Non-Goals

- Reproducing Databricks Runtime, Fabric Runtime, Synapse Runtime, or
  Lakeflow serverless locally.
- Installing every Kindling extension in every domain environment.
- Combining PySpark 3.5 and the PySpark 4.1 SDP runtime in one Python
  environment.
- Replacing cloud system tests with container tests.
- Defining the framework-contributor or CI image in detail.
- Making the domain image a general-purpose private Python package registry.

## Design Principles

### Project state is authoritative

The selected image, Kindling dependencies, and resolved transitive
dependencies must all be represented in committed project files. Destroying
and recreating the container must preserve the environment contract.

### Container creation installs; it does not upgrade

`postCreateCommand` may install a lockfile, configure editor integration, and
install hooks. It must not run `poetry update`, rewrite dependency constraints,
or select a newer Kindling release.

### Image content is immutable

Files baked into `/opt` describe the image. Normal project commands must not
modify them with `sudo`. A changed release requires a changed project image
reference or an explicit project-local candidate workflow.

### One authoritative project interpreter

The image may contain a global bootstrap CLI, but application imports, tests,
Poe tasks, and routine Kindling commands must use the Poetry environment.

### Explicit incompatibility is better than a universal environment

Spark 3.5/Delta development and Spark 4.1 SDP validation require separate
Python environments. The container should make both workflows easy without
pretending they are one dependency set.

## Target Architecture

### Published domain SDK image

Publish:

```text
ghcr.io/sep/spark-kindling-framework/devcontainer:<kindling-version>
ghcr.io/sep/spark-kindling-framework/devcontainer:<kindling-version>-<revision>
ghcr.io/sep/spark-kindling-framework/devcontainer@sha256:<digest>
```

The first tag identifies the Kindling release. The optional image revision
allows OS or toolchain security fixes without publishing a new Python package
release. Release metadata must identify both values.

`latest` may remain as a human convenience tag, but generated projects and CI
must never commit it.

The image owns:

- Python 3.11;
- Java 21;
- one supported local PySpark 3.5/Delta combination;
- Poetry and Poe, pinned;
- Azure CLI and other supported cloud client tooling;
- Git and the GitHub CLI where needed by Kindling commands;
- Hadoop Azure JARs and the Kindling ABFSS token-provider JAR;
- the Kindling agent reference document;
- a bootstrap `kindling` command; and
- an immutable wheelhouse and simple index for the matching Kindling release.

The image does not own:

- the domain project's lockfile;
- domain packages or applications;
- optional Kindling extensions not selected by the project;
- credentials or `.env` files;
- mutable release history in `/opt`; or
- an authoritative global runtime for project tests.

### Coherent release wheelhouse

`/opt/kindling-packages` contains exactly one coherent release set:

```text
/opt/kindling-packages/
  manifest.json
  wheels/
    spark_kindling-<version>-py3-none-any.whl
    spark_kindling_cli-<version>-py3-none-any.whl
    spark_kindling_sdk-<version>-py3-none-any.whl
    spark_kindling_ext_<name>-<extension-version>-py3-none-any.whl
  simple/
    ... PEP 503 index ...
```

`manifest.json` records:

- the Kindling release;
- all wheel filenames, package versions, and SHA-256 digests;
- the source Git commit;
- the image revision;
- Python, Java, Spark, and Delta versions; and
- image build time and supported architectures.

The wheelhouse may contain all extension wheels released with Kindling so a
project can explicitly install one without fetching a second artifact set.
Their presence is not installation or activation. An extension becomes part
of a project only when declared in `pyproject.toml` and locked.

The directory is root-owned and read-only during ordinary development.

### Scaffolded project contract

`kindling repo init` writes a versioned image reference:

```json
{
  "name": "Kindling Domain Development",
  "image": "ghcr.io/sep/spark-kindling-framework/devcontainer:0.12.29",
  "workspaceFolder": "/workspaces/my-domain"
}
```

Where operational policy requires complete immutability, the generated file
may include the digest or an adjacent update manifest may record it. The
version tag remains visible for humans.

Generated package dependencies use exact Kindling release versions:

```toml
[[tool.poetry.source]]
name = "kindling-release"
url = "file:///opt/kindling-packages/simple/"
priority = "supplemental"

[tool.poetry.dependencies]
python = "^3.10"
spark-kindling = { version = "0.12.29", extras = ["standalone"] }

[tool.poetry.group.dev.dependencies]
spark-kindling-cli = "0.12.29"
spark-kindling-sdk = "0.12.29"
```

Exact direct constraints make the intended release reviewable. The lockfile
remains the authority for transitive versions.

The source name describes what it is: a release wheelhouse, not a mutable
local cache. The source path is acceptable only because all supported local
and CI workflows use the selected SDK image. A future authenticated package
feed can replace it without changing the ownership model.

### Lockfile policy

Every generated domain package commits `poetry.lock`. If a repository has
multiple independently versioned packages, each package commits its own
lockfile unless the repository adopts a documented shared-environment model.

A fresh container runs only installation commands:

```text
poetry install --with dev --sync
pre-commit install
```

If the lockfile and image wheelhouse disagree, creation fails with an
actionable diagnostic. It must not silently update the lockfile or fall back
to another Kindling release.

### Bootstrap CLI versus project CLI

The image includes a global bootstrap CLI so these operations work before a
project exists:

- `kindling repo init`;
- `kindling package init`;
- environment diagnostics; and
- the explicit Kindling upgrade command.

Once a Poetry project exists, project documentation and generated Poe tasks
invoke:

```text
poetry run kindling ...
poetry run poe ...
```

VS Code selects the project's Poetry interpreter rather than
`/usr/local/bin/python`. A shell convenience wrapper may delegate `kindling`
to `poetry run kindling` when the current directory belongs to a Poetry
project, but the delegation must be visible through `kindling --version` and
must not obscure version mismatches.

`kindling --version` should report at least:

```text
CLI:              0.12.29 (project Poetry environment)
Runtime:          0.12.29
SDK:              0.12.29
SDK image:        0.12.29-r1
Image wheelhouse: 0.12.29
```

An environment check fails when the project and image release sets are
incompatible.

## Upgrade Workflow

### Released upgrade

The durable upgrade command is project-oriented:

```text
kindling env update --version 0.12.30
```

It performs a preflight, then updates as one logical transaction:

1. Resolve the requested GitHub release and its published domain image.
2. Verify that the image and release wheel manifest exist and agree.
3. Update the committed devcontainer image tag/digest.
4. Update exact Kindling runtime, CLI, and SDK constraints.
5. Update explicitly declared Kindling extension constraints to versions in
   the target release manifest, subject to compatibility policy.
6. Resolve and write `poetry.lock` against the target release wheelhouse.
7. Refresh generated Kindling agent/reference files.
8. Print a summary of changed packages, runtime versions, migrations, and
   required container rebuild.

The command must not overwrite unrelated `pyproject.toml` content or edit
application dependency constraints.

Because the target wheelhouse is in a different image, lock resolution needs
one of these implementation mechanisms:

1. download the target release wheel manifest and wheels into a temporary
   project-local index;
2. run resolution inside a temporary container based on the target image; or
3. resolve from a future authenticated remote package feed.

Option 1 is the smallest transition from the current implementation. Its
temporary index is deleted after the lockfile is written. Nothing is copied
into the current image's `/opt` tree.

After a successful upgrade, the developer commits the changed project files
and rebuilds the devcontainer. Until rebuilt, diagnostics clearly report that
the running image is the previous release.

### Extension installation

Installing an extension is also a project mutation:

```text
kindling env add spark-kindling-ext-databricks
```

The command:

1. reads the release manifest selected by the project image;
2. confirms that the extension belongs to that release set;
3. adds the exact extension version to the appropriate dependency group;
4. updates the lockfile; and
5. does not install or cache another release globally.

An explicit `--version` is permitted only when the extension manifest declares
compatibility with the project's Kindling release. Otherwise the command
fails rather than constructing an untested release mixture.

### Candidate framework testing

Testing an unreleased Kindling build against a domain project is a separate,
explicit workflow. For example:

```text
kindling env use-candidate /path/to/dist
kindling env clear-candidate
```

Candidate wheels are copied into a project-local ignored directory such as
`.kindling/candidate-wheels/`, and Poetry is given a temporary or clearly
marked path override. Candidate mode must be reported by `kindling --version`
and `kindling env check`.

Candidate mode never mutates `/opt`, never changes the base image tag, and is
not presented as a durable released upgrade. A separate promotion command may
replace it with a released version later.

## Spark and SDP Runtime Policy

The main domain environment uses the Spark 3.5/Delta combination supported by
Kindling's standalone runner. It must not also install PySpark 4.1.

OSS Spark Declarative Pipelines validation remains isolated:

```text
poetry run poe sdp-runtime
poetry run poe test-sdp-dryrun
```

The SDK image may cache the Spark 4.1 wheelhouse or pre-provision an isolated
environment to reduce setup time. That environment has its own manifest and
does not appear on the default interpreter path.

Databricks Lakeflow, Fabric, and Synapse behavior continues to require real
platform system tests. The domain SDK image supplies client tooling and local
contract tests, not a simulated managed runtime.

## CI Contract

Generated CI uses the same versioned domain SDK image as the project. It must
not use `latest` and must not reconstruct a different Kindling package source.

CI performs:

```text
poetry install --with dev --sync
poetry run poe test
poetry run poe build
```

Before installation, CI verifies:

- the image manifest is present;
- its Kindling release matches the project's declared release;
- wheel digests match the manifest;
- the lockfile is current; and
- no candidate override is active.

Cloud deployment and system-test jobs may use specialized CI images, but
their Kindling wheel inputs must come from the same committed lock/release
selection or from the candidate artifacts under test.

## Security and Supply Chain

Published domain images must:

- build from a pinned base-image digest;
- pin globally installed tooling versions;
- verify every downloaded JAR and wheel digest;
- generate an SBOM;
- undergo image and dependency vulnerability scanning;
- publish provenance tying the image to a source commit and release;
- support `linux/amd64` and `linux/arm64` where the underlying tools do;
- contain no credentials, tokens, `.env` files, or authenticated package
  configuration; and
- run project work as the non-root `vscode` user.

Security-only rebuilds increment the image revision. Projects can update from
`0.12.29-r1` to `0.12.29-r2` without changing their Kindling dependency
versions.

The wheelhouse and Hadoop JAR directories are read-only at runtime. Commands
requiring `sudo` to update image-owned package state are removed from the
normal domain workflow.

## Failure Behavior

The system should fail early and explain the mismatch when:

- a project declares Kindling 0.12.30 inside a 0.12.29 image;
- the committed lockfile requests a wheel absent from the image release set;
- an extension is not compatible with the selected Kindling release;
- a generated project still uses `latest` in strict mode;
- the image manifest or a wheel digest is invalid;
- `postCreateCommand` finds no lockfile;
- the selected VS Code interpreter is the system interpreter instead of the
  project environment; or
- a candidate override is active in CI.

Diagnostics must give the exact corrective action: rebuild, run the upgrade
command, restore the lockfile, select a compatible extension, or clear
candidate mode. They must never repair these mismatches by silently running
`poetry update`.

## CLI Changes

### `kindling env check`

Add a domain SDK contract section reporting:

- running image release/revision;
- wheelhouse release and manifest validity;
- project Kindling constraint and locked version;
- active CLI/runtime/SDK versions and installation origin;
- selected Python interpreter;
- whether candidate mode is active; and
- whether a rebuild is pending after an upgrade.

### `kindling env update`

Change from “mutate `/opt`, global Python, and the current project” to
“update the project's declared SDK release.” Retain an explicit legacy mode
only for a bounded migration period if existing projects require it.

Suggested options:

```text
kindling env update --version <release>
                    [--project <path>]
                    [--dry-run]
                    [--no-agent-refresh]
```

`--dry-run` prints image, package, extension, and lockfile changes without
writing.

### `kindling env add`

Resolve extensions from the project's selected release manifest and write an
exact constraint. Do not refresh `/opt` and do not default to an independent
`latest` release.

### `kindling env migrate`

Provide a one-time command for projects generated under the mutable model:

```text
kindling env migrate --to-pinned-sdk
```

It:

- resolves the project's currently locked Kindling release;
- replaces `devcontainer:latest` with the matching versioned image;
- replaces open-ended Kindling constraints with exact constraints;
- renames or updates the local package source declaration;
- writes or refreshes the lockfile;
- updates generated post-create behavior; and
- reports any incompatible extension mix for manual resolution.

## Scaffold Changes

`kindling repo init` and `kindling package init` must generate:

1. a release-pinned devcontainer image;
2. installation-only `postCreateCommand` behavior;
3. exact Kindling direct dependencies;
4. a committed lockfile or a deterministic bootstrap step that creates it
   before the initial commit;
5. generated CI using the same image reference;
6. VS Code configuration selecting the Poetry interpreter;
7. Poe tasks that invoke project-local commands; and
8. an explicit Kindling upgrade task.

The post-create health check imports `kindling`, not the distribution name
`spark_kindling`. More importantly, a failed import produces a clear install
failure; it never triggers an automatic dependency update.

## Documentation Changes

The domain quickstart, setup guide, CLI reference, and generated repository
README must consistently state:

- Java 21 and PySpark 3.5 are the current primary local runtime;
- the image is pinned per project;
- `poetry.lock` is authoritative;
- container creation installs but does not upgrade;
- upgrades change committed files and require a rebuild;
- project commands run through Poetry;
- extension installation is explicit; and
- Spark 4.1 SDP validation uses an isolated environment.

Examples must use repository-sanctioned Poe tasks where they exist.

## Migration Plan

### Phase 1: Make current state observable

- Add the image/wheelhouse manifest.
- Extend `kindling --version` and `kindling env check` with installation
  origin and version-alignment reporting.
- Fix the generated import check.
- Correct Java/Spark and command documentation drift.
- Test the published image as a release artifact.

No existing update behavior changes in this phase.

### Phase 2: Pin new projects

- Scaffold versioned image tags.
- Generate exact Kindling constraints.
- Generate installation-only post-create behavior.
- Commit a valid lockfile in scaffold acceptance fixtures.
- Pin generated CI to the same image.

Existing repositories continue to work under the old contract and receive a
migration warning.

### Phase 3: Project-oriented upgrades

- Implement transactional `env update --version` behavior.
- Change `env add` to use the selected release manifest.
- Add dry-run and rollback-safe file writes.
- Add candidate-wheel commands.
- Add `env migrate --to-pinned-sdk`.

### Phase 4: Retire mutable image state

- Make `/opt/kindling-packages` read-only.
- Remove global reinstall and `/opt` refresh from default `env update`.
- Remove automatic `poetry update` from all generated lifecycle commands.
- Deprecate and then remove legacy flags such as `--no-global`,
  `--package-dir`, and `--no-sudo` from the released-upgrade path.

### Phase 5: Optional remote package feed

Evaluate replacing the image-local PEP 503 source with an authenticated
remote feed. This is optional: a single-release immutable image wheelhouse is
coherent if local and CI workflows use the same image. A remote feed becomes
valuable when projects need dependency resolution outside containers or when
the image release cadence becomes operationally expensive.

## Testing Strategy

### Unit tests

- Scaffold emits a versioned image and exact dependency constraints.
- Post-create contains no dependency-update command.
- Upgrade planning detects every file that must change.
- Dry-run performs no writes.
- Upgrade writes are transactional and preserve unrelated TOML/JSON content.
- Extension addition uses the project release manifest.
- Image/project mismatch diagnostics are actionable.
- Legacy-project migration is idempotent.
- Candidate mode never modifies `/opt`.

### Image contract tests

For every published architecture:

- read and validate `manifest.json`;
- verify wheel and JAR checksums;
- run `java -version`, Spark startup, and a small Delta round trip;
- create a generated domain project;
- install its lockfile without network access except for ordinary third-party
  dependencies not intentionally cached;
- run `kindling env check --local`;
- run representative unit and component tests; and
- verify the project CLI/runtime/SDK versions match the manifest.

### Upgrade acceptance test

Starting from a project pinned to release N:

1. run a dry-run upgrade to N+1;
2. confirm the expected change summary;
3. run the real upgrade;
4. rebuild using the new image;
5. install without updating;
6. run tests and build the domain wheel; and
7. confirm a clean second upgrade invocation is a no-op.

### CI parity test

Run the generated CI workflow against a newly scaffolded project and verify it
uses the same image and lockfile as local development.

## Acceptance Criteria

- [ ] ~~A generated domain project commits a non-`latest` SDK image
      reference.~~ Superseded — the image is no longer version-coupled to a
      Kindling release, so there is no Kindling version for its tag to pin.
- [x] The project declares exact Kindling runtime, CLI, and SDK versions
      (as a release wheel URL, not a version constraint).
- [x] The project commits a lockfile that installs without dependency
      resolution changes.
- [x] Opening or rebuilding the container never runs `poetry update`
      (`kindling env bootstrap` only ever runs `poetry add`/`poetry install`).
- [ ] Routine project commands use the Poetry environment. (Unaffected by
      this work; not verified here.)
- [ ] ~~`kindling env check` identifies image, wheelhouse, project, and
      active package versions and their origins.~~ Wheelhouse reporting is
      moot; project/CLI version-alignment reporting is not yet built.
- [x] ~~`/opt/kindling-packages` remains unchanged...~~ Superseded —
      `/opt/kindling-packages` no longer exists.
- [ ] A Kindling upgrade produces reviewable changes to dependency
      constraints and the lockfile (done); generated reference material is
      not refreshed by `env update` yet, and there is no image reference to
      change.
- [x] Adding an extension records it as an exact project dependency
      (resolved directly against GitHub releases, not a project-image
      manifest — see Implementation Update).
- [ ] Candidate framework testing is isolated, visible, reversible, and
      ignored by default in Git. Not implemented.
- [ ] Generated CI uses the same SDK image contract as local development.
      Not implemented.
- [ ] Spark 3.5 and Spark 4.1 remain isolated. (Unaffected by this work; not
      verified here.)
- [ ] ~~Published image manifests, wheels, JARs, and architectures are
      verified automatically.~~ Moot for Kindling wheels (no manifest);
      JAR/architecture verification not implemented.
- [x] Existing mutable-model projects have an idempotent path forward:
      `kindling env bootstrap` (undeclared projects) and `kindling env
      update` (old-style declared projects) both converge a project to the
      current model without a dedicated `env migrate` command.

## Alternatives Considered

### Keep the current mutable `/opt` cache

This preserves fast in-container upgrades, but the image tag no longer
describes the environment and a rebuild silently discards the upgrade. It also
requires global installation, project installation, and wheel-cache state to
remain synchronized. Rejected as the durable project model.

### Bake every dependency and extension into the image

This reduces first-install time but makes undeclared capabilities available,
increases conflicts and image size, and still cannot combine incompatible
Spark runtimes. Rejected for the default image. Shipping extension wheels in
an uninstalled immutable wheelhouse is acceptable.

### Use only PyPI or a private remote feed

This is the cleanest package-distribution model and remains a desirable
future option. It introduces registry publication, authentication, retention,
and availability concerns that are not required to correct the current
ownership problem. Deferred, not rejected.

### Install Kindling only globally

This makes the image authoritative but prevents the domain project from
locking, testing, and building against an explicit dependency set. It also
weakens editor and CI reproducibility. Rejected.

### Rebuild the image for every extension choice

This creates a combinatorial image matrix and turns ordinary project
dependency changes into infrastructure publication. Rejected.

## Open Questions

1. Should generated projects pin only a release tag, or both a readable tag
   and an immutable digest?
2. Should an image security rebuild use `0.12.29-r2`, an OCI annotation plus
   digest, or both?
3. Should all released extension wheels be included in the immutable
   wheelhouse, or should a release manifest point to remotely downloaded,
   checksum-verified extension wheels?
4. Is one lockfile per domain package the long-term repository model, or
   should `repo init` support a shared workspace environment explicitly?
5. Should the bootstrap CLI automatically delegate to the project CLI, or
   should generated documentation always require `poetry run kindling`?
6. How long should legacy mutable `env update` behavior remain available?
7. Should the first implementation resolve target upgrades from a temporary
   downloaded wheel index or by launching the target SDK image?

## Decision Requested

Approve the following product contract:

1. Keep the published domain devcontainer as a supported Kindling developer
   experience.
2. Treat it as a versioned, immutable SDK image.
3. Make the domain project's exact constraints and lockfile authoritative.
4. Make container creation installation-only.
5. Make upgrades explicit, transactional repository changes.
6. Keep extensions explicit and Spark 4.1 isolated.
7. Provide a migration path from the existing `latest` plus mutable `/opt`
   model.
