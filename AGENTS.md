# Agent Guide

Onboarding for coding agents (and humans) working on spark-kindling-framework.
Work from the repository state, the project documentation, and the current git
branch. Keep changes focused, verified, and easy for the next contributor to
pick up.

## What this is

`spark-kindling` is a unified framework for building data apps that run
unchanged across **Databricks, Microsoft Fabric, Azure Synapse, and
standalone Spark**. Core ideas: declaratively registered data entities and
data pipes (transformations), a pluggable entity-provider storage
abstraction, hierarchical Dynaconf configuration, watermarked/incremental and
streaming pipes with SCD1/SCD2 merge sinks, schema convergence via
`kindling migrate`, and packaging/deployment of apps as `.kda` archives.

## Repository layout

- `packages/kindling/` — core runtime framework (import name `kindling`).
- `packages/kindling_cli/` — `spark-kindling-cli`: the `kindling` CLI,
  scaffolding, test runner. Design-time only.
- `packages/kindling_sdk/` — `spark-kindling-sdk`: programmatic platform APIs
  for deploying and running jobs from a dev machine. Design-time only.
- `packages/extensions/kindling_ext_<name>/` — extensions, published as
  `spark-kindling-ext-<name>`.
- `tests/` — `unit/`, `integration/`, `system/{core,extensions}/`,
  `local-project/`; `docs/` — see documentation conventions below.

## Architecture essentials

- **Initialization**: `kindling.initialize(...)` wraps
  `initialize_framework` in `packages/kindling/bootstrap.py`. Idempotent;
  platform is auto-detected (databricks/fabric/synapse/standalone).
- **Platform services**: each platform module self-registers via
  `@PlatformServices.register(...)` and is discovered through the
  `spark_kindling.platforms` entry-point group. Platform differences stay
  behind this service layer plus runtime feature flags — never leak
  platform conditionals into shared code.
- **Entity providers**: storage backends implementing the interfaces in
  `packages/kindling/entity_provider.py`, registered in
  `entity_provider_registry.py`. An entity picks its provider via the
  `provider_type` tag (default `delta`). Built-ins include delta, csv,
  parquet, memory, eventhub, current_view, adx-api. SQL entities use a
  separate declaration path (`@DataEntities.sql_entity(...)`), not a
  `provider_type` tag.
- **Configuration**: dotted `kindling.*` keys via Dynaconf; layered YAML
  overlays (base → platform → workspace → environment → per-app);
  `KINDLING_`-prefixed env vars override; `spark.kindling.*` SparkConf keys
  merge in at init; secrets via the `@secret` loader. Execution options
  belong in `kindling.*` config — parameters are just-in-time overrides.
- **Extensions**: import-time registration (no entry points). Provider
  extensions call `register_provider()` at module scope; engine extensions
  expose `engine_extension()` resolved by `kindling.initialize(engine=...)`.
- **JVM boundary**: never touch `spark._jvm` / `spark._jsc` (enforced by
  `tests/unit/test_architecture_jvm_boundary.py`).
- **SDK note**: the SDK submits Synapse (and inspects Fabric) Spark jobs via
  the platforms' Livy batch APIs. Livy is a design-time submission
  transport, not part of the on-cluster runtime.

## Commands

Always use `poe` tasks (never call pytest directly):

- `poe test-unit` — unit suite (`tests/unit`).
- `poe test-integration` — integration suite (local Spark, plus Azure
  storage access from environment config).
- `poe test-system --platform <fabric|synapse|databricks>` — system tests
  against real platforms; slow and costly, run only what your change
  impacts. `--test <pattern>` narrows further.
- `poe test-quick` — unit + integration.
- `poe test-extension --extension <name>` — extension system tests.
- `poe format` — black; `poe lint` — pylint; `poe check` — format + lint +
  full tests.
- `poe cleanup [--platform ...]` — tear down cloud test resources.

Pre-commit hooks run black, isort, flake8, and private-key detection. A
reformat aborts the commit silently — if files change, re-stage and commit
again, and confirm HEAD actually moved before pushing.

Integration and system tests read Azure credentials and endpoints from the
environment; without them, run unit tests only.

## Documentation conventions

- `docs/proposals/` is the decision record: one proposal doc per design
  decision, moved to `docs/proposals/obsolete/` when superseded. Durable
  design decisions go here (or the matching `docs/contributing/` doc), not
  in commit messages alone. Migration-style features follow desired-state
  convergence (declare the target; the framework converges) — never
  versioned migration scripts.
- `CHANGELOG.md` is Keep-a-Changelog style: add entries under
  `## Unreleased` (`### Added/Changed/Fixed`) with each user-visible change.
- Release notes live in `docs/releases/vX.Y.Z.md`.

## Contribution expectations

- Conventional commits with scopes (`feat(streaming): ...`,
  `fix(bootstrap): ...`, `docs(proposals): ...`).
- Branch from `main` using `<type>/<slug>` (e.g. `feat/123-short-slug` when
  addressing issue #123). Never commit directly to `main`; open a PR. PR
  bodies reference the issue (`Closes #123`).
- Tests and formatting are required for code changes: run targeted suites
  for what you touched plus `poe format` before pushing. CI gates on black,
  unit, integration, KDA packaging, and bandit (pylint/mypy are
  report-only).
- Type hints and docstrings per `CONTRIBUTING.md`; PEP 8; Spark 3.x
  compatibility across all supported platforms.
- Preserve user changes you did not make; keep implementation changes
  scoped to the task at hand.

## Session completion

When ending a work session:

1. Record any remaining follow-up work.
2. Run quality gates if code changed.
3. Commit completed changes when appropriate.
4. Push committed work to the remote branch:

   ```bash
   git pull --rebase
   git push
   git status
   ```

5. Verify the branch is clean or clearly note any intentional uncommitted
   work.
6. Hand off enough context for the next session.
