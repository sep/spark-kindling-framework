# Workspace: Spark Kindling Framework
**Updated:** 2026-04-29

## Project
Spark Kindling (`spark-kindling` on PyPI, `import kindling`) is an open-source MIT-licensed framework for building declarative, dependency-injection-driven data pipelines on Apache Spark across Microsoft Fabric, Azure Synapse Analytics, and Databricks. It provides data entities, transformation pipes, hierarchical YAML configuration, Delta Lake integration, a plugin/extension system, and CLI tooling for scaffolding, packaging, and deploying apps as `.kda` archives.

## Tech Stack
- Language: Python 3.10+ (dev env: 3.11.15)
- Framework: PySpark 3.4+, Delta Lake (delta-spark 2.4+)
- Config: Dynaconf (hierarchical YAML: platform / workspace / environment layers)
- Key libraries: injector (DI), blinker (signals/events), azure-identity, azure-storage-file-datalake, azure-synapse-artifacts, databricks-sdk
- Build: Poetry + poethepoet (`poe` tasks)
- CI/CD: GitHub Actions (`.github/workflows/`)

## Repo Structure
```
packages/
  kindling/              — Main framework package (source of truth for runtime)
  kindling_cli/          — `kindling` CLI (scaffolding, testing, deployment)
  kindling_sdk/          — Programmatic SDK access
  kindling_otel_azure/   — Extension: Azure Monitor OpenTelemetry
  kindling_databricks_dlt/ — Extension: Databricks DLT
  kindling_visualization/  — Extension: visualization utilities
tests/
  unit/                  — No cloud infra, Spark mocked
  integration/           — Local Spark session, no cloud
  system/                — Require live cloud infra (Fabric / Synapse / Databricks)
  local-project/         — Local Python-first dev project used for local tests
docs/                    — Markdown documentation + proposals/
scripts/                 — Build, deploy, version management
iac/                     — Terraform (Databricks workspace infra)
runtime/                 — Runtime bootstrap scripts
examples/                — Example configurations
.github/agents/          — Agent role definitions (coordinator, planner, implementer, etc.)
.github/skills/          — Agent skill definitions (handoff, memory)
.agent-memory/           — Agent coordination memory (this directory)
```

## Entry Points
- Main: `packages/kindling/__init__.py`
- CLI: `packages/kindling_cli/kindling_cli/cli.py`
- Tests (unit): `poe test-unit`
- Tests (unit + integration): `poe test-quick`
- Tests (all): `poe test`
- Lint: `poe lint` (pylint) + `poe format` (black)
- Type check: `mypy`
- Build: `poe build` or `poe build-wheels`
- Dev server: n/a — library, not a server

## Off-Limits Paths
Agents must not modify these without explicit human instruction:
- `.env` — real credentials; never read, write, or commit
- `.env.sep`, `.env.rr` — environment-specific credential files
- `iac/` — Terraform state and infrastructure; modify only with human review

## Agent Notes
- Platform entry points are registered via `pyproject.toml` plugin entrypoints (`spark_kindling.platforms`); adding a new platform requires both a `platform_*.py` module and an entrypoint registration.
- Worktrees live at `.worktrees/TASK-YYYYMMDD-NNN/` — parent directory `/workspaces/` is read-only in this devcontainer; the `../kindling-worktrees/` pattern cannot be used.
- Use `gh issue list` / `gh issue view` at task start — issues are an active memory source alongside this directory.
- Agent platform defaults: coordinator/planner/reviewer/security → Claude; implementer/tester → Codex (gpt-4o); integrator → Copilot. Route to available platforms if any are unavailable.
