# Conventions

All agents must follow these standards. Implementer and integrator agents
enforce these when writing. Reviewer agents check against these.

To change a convention: write a decision to DECISIONS.md first, then update here.
To deprecate a convention: mark `[DEPRECATED - see DECISIONS.md DATE]` — do not delete.

---

## Naming
- Modules: `snake_case`
- Classes: `PascalCase`
- Constants: `UPPER_SNAKE_CASE`
- Private methods/vars: `_leading_underscore`
- Test files: `test_[module_name].py`
- Test functions: `test_[function]_[scenario]_[expected_outcome]`

## File Organization
- Core framework source: `packages/kindling/`
- CLI source: `packages/kindling_cli/kindling_cli/`
- Tests mirror source: `tests/unit/`, `tests/integration/`, `tests/system/`
- Agents never create new top-level directories without a DECISION record

## Patterns
- Dependency injection via `injector` library — use `GlobalInjector` / `get_kindling_service`
- Platform detection before any platform-specific call — use `PlatformServices`
- Structured logging only — no bare `print()` in library code; use `spark_log`
- Type hints on all public functions
- Docstrings on all public classes and functions (this is an OSS public contract)
- Platform adapters registered via `@PlatformServices.register(name="<platform>")` entry points
- Config loaded hierarchically via Dynaconf: platform → workspace → environment layers

## Anti-Patterns
- No hardcoded credentials, tokens, connection strings, or URLs
- No bare `except:` — always catch specific exceptions
- No mutable default arguments
- No commented-out code in commits
- No internal client names, SEP project names, or proprietary references in any file (OSS)
- No `print()` in library code — use the `kindling` logging system

## Testing Requirements
- Tests after implementation (not strict TDD)
- All public API, data transforms, and platform adapters require tests
- Unit tests: mock platform calls — no real Spark/Azure
- Integration tests: local Spark session allowed, no cloud
- System tests: require live cloud infra — tag with `@pytest.mark.system` etc.
- Fixtures in `conftest.py`; use pytest-mock
- **Always use `poe` tasks to run tests — never `python -m pytest` directly:**
  - `poe test-unit` — unit tests only
  - `poe test-quick` — unit + integration
  - `poe test` — all suites
  - `poe test-coverage` — with HTML coverage report

## Error Handling
- Catch specific exceptions — never bare `except:`
- Surface meaningful messages at system boundaries (user input, external APIs)
- Error messages must not leak filesystem paths or environment details

## Logging
- Never log credentials, tokens, or PII
- Use the `kindling` structured logging system (`spark_log`)
- No bare `print()` in library code

## Comments
- Comment why, not what
- TODOs must include TASK-ID: `# TODO(TASK-20260429-001): ...`
- No commented-out code in commits

## OSS Security
- No secrets in any form — env var references only
- All new dependencies must be Apache 2.0 / MIT / BSD compatible (MIT project)
- No GPL/LGPL/AGPL dependencies
- No internal URLs, hostnames, or tenant IDs in code or tests
- Pin dependency versions in requirements files

## Agent Tagging Convention
When an agent modifies a file, tag the change:
```
# [agent-name] [brief reason] — TASK-[ID]
```
Example: `# [integrator] wired SparkJdbcProvider registration — TASK-20260429-001`

## Agent Tooling — Platform Assignment
Role files are in `.github/agents/`. Default platform per role:

| Role | Default platform | Notes |
|------|-----------------|-------|
| coordinator | Claude (`claude-sonnet-4-5`) | Entry point; decomposes and routes |
| planner | Claude (`claude-sonnet-4-5`) | Design docs only; no production code |
| reviewer | Claude (`claude-sonnet-4-5`) | Verdicts and feedback; doesn't rewrite |
| security | Claude (`claude-sonnet-4-5`) | Security review; read-only |
| implementer | Codex (`gpt-4o`) | Code generation; cloud sandbox |
| tester | Codex (`gpt-4o`) | Test generation and validation |
| integrator | Copilot | Wires modules in; in-editor context |

If a platform is unavailable, coordinator re-assigns to what's available.
Skills live in `.github/skills/` (handoff, memory).

## GitHub Issues as Memory
Use `gh issue list` / `gh issue view <N>` at task start for bugs, features,
and in-progress context. Cross-reference issue numbers (`#NNN`) in task blocks.
