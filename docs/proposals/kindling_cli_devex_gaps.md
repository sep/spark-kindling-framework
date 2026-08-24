# Kindling CLI Devex Gaps Proposal

**Date:** 2026-08-16
**Status:** Proposal
**Scope:** New CLI commands closing verified gaps in config introspection, entity/pipe listing, and environment diagnosis

---

## Executive Summary

Kindling's CLI already covers the app lifecycle well: scaffold (`app init`,
`package init`, `package add ...`), validate structure (`app validate`),
run locally or remotely (`app run`, `pipeline run`), migrate schemas
(`migrate plan/apply/rollback/cleanup`), and deploy (`package deploy`,
`runtime deploy`, `workspace deploy`). What it does **not** yet give a
developer is visibility *into* the config-overlay machinery once a project
has more than one settings file layered with `dataentities:`,
`dataentities-bytag:`, `datapipes:`, `datapipes-bytag:`, and `entity_tags:`
sections — and no single command tells a new contributor "is my project
actually healthy" the way `env check` tells them "is my machine ready."

This proposal adds seven new commands and one flag, all inside existing
command groups (`config`, `entity`, `pipeline`, `app`, `package`) --
deliberately no new top-level command. An earlier draft of this proposal
added a top-level `kindling doctor` composite health check; that idea is
now folded into the existing `app`/`package` groups as `app check`/
`package check`, consistent with the CLI's existing `env check` naming
(see Design Principle 1 and the "Proposed Commands" section below).

| Command | Group | Fills |
|---|---|---|
| `kindling config show` | `config` | No way to see the *merged* effective config |
| `kindling config diff` | `config` | No way to compare effective config across env/platform |
| `kindling entity list` | `entity` | No top-level entity listing (only nested under `app inspect`) |
| `kindling entity tags` | `entity` | No way to see resolved tags + which layer set them |
| `kindling pipeline show` | `pipeline` | No pipe-level analog to `entity show` at all |
| `kindling app check` | `app` | No single "is my app healthy" composite check (import smoke test, entity/pipe graph, runtime version skew) |
| `kindling package check` | `package` | No package-level health check before `package deploy` (metadata, layout, wheel build) |
| `--trace` on `app run`/`pipeline run` | `app`, `pipeline` | Bootstrap trace printing exists but is undiscoverable |

---

## Problem Statement

`docs/reference/config_reference.md` documents a genuinely intricate
precedence stack for entity/pipe configuration:

1. Literal `tags=` on the entity/pipe definition in code.
2. `dataentities:` / `datapipes:` — id-glob pattern overrides.
3. `dataentities-bytag:` / `datapipes-bytag:` — tag-value glob overrides
   (`TagRuleMatcher`, applied *before* the id-glob overrides above, so a
   specific id-pattern entry can still win over a broader tag-based
   default).
4. Top-level `entity_tags:` — id-keyed overrides merged at lookup time.
5. `@secret:<key>` references inside any of the above, resolved once
   platform services exist, with a second overlay pass so the resolved
   value (not the literal reference) lands in the registered metadata.

None of this is observable from the CLI today. A developer who sees an
entity write to the wrong catalog, or a pipe pick up an unexpected
`schema.drift` policy, has exactly one tool: read the YAML files by hand
and mentally re-implement the precedence rules from the docs. As overlay
usage grows, that manual process gets more error-prone, not less.

Separately, `pipeline list` — the sibling of `entity show`/`entity validate`
— was never given a pipe-level equivalent to those two commands. And
there is no single "is my app/package okay" check comparable to what
`env check` already does for the machine.

---

## Design Principles

These follow directly from the existing CLI's own conventions:

1. **New commands live inside existing groups wherever the noun already
   exists.** `config show`/`config diff` join `config init`/`config set`;
   `entity list`/`entity tags` join `entity show`/`entity validate`;
   `pipeline show` joins `pipeline run`/`pipeline list`; the composite
   health check is split as `app check` (app-level: entity/pipe graph,
   app-import smoke test, runtime version skew) and `package check`
   (package-level: metadata, layout, wheel build), each joining its own
   group's existing verbs, using the `check` verb the CLI already
   established with `env check`. No new top-level command is introduced —
   a project that wants "check everything" composes its own `poe`/Make
   task calling both, rather than this proposal building a meta-command
   that calls both for them.

2. **`--env` resolution always falls back to `KINDLING_ENV`.** A consistency
   audit found `entity show`/`entity validate`/`app inspect` silently
   *don't* do this while `pipeline run`/`app validate` do. Every new
   command below uses the `pipeline run` convention
   (`env or os.getenv("KINDLING_ENV", "local")`) so it doesn't compound the
   existing inconsistency.

3. **`--json` is included on every new command from day one.** The audit
   found `--json` support has no discernible rule today; rather than adding
   six more inconsistent cases, every new command reuses the existing
   `_emit_result`/`_emit_json` helpers already used by `app package`,
   `app deploy`, `runner status`, etc.

4. **Secrets are never printed in plaintext by default.** This mirrors the
   framework's own stance (a secret that can't be resolved after platform
   init fails bootstrap loudly rather than handing a literal reference to a
   provider) — a config-inspection tool must not be the thing that leaks a
   resolved secret into a terminal scrollback or CI log. Both `config show`
   and `entity tags` redact resolved `@secret:` values by default and
   require an explicit, loud opt-in flag to reveal them, the same
   "explicit flag for dangerous behavior" shape as `migrate apply
   --destructive`.

5. **Read-only commands don't need a confirmation prompt.** Every command
   in this proposal is read-only (config/entity/pipe introspection,
   diagnostics) — none of them need the `--yes`/`click.confirm()` pattern
   from `runner delete`. (A separate, real gap exists on `app cleanup`
   lacking any such confirmation at all, but fixing existing command
   safety is out of scope for a devex-gaps proposal about *missing*
   commands; it's called out here so it isn't lost.)

---

## Proposed Commands

### `kindling config show`

Prints the fully merged effective configuration for a given app, exactly
as Kindling would layer it at bootstrap: base `settings.yaml` → platform
overlay → environment overlay, then the effect of `dataentities:` /
`dataentities-bytag:` / `datapipes:` / `datapipes-bytag:` / `entity_tags:`
if an entity or pipe ID is given.

```bash
kindling config show --app myapp --env dev
kindling config show --app myapp --env dev --platform databricks
kindling config show --app myapp --env prod --json
```

Flags:

- `--app` (`app_path` under the hood) — same convention/option as
  `pipeline run`/`entity show`.
- `--env` — defaults via `KINDLING_ENV`, per Design Principle 2.
- `--platform` — optional Choice(`SUPPORTED_PLATFORMS`); when supplied,
  layers in `settings.<platform>.yaml` the same way `workspace deploy`
  does.
- `--key <dotted.path>` — print just one resolved key (e.g.
  `kindling.telemetry.tracing.level`), so this command also plugs the
  smaller "no `config get`" gap without a separate command.
- `--reveal-secrets` — off by default; resolved `@secret:` values print as
  `<secret: scope/key>` unless this flag is passed, in which case the
  command prints a one-line warning banner before the real value.
- `--json` — machine-readable form via `_emit_json`.

This directly answers "why is prod behaving differently than dev" without
requiring a full Spark bootstrap — it only needs to run the same YAML-load
+ overlay-merge path the framework already uses, not start a session.

### `kindling config diff`

A thin, high-value wrapper around `config show`: resolves config twice
(different `--env`/`--platform` combinations) and prints only the keys
that differ.

```bash
kindling config diff --app myapp --env dev --diff-env prod
kindling config diff --app myapp --platform databricks --diff-platform fabric
kindling config diff --app myapp --env dev --diff-env prod --json
```

Flags mirror `config show`, with `--diff-env`/`--diff-platform` naming the
second side of the comparison (kept as separate flags rather than
overloading `--env`/`--platform` with a list, so existing single-value
usage patterns aren't disturbed). Output is a compact table: key, value in
side A, value in side B — same rendering helper already used by
`entity show`/`app inspect`.

This can ship as a fast-follow to `config show` rather than in the same
release; it's listed here because "why does prod behave differently than
dev" is one of the most common support questions once overlays are in
play, and the diff is nearly free once `show`'s merge logic exists.

### `kindling entity list`

A top-level listing command, symmetric with `pipeline list`, replacing the
current situation where entity listing only exists nested inside
`app inspect <app> --entities`.

```bash
kindling entity list --app myapp
kindling entity list --app myapp --tags
kindling entity list --app myapp --json
```

Flags:

- `--app`, `--env` — same as `entity show`.
- `--tags` — adds a `Tags` column showing the full resolved tag dict per
  entity (not just `provider_type`), addressing the "no tag visibility in
  listings" gap.
- `--json` — full entity metadata as structured output.

`app inspect --entities` keeps working unchanged; this is additive, not a
replacement, so nothing existing breaks.

### `kindling entity tags <entity_id>`

The direct answer to "why did this entity get the config it got." Prints
the entity's final resolved tag dict with each tag annotated by which
config layer last set it, in the framework's own precedence order.

```bash
kindling entity tags bronze.orders --env dev
kindling entity tags bronze.orders --env prod --platform databricks --json
```

Example output:

```text
Entity: bronze.orders  [env: prod, platform: databricks]

  provider_type              delta                    (literal tags=)
  provider.table_catalog     dev_bronze               (dataentities-bytag: tier=bronze)
  schema.drift               fail                     (dataentities-bytag: tier=gold*)  <- overridden below
  schema.drift               warn                     (dataentities: bronze.orders)
  provider.table_name        <secret: kv/orders-table> (entity_tags: bronze.orders)
```

Flags: `--env`, `--app`, `--platform`, `--json`, `--reveal-secrets` (same
redaction default as `config show`).

Implementation-wise this walks the same `ConfigPatternMatcher`/
`TagRuleMatcher` code path already used by `DataEntityRegistry` at lookup
time — the command doesn't reimplement precedence, it just makes the
existing resolution observable and prints provenance alongside it.

### `kindling pipeline show <pipe_id>`

The pipe-level analog of `entity show`, closing the asymmetry where entity
has `show`+`validate` and pipes have neither. Rather than adding a third
pipe-specific command, tag provenance for pipes is folded into this one
command via a flag, keeping the surface smaller than the entity side while
still covering the same need.

```bash
kindling pipeline show bronze.ingest_orders --app myapp --env dev
kindling pipeline show bronze.ingest_orders --app myapp --tags --json
```

Prints: pipe name, `input_entity_ids`, `output_entity_id`, `output_type`,
and — with `--tags` — the resolved tag dict with provenance, exactly as
`entity tags` does but sourced from `datapipes:`/`datapipes-bytag:`.

### `kindling app check`

A composite health check for a single app, superset of `app validate`,
reported with the same `[PASS]`/`[FAIL]` convention as `env check`/
`app validate` rather than inventing a third status-report style (a
consistency-audit finding was that two incompatible check-report formats
already coexist between `env check`/`app validate` and `entity
validate` — `app check` should not add a third).

```bash
kindling app check
kindling app check --app myapp --env dev
kindling app check --platform databricks
```

Composes, without duplicating logic:

- **New:** an app-import smoke test — attempts to load the discovered
  `app.py` module and reports import/registration errors as a
  `[FAIL] app_import` line, without executing any pipe (mirrors what
  `app validate` does for the entity/pipe graph, but catches the class of
  errors that happen *before* the registries are even populated — plain
  Python import errors, missing `initialize()`, etc.). `app validate`
  today lets this class of failure abort the command outright with a
  `ClickException`; `app check` instead converts it into a named,
  reportable check line and skips the graph checks below if it fails.
- Everything `app validate` already checks once import succeeds:
  entities/pipes registered, every pipe's input/output entities resolve,
  every Delta entity declares `merge_columns`.
- **New:** CLI/deployed-runtime version skew, gated behind `--platform`
  (plus the same `--artifacts-path`/`--storage-account`/`--container`/
  `--base-path` options `package deploy` already exposes) since it needs
  artifact-storage access — reuses the existing runtime-outdated warning
  logic, today only triggered as a side effect inside `app run`/
  `package deploy`, promoted here to a named check. Without `--platform`,
  or if no artifacts destination can be resolved, this check is skipped
  (reported as a non-blocking note, not a failure) rather than forcing
  every local `app check` invocation to need remote credentials.
- A one-line summary of registered entity/pipe counts (reusing the same
  counts `app validate` already prints).

Exit code: `0` if every check passes, `1` otherwise — same convention as
`env check`. Supports `--json` per Design Principle 3.

### `kindling package check`

The package-level counterpart, checking the things `package deploy`
depends on *before* a developer runs a real deploy: valid Poetry
metadata, a plausible `src/` layout, and (unless `--skip-build`) that the
package's wheel actually builds via the same `poetry build` invocation
`package deploy` uses internally.

```bash
kindling package check my-domain-package
kindling package check my-domain-package --local-folder packages/my-domain-package
kindling package check my-domain-package --skip-build
```

Composes, without duplicating logic:

- `pyproject`: `tool.poetry.name`/`version` are present and parse (reuses
  `package deploy`'s own metadata loader).
- `src_layout`: a `src/` directory exists under the package root and
  contains at least one importable Python package (an `__init__.py`
  under a subdirectory) — layout-agnostic across the `medallion`/
  `minimal` scaffold styles `package init --layers` produces.
- `wheel_build` (skippable via `--skip-build` for a fast check in tight
  loops): actually runs `poetry build --format wheel`, the same call
  `package deploy` makes, and reports success/failure rather than
  inferring buildability from file presence alone.

Exit code: `0` if every check passes, `1` otherwise. Supports `--json`.

`kindling app check` and `kindling package check` are deliberately
separate, uncomposed commands — see Design Principle 1. A project that
wants both run together (e.g. in CI) composes them itself:

```bash
# poe check = kindling app check && kindling package check my-pkg
```

### `--trace` on `kindling app run` / `kindling pipeline run`

Not a new capability — `--param print_trace=true` already works today for
both standalone and remote runs. The gap is pure discoverability: nothing
in `--help` mentions it, and a user has to already know the internal flat
bootstrap key name from `docs/reference/config_reference.md`.

```bash
kindling app run myapp --trace
kindling app run myapp --trace --trace-level verbose --platform databricks
kindling pipeline run bronze.ingest_orders --trace
```

`--trace` is documented sugar for `--param print_trace=true`;
`--trace-level {minimal,standard,verbose}` is sugar for
`--param kindling.telemetry.tracing.level=<level>`. Both compose with
existing `--param`/`--parameters` — passing an explicit
`--param print_trace=false` still wins, consistent with how `--param`
already overrides file-based and env-based parameters elsewhere. This is a
one-line addition to each command's option list and requires no new
plumbing.

---

## What This Proposal Deliberately Does Not Do

- **No `pattern`-style app scaffolding** — that's already proposed
  separately in `docs/proposals/obsolete/kindling_patterns_cli.md` and shouldn't be
  duplicated here.
- **No fixes to CLI consistency findings** (e.g. `app cleanup` missing a
  confirmation prompt, the `--env`/`--platform` default-semantics split
  across sibling commands, the `pipeline` vs. `pipe` naming mismatch).
  Those are real, but they're behavior changes to *existing* commands, not
  new commands — a separate, smaller proposal (or a fast-follow PR) is the
  right vehicle so it can be reviewed and versioned independently of new
  surface area.
- **No `--fix` mode on `app check`/`package check`.** Auto-remediation
  (e.g. running `env ensure` automatically) is tempting but changes them
  from a read-only diagnostic into a side-effecting command, which would
  need its own confirmation-prompt story (see Design Principle 5) —
  deferred.
- **No meta-command that runs `app check` and `package check` together.**
  A project that wants "check everything" composes its own `poe`/Make
  task; see Design Principle 1.
- **No secret *values* ever appear by default** anywhere in this proposal;
  `--reveal-secrets` is opt-in and always paired with a warning banner.

---

## Implementation Plan

### Phase 1 — `config show` + `entity tags`
These share the same underlying need (walk the overlay stack and report
provenance) and are the highest-value pair — they directly answer "why did
my entity/config end up like this." Ship together.

### Phase 2 — `entity list` + `pipeline show`
Additive, symmetric commands with no shared new plumbing beyond Phase 1's
tag-resolution helper (reused by `pipeline show --tags`).

### Phase 3 — `app check` + `package check`
Composes existing checks plus the new ones each needs (app-import smoke
test and gated version skew for `app check`; metadata/layout/wheel-build
for `package check`); no new resolution logic needed, so it's independent
of Phases 1-2 and can ship whenever convenient.

### Phase 4 — `config diff` + `--trace`/`--trace-level`
`config diff` is a thin diffing wrapper over Phase 1's `config show`.
`--trace` is a one-line option addition to `app run`/`pipeline run`. Both
are low-risk, low-effort, and can land in any order relative to Phase 3.

---

## Acceptance Criteria

1. `kindling config show --app <app> --env <env>` reflects the exact
   config a real bootstrap would use for that env/platform — verified by
   a test that bootstraps an app both ways and asserts the resolved
   dictionaries match.
2. `kindling entity tags <id>` never prints a resolved `@secret:` value
   without `--reveal-secrets`, and prints a visible warning when it does.
3. `kindling entity list`/`kindling pipeline show` do not change any
   existing command's output — `app inspect --entities` and `pipeline list`
   keep working exactly as before.
4. `kindling app check` and `kindling package check` each exit `0` iff
   every constituent check passes, matching `env check`'s exit-code
   convention.
5. `--trace`/`--trace-level` are documented in `--help` text for both
   `app run` and `pipeline run`, and are provably equivalent to the
   existing `--param print_trace=...`/
   `--param kindling.telemetry.tracing.level=...` invocations (a test
   asserts the same env vars/parameters dict result from both forms).

---

## Recommendation

Ship Phase 1 (`config show`, `entity tags`) first. It is the proposal's
highest-leverage item: it turns a currently-undebuggable class of
problem — "why did this entity/pipe end up with this config" — into a
one-command answer, using logic the framework already has. Everything
else in this proposal is either symmetric scaffolding around that same
idea (`entity list`, `pipeline show`, `config diff`) or a low-cost,
low-risk addition (`app check`, `package check`, `--trace`) that composes
existing behavior rather than inventing new subsystems.

---

## Appendix: Consistency Findings Motivating the Design Principles Above

A companion audit of the existing ~50-command CLI (`packages/kindling_cli/kindling_cli/cli.py`)
surfaced the following, recorded here so they aren't lost even though fixing
them is out of scope for this proposal:

1. **Group naming mismatch**: the pipe-inspection group is called
   `pipeline`, not `pipe`, even though every message inside it says "pipe."
2. **`--env` has two incompatible default behaviors**: `pipeline run`/
   `app validate` honor `KINDLING_ENV`; `entity show`/`entity validate`/
   `app inspect` hardcode `default="local"` and ignore it.
3. **`--platform` has two incompatible default semantics**: remote-operation
   commands auto-detect from environment variables when unset; `app run`
   instead defaults to the literal string `"standalone"` and never
   auto-detects.
4. **The "which app" argument's name varies**: `app_name` on three
   subcommands, `app` on a fourth, producing inconsistent `--help` metavars
   for the same concept.
5. **"Remove/delete" uses three different verbs** (`cleanup`, `delete`)
   with no consistent mapping to destructiveness.
6. **Destructive-command safety is inconsistent**: `runner delete` is the
   only command with a confirmation prompt + `--yes`; `app cleanup`
   (which permanently deletes a deployed remote app) has no confirmation,
   no `--yes`, and no `--force` at all.
7. **Two incompatible "run checks and report" output styles coexist**:
   `env check`/`app validate` share one reporter; `entity validate`
   implements its own bespoke one.
8. **`--json` support is inconsistent** with no discernible rule across
   commands that all emit structured data.
9. **Flag naming style itself is consistent** (kebab-case throughout) —
   checked specifically and found to be a non-issue.
10. **Pipes have no `show`/`validate` analog to entities'** — addressed
    directly by this proposal's `pipeline show`.
11. **Exit-code mechanics differ** across otherwise-similar "run and
    report" commands (`ClickException` vs. `sys.exit` vs.
    `click.exceptions.Exit`), with no documented convention for which to
    use when.
