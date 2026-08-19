# Poetry to uv Migration

**Date:** 2026-08-19
**Status:** Proposal
**Scope:** Replacing Poetry with uv as the toolchain `kindling repo init` /
`kindling package init` scaffold into domain projects, and specifically
enabling uv workspaces for the apps/packages monorepo layout domain teams
already build on top of Kindling. This framework repo's own internal build
(the 10 packages under `packages/`) is explicitly **out of scope** — see
"Deliberately Out of Scope" — since nothing here depends on it changing.
Does not cover changing the build backend (see "What doesn't need to
change").

---

## Executive Summary

The problem this targets is downstream, not internal to this framework:
domain teams build company monorepos of multiple Kindling apps and shared
packages under one root, and Poetry has no native concept of a workspace to
converge them. That gap is real and already costing custom code —
`_reconcile_root_kindling_dependencies` (a ~90-line function added this
session to `kindling_cli`) exists specifically because a root
`pyproject.toml` with no Kindling dependency of its own has no way to learn
"what version do my apps already use" from Poetry itself; it has to walk
the directory tree and hand-roll the reconciliation.

uv's `[tool.uv.workspace]` is exactly this: one root, N member projects
(`apps/*`, `packages/*`), one lockfile, dependency resolution that converges
across members natively. Adopting it for domain projects would let that
reconciliation logic retire (for uv-based projects — see phasing) instead of
growing further as monorepos get bigger or the "root vs. nested" cases get
more elaborate.

The recommendation is to do this, but as a real migration with its own
phases, not a drive-by tool swap: Kindling's CLI *is* Poetry-schema-aware
code (it parses `[tool.poetry.dependencies]`/`[tool.poetry.group.*]`
directly and shells out to `poetry add <url>`), and every domain project
scaffolded so far is a standalone Poetry project. This document inventories
what changes, what doesn't, and proposes a phased path — focused entirely on
the domain-project side — that keeps existing Poetry-based domain projects
working throughout.

---

## Current State: Poetry Touchpoints

### In every domain project `kindling repo init` scaffolds

| Location | What it does |
|---|---|
| `packages/kindling_cli/kindling_cli/templates/pyproject.toml.j2` | Generates a standalone Poetry project: `[tool.poetry.dependencies]` with `spark-kindling = { url = "...", extras = [...] }`, `[tool.poetry.group.dev.dependencies]` for `spark-kindling-sdk`/`spark-kindling-cli`, `[tool.poe.tasks]` including `build = "poetry build"`. |
| `templates/.devcontainer/devcontainer.json.j2` | `postCreateCommand` runs `kindling env bootstrap`. |
| `templates/.github/workflows/ci.yml.j2` | Runs inside `devcontainer:latest`; relies on Poetry being present in that image. |
| `packages/kindling_cli/kindling_cli/cli.py` | The whole `env` command group is Poetry-schema-coupled: `_load_pyproject_toml` parses via `tomllib`, but `_iter_kindling_dependency_entries` walks `poetry.get("dependencies")` / `poetry.get("group", {})[g]["dependencies"]` specifically (Poetry's proprietary table shape, not PEP 621's `[project.dependencies]`); `_poetry_add_url` shells out to `poetry add <url> [--group G] [--extras E...]`; `_declared_kindling_version` reads the `url=`/`version=` keys Poetry's dependency tables use. |
| `_reconcile_root_kindling_dependencies` / `_discover_descendant_pyprojects` (added this session) | Exists specifically because domain teams build company monorepos of multiple Kindling apps/packages under one root, and Poetry gives no native way for the root to converge on "whatever version the apps already use." |

**Net:** every dependency-management code path in `kindling_cli` — not just
templates — assumes Poetry's schema and CLI surface. This is a real
migration, not a config swap.

---

## The Case for uv

1. **Native workspaces map directly onto the apps/packages monorepo layout.**
   `[tool.uv.workspace]` with `members = ["apps/*", "packages/*"]` gives the
   "root converges on one version, apps/packages just declare what they
   need" behavior domain teams are already building by hand — the exact
   thing `_reconcile_root_kindling_dependencies` was built to approximate
   without workspace support. Under a real workspace, that function (and
   its conflict-detection logic) becomes largely unnecessary for uv-based
   monorepos: uv's resolver raises on the same kind of cross-member
   disagreement natively, without a custom directory walk.

2. **Removes a real, documented class of Poetry friction from generated
   devcontainers/CI.** `Dockerfile.ci` (this repo's own CI image, unrelated
   to domain projects directly, but instructive) documents a real bug:
   Poetry's implicit virtualenv auto-detection breaks when HOME/cwd differ
   between two container contexts, silently resolving into a fresh empty
   venv (`:42-47`). Domain projects' generated CI (`ci.yml.j2`) runs inside
   the published devcontainer image and is exactly the kind of
   different-HOME/different-cwd situation that failure mode targets — it
   just hasn't been hit yet because domain project CI doesn't do much
   Poetry-environment juggling today. uv's environment resolution is
   explicit and path-based (`UV_PROJECT_ENVIRONMENT`, `.venv` next to the
   lockfile) rather than heuristic, so this class of bug has no uv analog to
   guard against as domain project CI grows more elaborate.

3. **Simpler, faster devcontainer bootstrap.** `kindling env bootstrap` (the
   devcontainer `postCreateCommand`) currently shells out to `poetry
   install --sync`; uv's resolver and installer are dramatically faster, and
   uv ships as a single static binary with no separate Python-managed
   install step. It can also manage the Python interpreter itself (`uv
   python install`), which is one less moving part in
   `Dockerfile.devcontainer` (currently: base image's Python +
   `pip install poetry==1.8.3 poethepoet==0.45.0`).

4. **Standards-based schema.** uv projects use PEP 621 (`[project]`,
   `dependencies = [...]` as plain PEP 508 strings) plus `[tool.uv.sources]`
   for the URL/path/git overrides Poetry expresses inline. This is more
   interoperable (any PEP 621-aware tool can read it) and, for Kindling's
   CLI specifically, means `_iter_kindling_dependency_entries` would parse
   one standard shape instead of Poetry's proprietary
   `dependencies`/`group.<name>.dependencies` split.

5. **`poetry add <url>` has a direct uv equivalent.** `uv add <wheel-url>`
   adds the resolved package name to `[project.dependencies]` and records
   the exact URL under `[tool.uv.sources]` — functionally the same pinning
   idiom as today's `spark-kindling = { url = "..." }`, just split across
   two tables instead of embedded in one. `_poetry_add_url`'s group/extras
   re-supply logic (documented as necessary because Poetry doesn't preserve
   either automatically on re-add) needs re-verifying against uv's
   behavior, but the same category of helper is still needed either way.

---

## What Doesn't Need to Change

- **`[tool.poe.tasks]` / poethepoet.** Poe is not Poetry-specific — it's a
  standalone task runner that reads `pyproject.toml` regardless of which
  dependency manager populated the environment. `poe test-unit`, `poe
  build`, etc. keep working; only *what* `poe build`/`update-kindling`
  shell out to changes (`poetry build` → `uv build`, `kindling env update`
  is internal and just needs its own implementation updated).
- **The build backend.** `poetry-core` is a standalone PEP 517 backend —
  it doesn't require Poetry itself to be installed to build a wheel, and uv
  (or any PEP 517 frontend) can invoke it directly. `[build-system] requires
  = ["poetry-core>=1.0.0"]` can stay as-is; switching to `hatchling` or
  another backend is a separate, independent decision, not a prerequisite.
- **The GitHub-Releases-as-source-of-truth model.** `_resolve_kindling_release_wheels`,
  `_github_release_for_tag`, `_resolve_github_version` — the whole "GitHub
  Releases holds the checksummed wheel assets, `poetry add <url>` pins one
  of them" design from `domain_devcontainer_contract.md` is dependency-manager-agnostic.
  Only the "how it gets written into pyproject.toml and re-added" step
  (`_poetry_add_url`) is Poetry-specific.
- **Runtime code.** `packages/kindling` itself has zero Poetry awareness at
  import time or runtime — this is purely a design-time/CLI/CI concern.

---

## Deliberately Out of Scope

This framework repo's own internal build — `pyproject.toml` (root) +
`packages/kindling_sdk/pyproject.toml` + `packages/kindling_cli/pyproject.toml`
+ 8× `packages/extensions/kindling_ext_*/pyproject.toml`, all built via
`scripts/build.py`'s per-directory `poetry build` calls, plus
`.github/Dockerfile.ci`'s Poetry `2.4.0` pin — stays on Poetry. Nothing a
domain project does depends on how this repo builds its own release wheels;
`kindling-cli`'s published wheel is just a wheel, regardless of what tool
produced it. Revisiting that later is an independent decision with its own
tradeoffs and isn't a prerequisite for anything below.

The one place this repo's own tooling *does* matter to domain projects is
`.github/Dockerfile.devcontainer`, which pins Poetry `1.8.3` for domain
projects to use inside the published devcontainer image — that's addressed
below since it's part of what a domain project actually runs.

---

## Migration Touchpoints (concrete)

If this goes ahead, the work breaks down as:

**`kindling_cli` (the product surface domain teams depend on):**
- `_load_pyproject_toml`, `_iter_kindling_dependency_entries`,
  `_dependency_extras`, `_declared_kindling_version`, `_poetry_add_url`,
  `_canonical_distribution_name`'s callers — all need a uv-schema-aware
  path (`[project.dependencies]` + `[tool.uv.sources]`) alongside or
  instead of the current Poetry-schema path.
- `env update` / `env add` / `env bootstrap` / `env check` — the
  `poetry add`/`poetry install --sync` subprocess calls become `uv add`/`uv sync`.
- `_reconcile_root_kindling_dependencies` / `_discover_descendant_pyprojects`
  — retires once the 3 existing projects are converted to real
  `[tool.uv.workspace]` members (Phase 2); no need to keep it as a
  long-lived fallback given how small that migration is.
- `templates/pyproject.toml.j2`, `templates/.devcontainer/devcontainer.json.j2`,
  `templates/.github/workflows/ci.yml.j2` — new uv-flavored versions. A
  `--package-manager poetry|uv` scaffolding flag is still useful briefly
  (covers the gap between Phase 1 shipping and the 3 existing projects
  converting in Phase 2), but doesn't need to be a long-term supported
  option.
- `docs/proposals/domain_devcontainer_contract.md` — needs its own
  "Implementation Update" section (matching how it already documents its
  divergence from the original wheelhouse design) once this lands, since
  its whole contract is phrased in terms of `poetry.lock`/`poetry add`.
- `.github/Dockerfile.devcontainer` — swap the pinned `poetry==1.8.3` /
  `poethepoet==0.45.0` install for a pinned uv version (poethepoet stays;
  it's still needed to run `poe` tasks inside the generated project). This
  is the one file in this repo's own build that domain projects genuinely
  depend on, since it's baked into the devcontainer image every domain
  project's `.devcontainer/devcontainer.json.j2` pulls.

**The 3 existing domain projects (e.g. `cwmdp-test-pkg`):**
- All 3 are early in development, so converting them directly (either by
  hand or a small one-off `kindling env migrate --to-uv` — worth building
  only if it saves more effort than doing 3 conversions by hand) is
  cheaper than building and maintaining an indefinite Poetry-schema
  deprecation window. Once all 3 are converted, Poetry-schema support can
  come out of `kindling_cli` entirely.

---

## Open Questions / Risks — Resolved

1. **Dependency groups** — **Answered by spike.** uv writes
   `[dependency-groups] dev = [...]` (PEP 735) via `uv add <url> --group
   dev`, not `[tool.uv.dev-dependencies]`. Direct analog of Poetry's
   `--group dev`; no `poe` task changes needed.
2. **`uv add <url>` semantics** — **Answered by spike, and it's worse than
   expected.** Re-adding an already-dev-grouped package *without*
   re-passing `--group dev` doesn't move it to main deps the way Poetry
   does (Poetry's documented footgun) — it **duplicates** it: after a bare
   re-add, `spark-kindling-cli` ended up listed in both
   `[project.dependencies]` and `[dependency-groups].dev` simultaneously.
   `uv lock` doesn't error on this, but it's a real inconsistency (anything
   reading `[project.dependencies]` as "runtime deps of the published
   package" would now see a dev-only CLI tool as one). `_poetry_add_url`'s
   uv equivalent must keep the explicit group re-supply logic, and should
   defensively strip any stray bare-name entry from `[project.dependencies]`
   when re-adding into a group. Extras, separately, *do* survive a bare
   re-add automatically — only group placement has the footgun. Also
   discovered in the same spike: `spark-kindling-cli`'s transitive
   `spark-kindling-sdk>=0.9.10` constraint only resolves if
   `spark-kindling-sdk`'s own URL override is already in `[tool.uv.sources]`
   — matches the current template's existing declaration order, so no
   change needed there, but worth calling out since reordering the template
   later would silently break it.
3. **Deprecation window** — **Answered.** Only 3 domain projects exist
   today, all early in development and easy to migrate. No indefinite
   dual-schema support is needed — see the tightened phasing below.
4. **Existing devcontainer image compatibility** — **Answered.** None of
   the 3 projects pin the devcontainer image by digest, so there's no
   float-vs-pin rollout risk to communicate.

---

## Proposed Phasing

Given only 3 easy-to-migrate projects, this doesn't need a long-lived
dual-schema era or a deprecation timeline — it can be a short, coordinated
cutover instead of an indefinitely-supported dual path.

- **Phase 0 — Spike. Done.** Validated directly against real release wheels
  (see "Resolved" above): `uv add <wheel-url>` records the same
  `[tool.uv.sources]` pinning idiom Poetry's `url=` table does, dependency
  groups map cleanly, and the group re-add footgun (and how to guard
  against it) is now known up front rather than discovered mid-implementation.
- **Phase 1 — uv scaffolding + `kindling_cli` uv support.** New
  uv-flavored `pyproject.toml.j2`/`devcontainer.json.j2`/`ci.yml.j2`
  templates; teach `_iter_kindling_dependency_entries` (and
  `_dependency_extras`, `_declared_kindling_version`, `_poetry_add_url`) to
  read/write uv's schema; extend `kindling repo init` with real
  `[tool.uv.workspace]` support for the apps/packages monorepo layout. Only
  needs to run *alongside* Poetry-schema support briefly, not indefinitely.
- **Phase 2 — Migrate the 3 existing projects, then drop Poetry support.**
  Convert each of the 3 (by hand or a one-off `kindling env migrate
  --to-uv` if worth building given there are only 3) shortly after Phase 1
  ships, then retire Poetry-schema parsing from `kindling_cli` entirely —
  no extended deprecation window to manage. `_reconcile_root_kindling_dependencies`
  retires at this point too, once every domain project can use a real
  `[tool.uv.workspace]`.

Each phase is independently shippable and reversible — Phase 0 is already
done, Phase 1 is safe to ship with zero visible change to existing projects,
and Phase 2 is small precisely because the existing footprint is small.
