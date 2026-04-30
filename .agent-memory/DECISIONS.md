# Decision Log

Append-only. To supersede a decision, add a new entry with
`Status: Supersedes [DATE] [Original Title]`.

---

## 2026-04-29 Worktree location uses `.worktrees/` inside repo
**Status:** Accepted
**Agent:** coordinator (Claude Code)
**Context:**
  The `/workspaces` parent directory is read-only in this devcontainer —
  `mkdir ../kindling-worktrees` fails with permission denied.
**Decision:**
  Use `.worktrees/TASK-YYYYMMDD-NNN/` inside the repo root instead of
  `../kindling-worktrees/TASK-ID/`. `.worktrees/` is added to `.gitignore`.
**Consequences:**
  Worktrees are not visible from outside the repo checkout. Agents must use
  relative paths `.worktrees/<TASK-ID>/` when setting up worktrees.

---

## 2026-04-29 GitHub Issues are an active agent memory source
**Status:** Accepted
**Agent:** coordinator (Claude Code)
**Context:**
  Human directed agents to use `gh issues` as memory alongside `.agent-memory/`.
**Decision:**
  Agents run `gh issue list` / `gh issue view` at task start. Related issue
  numbers are cross-referenced in ACTIVE_TASK.md task detail blocks.
**Consequences:**
  Agents need `gh` CLI available (present in this devcontainer). Issues are
  authoritative for feature scope; ACTIVE_TASK.md tracks execution state.

---

## 2026-04-29 SCD merge behavior is a Delta-scoped strategy
**Status:** Accepted
**Agent:** coordinator (Claude Code)
**Context:**
  Issue #63 (SCD Type 2) needs a design that doesn't branch SCD logic inside
  the Delta provider. A provider-agnostic DataFrame-transform approach was
  considered but ruled out.
**Decision:**
  SCD merge semantics are modeled as a `DeltaMergeStrategy` — Delta-scoped
  strategies applied by `DeltaEntityProvider`. The provider resolves the
  strategy from entity tags and delegates the `DeltaTable.merge()` call to it.
  No provider-agnostic abstraction; non-Delta providers keep their own write
  logic unchanged.
**Consequences:**
  Delta covers ~90% of foreseeable SCD use cases. Other providers can adopt
  a similar pattern independently if needed later. Avoids over-engineering a
  cross-provider abstraction prematurely.

---

## 2026-04-30 `kindling validate` uses standalone init, not Spark mocking
**Status:** Accepted
**Agent:** planner (Claude)
**Context:** `kindling validate` must not start a SparkSession, but entity/pipe decorators
  require the DI graph to be wired before they can fire. Two options: (A) mock SparkSession,
  (B) call `initialize_framework(platform="standalone")` which defers SparkSession creation
  until the first entity read/write.
**Decision:** Use option B — call `initialize_framework()` normally. The standalone platform
  only creates a SparkSession lazily, so validation is Spark-free as long as it only reads
  registry state and does not call any provider methods.
**Consequences:** Validates that the real bootstrap works end-to-end (no mock drift). Requires
  `spark-kindling[standalone]` to be installed, which is already the dev dependency.

---

## 2026-04-30 `KindlingNotInitializedError` lives in `data_entities.py`, imported by `data_pipes.py`
**Status:** Accepted
**Agent:** planner (Claude)
**Context:** The error needs to be raised in both `data_entities.py` and `data_pipes.py`.
  It should be importable directly from `kindling` (top-level export).
**Decision:** Define `KindlingNotInitializedError` in `data_entities.py` (alongside other
  public entity types), import it into `data_pipes.py`, and export it from `packages/kindling/__init__.py`.
**Consequences:** Single definition, no circular imports (data_pipes already imports from data_entities).

---

## 2026-04-30 Local scaffold config uses `provider_type: memory`, not local Delta paths
**Status:** Accepted
**Agent:** planner (Claude)
**Context:** Two options for local-first scaffold: (A) `provider_type: memory` (in-memory
  Spark tables, no filesystem), (B) local Delta paths like `/tmp/kindling-data/bronze`. Option
  A requires no filesystem setup; option B is closer to production but requires Delta JARs.
**Decision:** Use option A (`provider_type: memory`) for the default `env.local.yaml` overlay.
  Keep ABFSS config as a commented block. Memory provider is already registered as a builtin.
**Consequences:** Local runs are ephemeral (data lost on session end), which is fine for
  development. Developers who want persistence can uncomment the ABFSS block.

---

## 2026-04-30 WatermarkEntityFinder standalone binding strategy
**Status:** Accepted
**Agent:** planner (Claude)
**Context:** TASK-20260430-002 Item 1. `kindling run` fails in standalone mode because
  `WatermarkEntityFinder` is abstract with no concrete binding. Three options were evaluated:
  (A) CLI auto-binds before `run_datapipes`; (B) scaffold `app.py.j2` includes a stub binding;
  (C) `initialize_framework` auto-binds a `NullWatermarkEntityFinder` when `platform=standalone`.
**Decision:** Option C — `initialize_framework` binds `NullWatermarkEntityFinder` immediately
  after `initialize_platform_services()` returns, only when `platform == "standalone"` and only
  if no concrete binding already exists. The null impl raises `NotImplementedError` with a clear
  message if actually called, so it does not silently mask missing config.
**Consequences:** Standalone/local-dev "just works" without boilerplate in `app.py`. Production
  platforms are unaffected (they run Fabric/Synapse/Databricks, not standalone). Apps that need
  real watermarking must still bind their own `WatermarkEntityFinder` — same as today. The new
  component test (Item 6) catches any future regression.

---

## 2026-04-30 Debug print suppression: remove/route, no KINDLING_QUIET env var
**Status:** Accepted
**Agent:** planner (Claude)
**Context:** TASK-20260430-002 Item 2. 86 `print()` calls in `bootstrap.py` and 23 in
  `spark_config.py` violate the CONVENTIONS.md "no bare print() in library code" rule and
  flood `kindling run` output. Options: (A) remove all prints, (B) route to logger, (C) add
  `KINDLING_QUIET=1` env-var gate, (D) categorise and do A+B together.
**Decision:** Option D — categorise into four groups: (A) pre-logger startup announcements →
  delete; (B) operational diagnostics after logger is available → route to `logger.debug/info/
  warning/error`; (C) clearly leftover DEBUG/emoji prints → delete entirely; (D) intentional
  print-based fallback logger in `spark_config.py` → keep with explanatory comment. No
  `KINDLING_QUIET` env var is introduced — the structured logger is already configurable via
  `kindling.telemetry.logging.level`.
**Consequences:** CLI stdout is clean by default. All diagnostic information is still accessible
  via log level configuration. Conventions compliance restored.

---

## 2026-04-29 Agent platform assignment strategy
**Status:** Accepted
**Agent:** coordinator (Claude Code)
**Context:**
  Not all developers have all three agent platforms (Claude Code, Codex, Copilot).
  Agent roles are fixed; only platform routing varies.
**Decision:**
  Default platform assignments follow `.github/agents/*.agent.md` model fields:
  coordinator/planner/reviewer/security → Claude; implementer/tester → Codex;
  integrator → Copilot. If a platform is unavailable, coordinator re-routes to
  an available one. Roles and ACTIVE_TASK.md registry are unchanged.
**Consequences:**
  Tasks can always proceed regardless of which platforms are available.
  Sequential execution replaces parallel worktrees when Codex is unavailable.
