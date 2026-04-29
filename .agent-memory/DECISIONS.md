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
