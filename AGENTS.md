# Agent System

Multi-agent workflow across Claude Code, Codex (local + cloud), and VS Code Copilot.
All agents share the same memory banks, mailboxes, and handoff protocol.

## Agents

| Agent | Role | Writes Code? |
|-------|------|-------------|
| coordinator | Decomposes tasks, dispatches via mailboxes, monitors escalations | No |
| planner | Design docs, architecture, tradeoff analysis | No |
| implementer | Builds code from design docs | Yes |
| reviewer | Correctness, convention compliance, design fidelity | No |
| integrator | Wires modules — imports, DI, config, index files | Yes (integration only) |
| tester | Test generation and execution | Yes (test files only) |
| security | Security review, OSS license and secret hygiene | No |
| ship | Creates PR, polls for Copilot auto-review, routes changes, signals ready-to-merge | No |

## Memory Banks (.agent-memory/)

| File | Purpose | Rule |
|------|---------|------|
| WORKSPACE.md | Project context, stack, commands | Fill in once |
| ACTIVE_TASK.md | Task registry + handoff log | Agents append |
| DECISIONS.md | Architecture decisions | Append-only, never delete |
| CONVENTIONS.md | Coding standards | Agents enforce |
| events.jsonl | Structured event log | Append-only |
| escalations.md | Human review queue | Agents write, human resolves |
| mailboxes/[role].md | Per-agent work queue (coordinator/planner/implementer/reviewer/integrator/tester/security/ship) | STATUS: IDLE/PENDING/IN_PROGRESS/ESCALATED |

## Mailbox Protocol

Dispatcher writes:
  STATUS: PENDING
  TASK: TASK-[ID]
  FROM: [role]
  RECEIVED: [ISO]
  ## Instruction / ## Context Files / ## On Complete

Recipient polls, sets IN_PROGRESS, does work, sets IDLE, writes to next mailbox.

## Invocation

| Tool | How |
|------|-----|
| Claude Code | /coordinator, /planner, /implementer, etc. |
| Codex local | codex "Use the coordinator skill: [task]" |
| Codex cloud | codex exec "Use the implementer skill. Read mailboxes/implementer.md for task." |
| VS Code | Open session → paste from .github/sessions/[role]-startup.md |

## Standing Sessions (VS Code / Codex local)

Each agent runs as a standing session polling its mailbox every 10s.
Coordinator is your only interface — all requests go through it.
Reviewer pauses and writes to escalations.md when CHANGES REQUESTED.
Human resolves escalations, then coordinator re-dispatches.

## Worktree Convention

- Full chain: coordinator → planner → implementer → tester → reviewer → integrator → ship
- ship polls GitHub for Copilot auto-review, routes changes back through chain if needed
- ship writes to escalations.md when PR is approved and ready for human merge
- Integration branch: dev
- Feature branches: agent/TASK-YYYYMMDD-NNN/slug
- Worktree root: ../REPO-worktrees/
- Merge: feature → dev (squash), dev → main (human only)


<!-- BEGIN BEADS INTEGRATION v:1 profile:full hash:f65d5d33 -->
## Issue Tracking with bd (beads)

**IMPORTANT**: This project uses **bd (beads)** for ALL issue tracking. Do NOT use markdown TODOs, task lists, or other tracking methods.

### Why bd?

- Dependency-aware: Track blockers and relationships between issues
- Git-friendly: Dolt-powered version control with native sync
- Agent-optimized: JSON output, ready work detection, discovered-from links
- Prevents duplicate tracking systems and confusion

### Quick Start

**Check for ready work:**

```bash
bd ready --json
```

**Create new issues:**

```bash
bd create "Issue title" --description="Detailed context" -t bug|feature|task -p 0-4 --json
bd create "Issue title" --description="What this issue is about" -p 1 --deps discovered-from:bd-123 --json
```

**Claim and update:**

```bash
bd update <id> --claim --json
bd update bd-42 --priority 1 --json
```

**Complete work:**

```bash
bd close bd-42 --reason "Completed" --json
```

### Issue Types

- `bug` - Something broken
- `feature` - New functionality
- `task` - Work item (tests, docs, refactoring)
- `epic` - Large feature with subtasks
- `chore` - Maintenance (dependencies, tooling)

### Priorities

- `0` - Critical (security, data loss, broken builds)
- `1` - High (major features, important bugs)
- `2` - Medium (default, nice-to-have)
- `3` - Low (polish, optimization)
- `4` - Backlog (future ideas)

### Workflow for AI Agents

1. **Check ready work**: `bd ready` shows unblocked issues
2. **Claim your task atomically**: `bd update <id> --claim`
3. **Work on it**: Implement, test, document
4. **Discover new work?** Create linked issue:
   - `bd create "Found bug" --description="Details about what was found" -p 1 --deps discovered-from:<parent-id>`
5. **Complete**: `bd close <id> --reason "Done"`

### Quality
- Use `--acceptance` and `--design` fields when creating issues
- Use `--validate` to check description completeness

### Lifecycle
- `bd defer <id>` / `bd supersede <id>` for issue management
- `bd stale` / `bd orphans` / `bd lint` for hygiene
- `bd human <id>` to flag for human decisions
- `bd formula list` / `bd mol pour <name>` for structured workflows

### Auto-Sync

bd automatically syncs via Dolt:

- Each write auto-commits to Dolt history
- Use `bd dolt push`/`bd dolt pull` for remote sync
- No manual export/import needed!

### Important Rules

- ✅ Use bd for ALL task tracking
- ✅ Always use `--json` flag for programmatic use
- ✅ Link discovered work with `discovered-from` dependencies
- ✅ Check `bd ready` before asking "what should I work on?"
- ❌ Do NOT create markdown TODO lists
- ❌ Do NOT use external issue trackers
- ❌ Do NOT duplicate tracking systems

For more details, see README.md and docs/QUICKSTART.md.

## Session Completion

**When ending a work session**, you MUST complete ALL steps below. Work is NOT complete until `git push` succeeds.

**MANDATORY WORKFLOW:**

1. **File issues for remaining work** - Create issues for anything that needs follow-up
2. **Run quality gates** (if code changed) - Tests, linters, builds
3. **Update issue status** - Close finished work, update in-progress items
4. **PUSH TO REMOTE** - This is MANDATORY:
   ```bash
   git pull --rebase
   bd dolt push
   git push
   git status  # MUST show "up to date with origin"
   ```
5. **Clean up** - Clear stashes, prune remote branches
6. **Verify** - All changes committed AND pushed
7. **Hand off** - Provide context for next session

**CRITICAL RULES:**
- Work is NOT complete until `git push` succeeds
- NEVER stop before pushing - that leaves work stranded locally
- NEVER say "ready to push when you are" - YOU must push
- If push fails, resolve and retry until it succeeds

<!-- END BEADS INTEGRATION -->
