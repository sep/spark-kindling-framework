# Agent System

Multi-agent workflow across Claude Code, Codex (local + cloud), and VS Code Copilot.
All agents share memory banks, mailboxes, and handoff protocol.

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
| ship | Creates PR, polls Copilot auto-review, routes changes, signals ready-to-merge | No |

## Memory Banks (.agent-memory/)

| File | Purpose |
|------|---------|
| WORKSPACE.md | Project context. Fill in once. |
| ACTIVE_TASK.md | Task registry + handoff log. Agents append. |
| DECISIONS.md | Architecture decisions. Append-only. |
| CONVENTIONS.md | Coding standards. Agents enforce. |
| events.jsonl | Structured event log. Append-only. |
| escalations.md | Human review queue. |
| mailboxes/[role].md | Per-agent work queue. STATUS: IDLE/PENDING/IN_PROGRESS/ESCALATED |

## Chain

coordinator → planner → implementer → tester → reviewer → integrator → ship
  ship polls GitHub for Copilot auto-review, routes changes back if needed
  ship writes to escalations.md when PR is approved and ready for human merge
  CHANGES REQUESTED at any stage routes back to implementer

## Sessions (3 standing + cloud on demand)

| Session | Roles | Start |
|---------|-------|-------|
| Claude Code | coordinator, planner, reviewer, security, ship | paste .github/sessions/claude-code-session.md |
| Codex local | implementer, tester | paste .github/sessions/codex-local-session.md |
| VS Code | integrator | paste .github/sessions/vscode-session.md |
| Codex cloud | implementer (heavy/parallel) | see .github/sessions/codex-cloud-dispatch.md |

## Worktree Convention

- Feature branches: agent/TASK-YYYYMMDD-NNN/slug
- Worktree root: ../REPO-worktrees/
- Merge: feature → dev (squash via PR), dev → main (human only)
