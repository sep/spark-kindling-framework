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

## Memory Banks (.agent-memory/)

| File | Purpose | Rule |
|------|---------|------|
| WORKSPACE.md | Project context, stack, commands | Fill in once |
| ACTIVE_TASK.md | Task registry + handoff log | Agents append |
| DECISIONS.md | Architecture decisions | Append-only, never delete |
| CONVENTIONS.md | Coding standards | Agents enforce |
| events.jsonl | Structured event log | Append-only |
| escalations.md | Human review queue | Agents write, human resolves |
| mailboxes/[role].md | Per-agent work queue | STATUS: IDLE/PENDING/IN_PROGRESS/ESCALATED |

## Mailbox Protocol

Dispatcher writes:
  STATUS: PENDING
  TASK: TASK-[ID]
  FROM: [role]
  RECEIVED: [ISO]
  ## Instruction / ## Context Files / ## On Complete

Recipient polls, sets IN_PROGRESS, does work, sets IDLE, writes to next mailbox.

## Invocation

| Tool | Roles handled | How to start |
|------|--------------|--------------|
| Claude Code | coordinator, planner, reviewer, security | Paste `.github/sessions/claude-code-session.md` |
| Codex local | implementer, tester | Paste `.github/sessions/codex-local-session.md` |
| VS Code Copilot | integrator | Paste `.github/sessions/vscode-session.md` |
| Codex cloud | implementer (heavy/parallel) | See `.github/sessions/codex-cloud-dispatch.md` |

Three standing sessions total. Each reads the role doc when it picks up work.
You only talk to the coordinator session.

## Standing Sessions (VS Code / Codex local)

Each agent runs as a standing session polling its mailbox every 10s.
Coordinator is your only interface — all requests go through it.
Reviewer pauses and writes to escalations.md when CHANGES REQUESTED.
Human resolves escalations, then coordinator re-dispatches.

## Worktree Convention

- Integration branch: dev
- Feature branches: agent/TASK-YYYYMMDD-NNN/slug
- Worktree root: ../REPO-worktrees/
- Merge: feature → dev (squash), dev → main (human only)
