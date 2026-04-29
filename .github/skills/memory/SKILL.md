# /memory — Memory Bank Protocol Skill

Use this skill to read, update, or initialize memory banks.
Memory banks are the shared context that persists across all agent sessions.

## Memory Bank Files

| File | Purpose | Who Writes | Frequency |
|------|---------|-----------|-----------|
| `WORKSPACE.md` | Project context, tech stack, key paths | Human + coordinator | Setup + major changes |
| `ACTIVE_TASK.md` | Current task + handoff log | All agents | Every session |
| `DECISIONS.md` | Architecture decision log | planner, reviewer, coordinator | When decisions are made |
| `CONVENTIONS.md` | Coding standards | Human + coordinator | As standards evolve |

All files live in `.agent-memory/` at project root.

## Reading Memory (Start of Session)

Read in this order:
1. `WORKSPACE.md` — establishes what project this is
2. `ACTIVE_TASK.md` — establishes where work stands
3. `DECISIONS.md` — establishes what's already been decided
4. `CONVENTIONS.md` — establishes how to write code

If any of these files don't exist, alert the user:
> "Memory bank `[file]` not found. Recommend running /memory init
>  or having the human populate it before proceeding."

## Writing Memory (During/End of Session)

### ACTIVE_TASK.md
Append, don't overwrite. Always add to the Handoff Log section.
Update the Status field and Artifacts section.

### DECISIONS.md
Append only. Never delete or modify existing decisions.
To supersede a decision, add a new entry with `Status: Supersedes [DATE] [Title]`.

### CONVENTIONS.md
Add only. Flag proposed removals as `[DEPRECATED]` — don't delete.

### WORKSPACE.md
Only update if something fundamental changed (new service, removed dependency).
Treat this as a reference doc that changes rarely.

## Initializing a New Project

If `.agent-memory/` doesn't exist, create it and seed these files:

### WORKSPACE.md seed
```markdown
# Workspace: [Project Name]
**Updated:** [date]

## Project
[1-paragraph description]

## Tech Stack
- Language:
- Framework:
- Data layer:
- Key libraries:

## Repo Structure
[key directories and what they contain]

## Entry Points
- [main file / startup path]

## Test Command
`[how to run tests]`

## Build Command
`[how to build]`

## Off-Limits Paths
[paths agents should never modify without explicit human instruction]

## Active Agents
- coordinator (Claude)
- planner (Claude)
- implementer (Codex)
- reviewer (Claude)
- integrator (Copilot)
- tester (Codex)
- security (Claude)
```

### ACTIVE_TASK.md seed
```markdown
# Active Task: None
**Status:** IDLE

No active task. Activate coordinator with your request to begin.
```

### DECISIONS.md seed
```markdown
# Decision Log

[Decisions will be appended here as work proceeds]
```

### CONVENTIONS.md seed
```markdown
# Conventions

## Naming
[project naming standards]

## File Organization
[where things go]

## Patterns
[patterns we use]

## Anti-Patterns
[patterns we explicitly avoid]

## Testing
[testing requirements and standards]

## Comments
[when and how to comment]
```
