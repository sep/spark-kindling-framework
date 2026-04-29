# Claude Code Context

## First action every session

```bash
cat .agent-memory/WORKSPACE.md .agent-memory/ACTIVE_TASK.md \
    .agent-memory/DECISIONS.md .agent-memory/CONVENTIONS.md
```

## Agent system

See AGENTS.md. Slash commands: /coordinator /planner /implementer
/reviewer /integrator /tester /security /prime /escalate

## Key rule

Follow .agent-memory/CONVENTIONS.md exactly.
Deviate only after writing a decision to .agent-memory/DECISIONS.md.

## Worktree root

../kindling-worktrees/
