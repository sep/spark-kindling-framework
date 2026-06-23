# Claude Code Context

## First Action Every Session

```bash
git status --short --branch
git worktree list
```

Read `AGENTS.md` and any task-relevant project documentation before writing
code.

## Agent System

See `AGENTS.md`. Slash commands: `/coordinator`, `/planner`, `/implementer`,
`/reviewer`, `/integrator`, `/tester`, `/security`, `/prime`, `/escalate`.

Use the current branch, repository documentation, and handoff notes as the
source of truth for active work.

## Key Rules

- Preserve user changes you did not make.
- Keep task context in the conversation, commits, or handoff notes.
- Record durable project decisions in the appropriate documentation.
- Run targeted tests, linters, or builds when code changes.

## Worktree Root

`.worktrees/`

## Session Completion

When ending a work session:

1. Record any remaining follow-up work.
2. Run quality gates if code changed.
3. Commit completed changes when appropriate.
4. Push committed work to the remote branch:

   ```bash
   git pull --rebase
   git push
   git status
   ```

5. Verify the branch is clean or clearly note any intentional uncommitted work.
6. Hand off enough context for the next session.
