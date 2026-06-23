# Agent System

Kindling agents should work from the repository state, the project documentation,
and the current git branch. Keep changes focused, verified, and easy for the
next contributor to pick up.

## Workflow

- Read the relevant source and documentation before making changes.
- Use git status and diffs to understand the current branch state.
- Preserve user changes you did not make.
- Keep implementation changes scoped to the task at hand.
- Run targeted tests, linters, or builds when code changes.
- Document follow-up work in the appropriate project documentation or handoff
  notes.

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
