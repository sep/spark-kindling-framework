# Global Agent Instructions

## Mandatory — every session

Work from the current repository state, project documentation, and git branch.

Start each session by checking the branch state:

```bash
git status --short --branch
```

## Core Rules

- Read relevant source and documentation before editing.
- Preserve user changes you did not make.
- Keep changes scoped to the task.
- Run targeted tests, linters, or builds when code changes.
- Document follow-up work in the appropriate docs or handoff notes.
- Commit and push completed changes when appropriate.

## Quick Reference

```bash
git status --short --branch
git diff
git pull --rebase
git push
```

## Workflow

1. Inspect current branch state.
2. Read the relevant source and documentation.
3. Make the focused change.
4. Run targeted verification.
5. Commit and push completed work.

## Context Loading

Use `AGENTS.md` and the repository docs as the source of truth for current
workflow guidance.
