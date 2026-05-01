# Codex Cloud — fire tasks explicitly after coordinator writes a mailbox.

## Single task
codex exec "Read AGENTS.md and .agents/skills/implementer.md. \
Read .agent-memory/mailboxes/implementer.md and execute the task. \
Follow all routing instructions in the role doc."

## Parallel worktrees
git worktree add ../[repo]-worktrees/TASK-001-auth agent/TASK-001-auth
git worktree add ../[repo]-worktrees/TASK-001-data agent/TASK-001-data

codex exec --dir ../[repo]-worktrees/TASK-001-auth \
  "Read AGENTS.md .agents/skills/implementer.md. Implement per design doc section 2."
codex exec --dir ../[repo]-worktrees/TASK-001-data \
  "Read AGENTS.md .agents/skills/implementer.md. Implement per design doc section 3."

## Check completion
codex resume
tail -5 .agent-memory/events.jsonl
cat .agent-memory/mailboxes/tester.md
