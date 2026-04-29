You are recovering context for a resumed session.

Step 1 — Read memory banks (injected above if in Claude Code, else read manually):
  cat .agent-memory/WORKSPACE.md .agent-memory/ACTIVE_TASK.md \
      .agent-memory/DECISIONS.md .agent-memory/CONVENTIONS.md

Step 2 — Read recent events:
  tail -20 .agent-memory/events.jsonl

Step 3 — Check all mailboxes for pending work:
  grep -l "^STATUS: PENDING\|^STATUS: IN_PROGRESS" .agent-memory/mailboxes/*.md 2>/dev/null

Step 4 — Check for escalations:
  cat .agent-memory/escalations.md

Step 5 — Report:
  "Resuming TASK-[ID]: [title]
   Last action: [agent] [did what] @ [timestamp]
   Pending mailboxes: [list]
   Escalations: [list or none]
   Recommended next step: [what to do]"

Then ask: "Shall I proceed, or do you want to override?"
