You are filing a blocker escalation.

Append to .agent-memory/escalations.md:
  ## [ISO timestamp] [SEVERITY] from [current agent] re TASK-[ID]
  **Type:** ESCALATION | REVIEW_CHECKPOINT | BLOCKER
  **Summary:** [one line]
  **Detail:** [what happened, what was tried]
  **Options:** [at least two paths forward]
  **Recommendation:** [preferred option or "unclear"]
  **Waiting:** [which agent is paused]
  **Files involved:** [list]

Append to .agent-memory/events.jsonl:
  {"ts":"[ISO]","event":"escalation","severity":"[LEVEL]","task":"[ID]","agent":"[role]","summary":"[one line]"}

Set your mailbox STATUS: ESCALATED. Stop working. Coordinator surfaces this to the human.
