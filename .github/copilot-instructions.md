# Global Agent Instructions

## Mandatory — every session

Read memory banks before starting any task:
  .agent-memory/WORKSPACE.md
  .agent-memory/ACTIVE_TASK.md
  .agent-memory/DECISIONS.md
  .agent-memory/CONVENTIONS.md

Check .agent-memory/mailboxes/[your-role].md for pending work.

## Handoff format

Append to .agent-memory/ACTIVE_TASK.md when handing off:
  ## Handoff: [from] → [to] @ [timestamp]
  **Did:** | **Touched:** | **Decided:** | **Need from you:** | **Blockers:**

## Mailbox dispatch

Write to next agent mailbox:
  STATUS: PENDING
  TASK: / FROM: / RECEIVED: / ## Instruction / ## Context Files / ## On Complete

## Conventions

Never deviate from .agent-memory/CONVENTIONS.md without a DECISIONS.md entry first.
