# Human Review Queue

Agents write here when they need human input. Coordinator surfaces these to you.
Resolve by writing a decision to DECISIONS.md, updating the relevant mailbox, and
removing (or striking through) the escalation entry.

<!-- ## [ISO timestamp] [SEVERITY] from [agent] re TASK-[ID]
**Type:** ESCALATION | REVIEW_CHECKPOINT | BLOCKER
**Summary:** | **Detail:** | **Options:** | **Recommendation:** | **Waiting:** -->

## 2026-04-29T20:42:00Z REVIEW_CHECKPOINT from ship re TASK-20260429-001
**Type:** REVIEW_CHECKPOINT
**PR:** https://github.com/sep/spark-kindling-framework/pull/77
**Action:** Review and merge PR #77 to main. Then tell coordinator "TASK-20260429-001 merged" to trigger cleanup.
**Copilot verdict:** COMMENTED — no changes required. 4 low-confidence findings responded to and tracked as follow-up issues #78–#81.
