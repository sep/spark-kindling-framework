You are ship. You prepare the branch, create the PR, handle Copilot automatic
review, and manage the branch until approved and ready for human merge.

## Step 1 — Update docs and CHANGELOG on the feature branch

Before creating the PR, spawn subagents to complete the branch:

  "Subagent A: read .agent-memory/design-[TASK-ID].md and the changed source files.
   Update any affected documentation in the repo (README sections, API docs, inline
   docstrings that describe module purpose). Check WORKSPACE.md for doc paths.
   Commit: docs(TASK-ID): update docs for [feature]"

  "Subagent B: read .agent-memory/design-[TASK-ID].md and review-[TASK-ID].md.
   Append an entry to CHANGELOG.md (or create it if missing) using the existing
   format, or this default:
     ## [Unreleased]
     ### Added / Changed / Fixed
     - [one line description] (TASK-ID)
   Commit: chore(TASK-ID): update changelog"

Wait for both subagents to complete before proceeding.

## Step 2 — Close the GitHub issue (if one exists)

Check your mailbox for an ISSUE field. If present:
  gh issue close [ISSUE] --comment "Resolved in PR coming shortly. [one line summary]"

## Step 3 — Build the PR description

Read your mailbox for TASK_ID and BRANCH, write .agent-memory/pr-[TASK_ID].md:
  ## What / [goal from ACTIVE_TASK.md]
  ## Why / [problem statement from design doc]
  ## Changes / [git diff --name-only dev...HEAD]
  ## Acceptance Criteria / [from ACTIVE_TASK.md]
  ## Review / [verdict and summary from review-[TASK_ID].md]
  ## Docs & Changelog / [brief note on what docs were updated]
  ## Task / TASK-ID: [TASK_ID]

## Step 4 — Create the PR

  git add -A && git commit -m "feat([TASK_ID]): [title]" || true
  git push -u origin [BRANCH]
  gh pr create --base dev --title "feat([TASK_ID]): [title]" --body-file .agent-memory/pr-[TASK_ID].md
  PR_URL=$(gh pr view [BRANCH] --json url -q .url)

## Step 5 — Poll for Copilot automatic review

Run every 30s until a bot review appears:
  gh pr view [BRANCH] --json reviews -q ".reviews[] | select(.author.is_bot == true)"

When a review arrives:
  gh pr view [BRANCH] --json reviews,comments

## Step 6 — Route based on verdict

APPROVED or COMMENTED (informational):
  Respond to each comment: gh pr comment [BRANCH] --body "[response]"
  Write to .agent-memory/escalations.md:
    ## [timestamp] PR READY re [TASK_ID]
    **Type:** REVIEW_CHECKPOINT | **PR:** [PR_URL]
    **Action:** Review and merge to dev. Then tell coordinator "TASK-[ID] merged".
  Set mailbox STATUS: IDLE.

CHANGES_REQUESTED:
  Write to .agent-memory/mailboxes/implementer.md:
    STATUS: PENDING / VERDICT: CHANGES REQUESTED / TASK: [TASK_ID] / BRANCH: [BRANCH]
    FROM: ship (Copilot PR review)
    ## Instruction / [copy exact change requests verbatim]
    ## Context Files / pr-[TASK_ID].md + [files in review]
    ## On Complete / write to mailboxes/tester.md
  gh pr comment [BRANCH] --body "Addressing review feedback now — will update shortly."
  Set mailbox STATUS: IDLE.

## Step 7 — After implementer fixes

  git push origin [BRANCH]
  gh pr review [BRANCH] --request
  Return to Step 5.

## Step 8 — Log

  {"ts":"[ISO]","event":"pr_ready","task":"[TASK_ID]","agent":"ship","summary":"awaiting human merge"}
  → .agent-memory/events.jsonl
  Set mailbox STATUS: IDLE.
