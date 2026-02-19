# Agent Development Guide - Big Feature SOP

> Purpose: A repeatable operating procedure for delivering any major feature in this repo.
>
> Trigger phrase: "Do this for [next big feature]".
>
> When that phrase is used, follow this guide end-to-end unless the user explicitly overrides a step.

---

## 1) Scope and Intent

Use this SOP for any feature that is more than a one-file fix, and for any work that affects release, tests, or platform behavior.

A "big feature" usually has at least one of these:
- New module/package or significant architectural change
- Runtime behavior changes across platforms
- CI/build/release/deploy workflow changes
- New config surface or breaking/compatibility implications
- Multi-PR or multi-issue effort

---

## 2) Non-Negotiables

These happen every time unless the user says otherwise:
- Review existing open issues and priorities before starting implementation.
- Track work in GitHub Issues and GitHub Project (create/update items as needed).
- Update design/docs and implementation docs as part of delivery.
- Add/adjust tests for the changed behavior.
- Create a PR and address review feedback before merge.
- Verify merge state and release/version impact.
- Report status clearly: what changed, what passed, what is left.

---

## 3) Standard Delivery Lifecycle

### Phase 0 - Intake and Alignment

Objective: Confirm goal, scope, and constraints before coding.

Checklist:
- Restate the requested outcome in concrete terms.
- Identify dependencies, risks, and likely affected areas.
- Confirm branch strategy (feature branch vs direct-to-main).
- Confirm release expectations (none, patch, alpha, etc.).

Exit criteria:
- Clear implementation target and acceptance criteria.

### Phase 1 - Issue and Project Hygiene

Objective: Ensure planning artifacts exist and are current.

Checklist:
- Review related issues for duplicates/conflicts.
- Create issue(s) if missing (feature issue + follow-ups if needed).
- Add issue(s) to the active GitHub Project.
- Set/confirm priority, status, and ownership.
- Add a concise implementation plan comment in the issue.

Exit criteria:
- Work is represented in issues and project board with current status.

### Phase 2 - Design and Documentation Plan

Objective: Lock in design direction before broad edits.

Checklist:
- Review existing proposals/docs and align to current intent.
- Create/update proposal docs for non-trivial behavior changes.
- Document migration/compatibility behavior if any.
- Document config contract changes and examples.

Exit criteria:
- Design decisions and behavior are documented enough to implement.

### Phase 3 - Implementation

Objective: Deliver the feature with focused, reviewable changes.

Checklist:
- Implement in small, coherent commits.
- Keep runtime vs design-time boundaries explicit.
- Avoid hidden breaking changes; preserve compatibility when expected.
- Update entrypoints, packaging, scripts, and automation where needed.
- Ensure logs/errors are actionable and non-ambiguous.

Exit criteria:
- Feature behavior implemented and locally verifiable.

### Phase 4 - Verification

Objective: Prove correctness at the right test depth.

Checklist:
- Run targeted unit tests for touched code paths.
- Run integration/system tests where behavior risk warrants it.
- Run build smoke checks for produced artifacts.
- Validate docs/examples still match actual behavior.

Exit criteria:
- Relevant tests/build checks pass or known failures are documented with reason.

### Phase 5 - PR and Review

Objective: Land high-quality changes with complete context.

Checklist:
- Open PR with clear summary, risk, and validation evidence.
- Link issues and project item(s).
- Include explicit "what changed / why / how verified".
- Address reviewer comments (human + bot) with code/doc changes or rationale.
- Re-run required checks after each fix push.

Exit criteria:
- Required reviews/checks are green and comments are resolved.

### Phase 6 - Merge, Release, and Deploy

Objective: Complete delivery and publish artifacts safely.

Checklist:
- Merge according to repo policy.
- Confirm merge commit and branch cleanup.
- If version/release required:
  - bump versions consistently across affected packages
  - build artifacts
  - deploy required runtime artifacts
  - create GitHub release with expected assets
- Verify release artifacts and paths.

Exit criteria:
- Changes merged and, if requested, released/deployed successfully.

### Phase 7 - Post-Delivery Closure

Objective: Ensure tracking and docs reflect reality.

Checklist:
- Update issue status and close completed issues.
- Update parent/capability issues and project status columns.
- Note any follow-up tasks as explicit issues.
- Provide final summary to user with links and identifiers.

Exit criteria:
- Project/issue/docs state matches delivered implementation.

---

## 4) Required Reviews Per Big Feature

Review these every time:

### A) Product/Planning Review
- Related issues and priorities
- Dependencies and blockers
- GitHub Project placement and status

### B) Technical Review
- API/config compatibility
- Runtime vs design-time boundaries
- Error handling and telemetry impact
- Build/release automation impact

### C) Quality Review
- Unit/integration/system test relevance
- CI check coverage for changed surfaces
- Artifact smoke checks for packaged outputs

### D) Documentation Review
- Proposal docs
- Development guides
- README or usage docs
- Release notes impact

### E) Delivery Review
- Commit history clarity
- PR review comments fully addressed
- Merge/release/deploy status verified

---

## 5) Git and Branching Policy

Default flow:
- Start from up-to-date `main`.
- Create feature branch: `feature/<short-topic>`.
- Commit logical units with clear messages.
- Push branch and open PR.
- Merge after checks/reviews are complete.

When direct-to-main is requested:
- Explicitly confirm user intent.
- Still run all validation and review steps possible.
- Document any skipped guardrails.

Never:
- Rewrite shared history without explicit instruction.
- Revert unrelated user changes.
- Use destructive git commands unless explicitly requested.

---

## 6) Commit and PR Standards

Commit message format:
- `feat: ...`
- `fix: ...`
- `refactor: ...`
- `docs: ...`
- `test: ...`
- `chore: ...`

PR body must include:
- Summary
- Related issues
- Design notes/decisions
- Validation evidence (tests/build/deploy)
- Risk and rollback notes (if relevant)
- Any exceptions (for example, hooks bypassed due env constraints)

---

## 7) Testing and Validation Requirements

Minimum for big features:
- Targeted unit tests for changed logic
- Targeted integration tests for boundary/contract changes
- Build validation for touched package artifacts
- CI checks green before merge

When platform behavior changes:
- Run/verify impacted system tests
- Confirm logs/status from each affected platform

When packaging/release changes:
- Build all expected wheels
- Install smoke test of new artifacts (CLI help, imports, entrypoints)
- Verify release asset list

---

## 8) Documentation Requirements

Every big feature updates docs at two levels:

1. Design intent:
- Proposal or architecture doc updates
- Compatibility/migration notes

2. Operational usage:
- User/developer-facing commands and examples
- Build/deploy/release instructions

If docs and implementation differ, implementation is not done.

---

## 9) Versioning, Release, and Deploy Rules

When release is requested:
- Bump versions consistently for all changed/distributed packages.
- Align cross-package dependency floors (for example, CLI -> SDK).
- Build artifacts from the bumped version.
- Deploy runtime artifacts to required storage paths.
- Create release tag and attach expected wheel assets.
- Confirm release page contents and artifact names.

Release verification checklist:
- Tag exists and points to expected commit.
- Release contains all required wheel files.
- Deployed runtime artifacts are present in storage.
- Bootstrap/runtime paths are valid.

---

## 10) Blockers and Escalation

If blocked:
- Document blocker in issue and PR thread.
- Propose concrete options and recommended path.
- Continue parallelizable tasks (docs/tests/refactors) while blocked.

If tooling/network constraints prevent a step:
- State exact command + failure reason.
- Use the safest fallback.
- Record deviation in PR notes.

---

## 11) Execution Checklist (Copy/Paste)

Use this checklist for every new big feature.

- [ ] Confirm scope, constraints, acceptance criteria
- [ ] Review related issues and priorities
- [ ] Create/update feature issue(s)
- [ ] Add/update GitHub Project item(s)
- [ ] Draft/update proposal/design docs
- [ ] Create feature branch (unless direct-to-main requested)
- [ ] Implement feature changes
- [ ] Add/update tests (unit/integration/system as needed)
- [ ] Run local validation (tests/build/smoke)
- [ ] Update user/developer docs
- [ ] Open PR with complete summary + evidence
- [ ] Address review comments (human + bot)
- [ ] Ensure required checks are green
- [ ] Merge PR and verify merge state
- [ ] Bump version(s) if release needed
- [ ] Build/deploy/release artifacts if requested
- [ ] Verify release assets and deploy paths
- [ ] Update issues/project status to done
- [ ] Provide final delivery report

---

## 12) Suggested Command Set

Issue and project workflow:
```bash
gh issue list --state open --repo sep/spark-kindling-framework
gh issue create --repo sep/spark-kindling-framework --title "..." --body-file ...
gh project list --owner sep
gh project item-list <project-number> --owner sep --format json
```

Branch and PR workflow:
```bash
git checkout main && git pull
git checkout -b feature/<topic>
git add ... && git commit -m "feat: ..."
git push -u origin feature/<topic>
gh pr create --base main --head feature/<topic> --title "..." --body-file ...
gh pr checks <pr-number>
gh pr view <pr-number> --comments
gh pr merge <pr-number> --merge --delete-branch
```

Build/deploy/release workflow (current repo conventions):
```bash
poe version --bump_type patch
poe build
poe deploy
gh release create v<version> dist/*.whl --title "Release v<version>" --generate-notes
```

---

## 13) Definition of Done (Big Feature)

A big feature is done only when all are true:
- Code implemented and tested for expected scope
- Docs/proposals updated to match behavior
- Issues and project board accurately reflect completion
- PR reviews addressed, checks green, and changes merged
- Version/release/deploy steps completed if requested
- Final summary with links/identifiers is provided

---

## 14) Final Status Report Template

Use this exact structure in completion updates:

1. Outcome
- What was delivered

2. Code and Docs
- Key files changed
- Key docs/proposals updated

3. Validation
- Tests/checks run and results

4. GitHub Tracking
- Issue(s)
- Project item status
- PR link and merge commit

5. Release/Deploy (if applicable)
- Version/tag
- Assets published
- Deploy locations

6. Follow-ups
- Remaining work as explicit next issues

---

Last updated: 2026-02-19
