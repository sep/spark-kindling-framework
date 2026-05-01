# Claude Code — Worker Session
# Roles: coordinator, planner, reviewer, security, ship

Your first action is to run the following command in the terminal RIGHT NOW:

```bash
echo "✅ Claude Code worker starting..." && \
cat AGENTS.md .agent-memory/WORKSPACE.md .agent-memory/CONVENTIONS.md 2>/dev/null && \
cat .agent-memory/escalations.md 2>/dev/null && \
{ gh auth status 2>/dev/null || echo "⚠️  gh not authenticated — run: gh auth login"; } && \
echo "--- Polling loop started ---" && \
while true; do \
  FOUND=""; \
  for ROLE in coordinator planner reviewer security ship; do \
    if grep -q "^STATUS: PENDING" ".agent-memory/mailboxes/${ROLE}.md" 2>/dev/null; then \
      FOUND="$ROLE"; break; \
    fi; \
  done; \
  if [ -n "$FOUND" ]; then \
    echo "📬 [$FOUND] $(date -u +%H:%M:%S)"; \
    sed -i "s/^STATUS: PENDING/STATUS: IN_PROGRESS/" ".agent-memory/mailboxes/${FOUND}.md"; \
    echo "PROCESS:$FOUND"; \
    break; \
  fi; \
  grep -q "^## " .agent-memory/escalations.md 2>/dev/null && echo "🚨 ESCALATION — check .agent-memory/escalations.md"; \
  sleep 10; \
done
```

Do not wait for further instructions. Run that command now.

When the loop prints PROCESS:[role]:
1. cat .agent-memory/mailboxes/$FOUND.md
2. cat .claude/commands/$FOUND.md
3. Read all Context Files listed in the mailbox.
4. Execute the role. Spawn subagents for parallel work.
5. sed -i "s/^STATUS: IN_PROGRESS/STATUS: IDLE/" .agent-memory/mailboxes/$FOUND.md
6. Append completed event to .agent-memory/events.jsonl
7. Immediately restart the polling loop from the top. Do not stop. Do not wait for input.
