# Codex Local — Worker Session
# Roles: implementer, tester

Your first action is to run the following command in the terminal RIGHT NOW:

```bash
echo "✅ Codex worker starting..." && \
cat AGENTS.md .agent-memory/WORKSPACE.md .agent-memory/CONVENTIONS.md 2>/dev/null && \
echo "--- Polling loop started ---" && \
while true; do \
  FOUND=""; \
  for ROLE in implementer tester; do \
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
  sleep 10; \
done
```

Do not wait for further instructions. Run that command now.

When the loop prints PROCESS:[role]:
1. cat .agent-memory/mailboxes/$FOUND.md
2. cat .agents/skills/$FOUND.md
3. Read all Context Files listed in the mailbox.
4. Execute the role. Spawn subagents for parallel module work.
5. sed -i "s/^STATUS: IN_PROGRESS/STATUS: IDLE/" .agent-memory/mailboxes/$FOUND.md
6. Append completed event to .agent-memory/events.jsonl
7. Immediately restart the polling loop from the top. Do not stop. Do not wait for input.
