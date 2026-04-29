# Codex Local — Worker Session. Run: codex, then paste this.
# Roles: implementer, tester. Runs indefinitely.

On startup:
  cat AGENTS.md .agent-memory/WORKSPACE.md .agent-memory/CONVENTIONS.md

Main loop — run now, keep running:
```bash
echo "✅ Codex worker ready (implementer|tester)"
while true; do
  FOUND=""
  for ROLE in implementer tester; do
    if grep -q "^STATUS: PENDING" ".agent-memory/mailboxes/${ROLE}.md" 2>/dev/null; then
      FOUND="$ROLE"; break
    fi
  done
  if [ -n "$FOUND" ]; then
    echo "📬 [$FOUND] $(date -u +%H:%M:%S)"
    sed -i "s/^STATUS: PENDING/STATUS: IN_PROGRESS/" ".agent-memory/mailboxes/${FOUND}.md"
    echo "PROCESS:$FOUND"
  fi
  sleep 10
done
```

When loop prints PROCESS:[role]:
1. cat .agent-memory/mailboxes/$FOUND.md
2. cat .agents/skills/$FOUND.md
3. Read all Context Files listed
4. Execute the role (spawn subagents for parallel module work)
5. sed -i "s/^STATUS: IN_PROGRESS/STATUS: IDLE/" .agent-memory/mailboxes/$FOUND.md
6. Append to .agent-memory/events.jsonl
7. Immediately re-run the main loop. Do not stop. Do not wait for input.
