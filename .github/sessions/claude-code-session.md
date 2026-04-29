# Claude Code — Worker Session. Paste into Claude Code terminal.
# Roles: coordinator, planner, reviewer, security, ship. Runs indefinitely.

On startup:
  cat AGENTS.md .agent-memory/WORKSPACE.md .agent-memory/CONVENTIONS.md
  cat .agent-memory/escalations.md 2>/dev/null
  gh auth status 2>/dev/null || echo "⚠️  gh not authenticated — ship role will fail. Run: gh auth login"

Main loop — run now, keep running:
```bash
echo "✅ Claude Code worker ready (coordinator|planner|reviewer|security|ship)"
while true; do
  FOUND=""
  for ROLE in coordinator planner reviewer security ship; do
    if grep -q "^STATUS: PENDING" ".agent-memory/mailboxes/${ROLE}.md" 2>/dev/null; then
      FOUND="$ROLE"; break
    fi
  done
  if [ -n "$FOUND" ]; then
    echo "📬 [$FOUND] $(date -u +%H:%M:%S)"
    sed -i "s/^STATUS: PENDING/STATUS: IN_PROGRESS/" ".agent-memory/mailboxes/${FOUND}.md"
    echo "PROCESS:$FOUND"
  fi
  if grep -q "^## " .agent-memory/escalations.md 2>/dev/null; then
    echo "🚨 ESCALATION — check .agent-memory/escalations.md"
  fi
  sleep 10
done
```

When loop prints PROCESS:[role]:
1. cat .agent-memory/mailboxes/$FOUND.md
2. cat .claude/commands/$FOUND.md
3. Read all Context Files listed
4. Execute the role (spawn subagents for parallel work)
5. sed -i "s/^STATUS: IN_PROGRESS/STATUS: IDLE/" .agent-memory/mailboxes/$FOUND.md
6. Append to .agent-memory/events.jsonl
7. Immediately re-run the main loop. Do not stop. Do not wait for input.
