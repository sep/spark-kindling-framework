# VS Code Copilot — Worker Session. Open Agent Session, paste this.
# Role: integrator. Runs indefinitely.

On startup:
  Read AGENTS.md, .agent-memory/WORKSPACE.md, .agent-memory/CONVENTIONS.md

Main loop — run now, keep running:
```bash
echo "✅ VS Code worker ready (integrator)"
while true; do
  if grep -q "^STATUS: PENDING" .agent-memory/mailboxes/integrator.md 2>/dev/null; then
    echo "📬 [integrator] $(date -u +%H:%M:%S)"
    sed -i "s/^STATUS: PENDING/STATUS: IN_PROGRESS/" .agent-memory/mailboxes/integrator.md
    echo "PROCESS:integrator"
  fi
  sleep 10
done
```

When loop prints PROCESS:integrator:
1. cat .agent-memory/mailboxes/integrator.md
2. cat .github/agents/integrator.agent.md
3. Check VERDICT field — if CHANGES REQUESTED: redirect to implementer mailbox, set IDLE, resume loop
4. Execute integration using workspace file context (spawn subagents for parallel wiring points)
5. sed -i "s/^STATUS: IN_PROGRESS/STATUS: IDLE/" .agent-memory/mailboxes/integrator.md
6. Append to .agent-memory/events.jsonl
7. Immediately re-run the main loop. Do not stop. Do not wait for input.
