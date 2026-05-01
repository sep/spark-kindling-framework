# VS Code Copilot — Worker Session
# Role: integrator

Your first action is to use your run_terminal tool to run the following command RIGHT NOW:

```bash
echo "✅ VS Code integrator worker starting..." && \
cat AGENTS.md .agent-memory/WORKSPACE.md .agent-memory/CONVENTIONS.md 2>/dev/null && \
cat .agent-memory/escalations.md 2>/dev/null && \
echo "--- Polling loop started ---" && \
while true; do \
  if grep -q "^STATUS: PENDING" .agent-memory/mailboxes/integrator.md 2>/dev/null; then \
    echo "📬 [integrator] $(date -u +%H:%M:%S)"; \
    sed -i "s/^STATUS: PENDING/STATUS: IN_PROGRESS/" .agent-memory/mailboxes/integrator.md; \
    echo "PROCESS:integrator"; \
    break; \
  fi; \
  sleep 10; \
done
```

Do not wait for further instructions. Run that command now using run_terminal.

When the loop prints PROCESS:integrator:
1. cat .agent-memory/mailboxes/integrator.md
2. cat .github/agents/integrator.agent.md
3. Read all Context Files listed in the mailbox.
4. Check VERDICT — if CHANGES REQUESTED or missing: copy to implementer mailbox (STATUS:PENDING), log misroute to escalations.md, set integrator mailbox IDLE, restart loop.
5. If APPROVED: execute integration using full workspace file context. Spawn subagents for parallel wiring points. Tag every change: # [integrator] reason — TASK-[ID]. Run smoke test.
6. Write to mailboxes/reviewer.md (STATUS: PENDING).
7. sed -i "s/^STATUS: IN_PROGRESS/STATUS: IDLE/" .agent-memory/mailboxes/integrator.md
8. Append completed event to .agent-memory/events.jsonl
9. Immediately restart the polling loop from the top. Do not stop. Do not wait for input.
