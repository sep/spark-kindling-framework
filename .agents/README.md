# Codex Skills

## Local (interactive)
codex "Use the coordinator skill: [task description]"
codex "Use the prime skill"   # context recovery

## Local (standing session — polls mailbox)
codex  # then paste the contents of .github/sessions/[role]-startup.md

## Cloud (fire and forget)
codex exec "Read .agent-memory/mailboxes/implementer.md. Use the implementer skill to complete the task. Write results per the On Complete instruction."
codex exec "Read .agent-memory/mailboxes/tester.md. Use the tester skill."

## Resume a previous session
codex resume       # picker of recent sessions
codex resume --last

## Config hint (~/.codex/config.toml)
# project_doc_fallback_filenames = ["AGENTS.md", "CLAUDE.md"]
