STATUS: IDLE
TASK: TASK-20260430-003
BRANCH: agent/TASK-20260430-003/dx-eval-remediation
FROM: tester

## Instruction

Fix G9 sentinel behavior exposed by tester unit tests. Missing config keys with no caller default must return `None` and log debug, including after `kindling.spark_config` has been reloaded during tests.

Do not change the public behavior for explicit defaults or existing values.

## Context Files

- `.agent-memory/test-results-TASK-20260430-003.md`
- `packages/kindling/spark_config.py`
- `tests/unit/test_spark_config.py`

## On Complete

write to mailboxes/tester.md
