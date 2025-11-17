---
applyTo: '**'
---

# Agent Behavior Preferences

## CRITICAL: First Actions (Do These Every Time)
1. Read this memory file
2. Read project.instruction.md for technical context
3. State what you're about to do, then DO it immediately

## Code Execution Rules (HIGHEST PRIORITY)

### FORBIDDEN - Never Do These:
- ❌ Creating temporary test scripts or example files
- ❌ Writing code to temp locations to "test" it
- ❌ Asking permission before taking action (just do it)
- ❌ Ending turn after saying "I will do X" without actually doing X
- ❌ Using `if __name__ == "__main__":` in Kindling test apps
- ❌ Using `if __name__ == "__main__":` in Kindling test apps

### REQUIRED - Always Do These:
- ✅ Write ALL code changes directly to actual project files
- ✅ Execute code using runCommands
- ✅ Execute code without cd'ing into the workspace. Prioritize using commands that don't require approval.
- ✅ Show updated todo list after completing each step
- ✅ Keep working until todo list is 100% complete
- ✅ Test your changes before considering work done

## Communication Style
- Casual and direct - "Checking memory first...", "Writing to src/main.py now..."
- State what you're doing in one sentence before doing it
- No excessive explanations or preamble

## Work Style
- Autonomous - solve problems completely before returning control
- When user says "continue/resume", check todo list and continue from incomplete step
- Use markdown todo lists wrapped in triple backticks (never HTML)
- Check off items with `[x]` as you complete them

## Tool Preferences
- Use Poe the Poet (poethepoet) for builds: `poe build`, `poe deploy-fabric`, etc.
- NOT direct Poetry commands
- System tests take several minutes - let them run to completion

## Architecture Preferences
- **Platform Consistency Over Directness**: All three platforms (Fabric, Synapse, Databricks) should follow the same patterns
  - Synapse creates job definitions (stored in memory mapping) just like Fabric and Databricks
  - All platforms clean up job definitions AND data-apps after tests complete
  - Tests use `_cleanup_test()` in finally blocks to ensure cleanup happens

## CRITICAL STORAGE ARCHITECTURE (READ THIS FIRST!)

### ALL ARTIFACTS ARE IN ADLS GEN2 (ABFSS)
- **ALL platforms store artifacts, logs, and data-apps in ABFSS storage**
- **Storage path**: `abfss://{container}@{storage_account}.dfs.core.windows.net/`
- **This includes**: Python wheels, data-app files, Spark diagnostic logs, cluster logs

### Fabric Lakehouse Shortcut Pattern
- **Runtime (Spark jobs)**: Use lakehouse shortcut `Files/artifacts` → points to ABFSS container
- **Python API/SDK**: Read directly from ABFSS using Azure Storage SDK
- **❌ NEVER use OneLake API** for reading logs/artifacts - OneLake is runtime-only
- **Example**: Job uses `Files/artifacts/logs/`, API reads `abfss://container/logs/`

### Log Storage - ALL PLATFORMS
- **Databricks**: Cluster logs → UC Volume → backed by ABFSS `logs/` path
- **Synapse**: Diagnostic emitters → write to ABFSS `logs/{workspace}.{pool}.{batch}/`
- **Fabric**: Diagnostic emitters → write to ABFSS `logs/{workspace}.{run}/`
- **API reads logs**: Always from ABFSS storage using Azure Storage SDK, never from platform APIs

### CRITICAL: Diagnostic Log Propagation Delays
- **Fabric & Synapse**: Diagnostic emitters write logs **asynchronously** to ABFSS storage
- **Delay**: Logs can appear 7-26 minutes AFTER job status shows "Completed"
- **Solution**: Retry log fetching with 30s intervals for up to 6 attempts (3 min total)
- **Databricks**: Uses Jobs API - logs available immediately (no delay)
- **Test Pattern**: Check for TEST_ID markers in logs to validate they contain actual application output

### Rule of Thumb
- **Runtime access**: Use platform-native paths (UC Volumes, lakehouse shortcuts)
- **API/SDK access**: Always use ABFSS direct paths with Azure Storage SDK
- **Never mix**: Don't use OneLake API for what should be ABFSS operations

## Project Context
- Framework: Kindling (multi-platform Spark data lakehouse)
- Platforms: Synapse, Databricks, Fabric
- See project.instruction.md for technical details
