# Agentic Development Guide - Kindling Framework Roadmap

> **Purpose:** Instructions for AI agents (and developers) on how to work with the Kindling Framework roadmap and deliver high-quality implementations.

---

## Table of Contents

1. [Roadmap Structure](#roadmap-structure)
2. [Development Workflow](#development-workflow)
3. [Picking Work](#picking-work)
4. [Implementation Standards](#implementation-standards)
5. [Testing Requirements](#testing-requirements)
6. [Documentation Requirements](#documentation-requirements)
7. [Pull Request Process](#pull-request-process)
8. [Updating Project Status](#updating-project-status)
9. [Handling Blockers](#handling-blockers)
10. [Code Quality Standards](#code-quality-standards)

---

## Roadmap Structure

### Issue Hierarchy

```
Capability (Parent Issue)
├── Feature 1 (Child Issue)
├── Feature 2 (Child Issue)
└── Feature 3 (Child Issue)
```

**Capabilities** are high-level containers with:
- Multiple features linked via task lists
- Success criteria for the overall capability
- Effort estimates (total weeks)
- Dependencies on other capabilities

**Features** are deliverable work items with:
- Detailed implementation tasks (checkboxes)
- Acceptance criteria (must be met for completion)
- Files to create/modify
- Effort estimates (S/M/L)
- Dependencies (blocked by other features)

### Priority Levels

- **P0 (Critical)**: Must be completed ASAP - active production needs
- **P1 (High)**: Important for framework functionality
- **P2 (Medium)**: Valuable enhancements, not blocking
- **P3 (Low)**: Future enhancements, nice-to-have

### Labels

- `capability` - Parent container issue
- `feature` - Deliverable feature
- `streaming`, `dag`, `providers`, `config` - Domain area
- `P0`, `P1`, `P2`, `P3` - Priority

---

## Development Workflow

### Standard Workflow

```
1. Pick Work (check dependencies)
   ↓
2. Create Branch (from main)
   ↓
3. Implement Feature (follow standards)
   ↓
4. Write Tests (unit + integration)
   ↓
5. Update Documentation
   ↓
6. Run Pre-commit Hooks
   ↓
7. Create Pull Request
   ↓
8. Address Review Feedback
   ↓
9. Merge to Main
   ↓
10. Update Project Status
```

### Branch Naming

```bash
# Format: <type>/<issue-number>-<short-description>
feature/18-streaming-query-manager
feature/22-dependency-graph-builder
fix/123-watermark-bug
docs/update-streaming-guide
```

### Commit Messages

```bash
# Format:
# <type>: <description>
#
# <body with details>
#
# Co-Authored-By: Claude Sonnet 4.5 <noreply@anthropic.com>

# Example:
git commit -m "feat: implement StreamingQueryManager

- Add query registration with builder functions
- Implement start/stop/restart lifecycle methods
- Add query status tracking with StreamingQueryInfo
- Emit signals for query lifecycle events

Implements #18

Co-Authored-By: Claude Sonnet 4.5 <noreply@anthropic.com>"
```

**Commit Types:**
- `feat:` - New feature
- `fix:` - Bug fix
- `refactor:` - Code refactoring
- `test:` - Add/update tests
- `docs:` - Documentation only
- `chore:` - Maintenance tasks

---

## Picking Work

### Step 1: Check Available Work

```bash
# List P0 features not blocked
gh issue list --label "feature,P0" --state open --repo sep/spark-kindling-framework

# Check specific capability's features
gh issue view 14 --repo sep/spark-kindling-framework  # View Capability #14
```

### Step 2: Verify Dependencies

**Before starting work on a feature:**

1. **Check "Blocked By" section** in the issue description
2. **Verify blocking issues are completed:**
   ```bash
   gh issue view <blocking-issue-number> --repo sep/spark-kindling-framework
   ```
3. **Only start if all dependencies are closed or in review**

**Example:**
```
Feature #20 (Auto-Recovery Manager) shows:
"Dependencies: #18 (needs StreamingQueryManager)"

Action: Check if #18 is complete before starting #20
```

### Step 3: Claim the Work

**Comment on the issue:**
```
Starting work on this feature.

**Approach:**
- [Brief description of implementation approach]

**Estimated completion:** [timeframe]
```

---

## Implementation Standards

### File Organization

```
packages/kindling/
├── streaming_query_manager.py    # New feature file
├── streaming_watchdog.py          # Another feature
├── pipe_streaming.py              # Existing file to modify
└── tests/
    └── unit/
        └── test_streaming_query_manager.py
```

### Code Structure

**Every new module should include:**

1. **Module docstring:**
   ```python
   """
   Streaming Query Manager

   Manages lifecycle of Spark streaming queries with health monitoring
   and automatic recovery capabilities.

   Key Components:
   - StreamingQueryManager: Main lifecycle management
   - StreamingQueryInfo: Query state and metrics
   - StreamingQueryState: Enum for query states

   Related:
   - Issue #18: https://github.com/sep/spark-kindling-framework/issues/18
   - Proposal: docs/proposals/signal_dag_streaming_proposal.md
   """
   ```

2. **Class docstrings with examples:**
   ```python
   class StreamingQueryManager(SignalEmitter):
       """
       Manages Spark streaming query lifecycle.

       Example:
           >>> manager = StreamingQueryManager()
           >>> manager.register_query(
           ...     "my_stream",
           ...     lambda: spark.readStream.format("delta").load(...)
           ... )
           >>> query_info = manager.start_query("my_stream")
       """
   ```

3. **Type hints everywhere:**
   ```python
   def start_query(
       self,
       query_id: str,
       config: Optional[StreamingQueryConfig] = None
   ) -> StreamingQueryInfo:
       """Start a registered streaming query."""
   ```

4. **Dependency injection:**
   ```python
   from kindling.injection import inject
   from kindling.signaling import SignalProvider

   @inject
   class StreamingQueryManager:
       def __init__(
           self,
           signal_provider: SignalProvider,
           logger_provider: SparkLoggerProvider
       ):
           self.signals = signal_provider
           self.logger = logger_provider.get_logger("streaming")
   ```

5. **Signal emissions:**
   ```python
   class StreamingQueryManager(SignalEmitter):
       EMITS = [
           "streaming.query_started",
           "streaming.query_stopped",
           "streaming.query_failed",
       ]

       def start_query(self, query_id: str) -> StreamingQueryInfo:
           self.emit("streaming.query_started", query_id=query_id, ...)
   ```

### Error Handling

**Always handle errors gracefully:**

```python
def stop_query(self, query_id: str, await_termination: bool = True) -> bool:
    """Stop a streaming query."""
    try:
        query_info = self._queries.get(query_id)
        if not query_info:
            self.logger.warning(f"Query {query_id} not found")
            return False

        query_info.query.stop()

        if await_termination:
            query_info.query.awaitTermination()

        self.emit("streaming.query_stopped", query_id=query_id)
        return True

    except Exception as e:
        self.logger.error(f"Failed to stop query {query_id}: {e}")
        self.emit("streaming.query_failed", query_id=query_id, error=str(e))
        return False
```

### Configuration

**Use existing config system:**

```python
from kindling.spark_config import ConfigService

@inject
class StreamingWatchdog:
    def __init__(self, config: ConfigService):
        # Read from settings.yaml
        self.poll_interval = config.get("STREAMING_WATCHDOG_POLL_INTERVAL", default=30)
        self.stall_threshold = config.get("STREAMING_STALL_THRESHOLD", default=300)
```

---

## Testing Requirements

### Test Coverage Requirements

**Every feature MUST have:**

1. **Unit tests** - Test individual components in isolation
2. **Integration tests** - Test components working together
3. **Minimum coverage:** 80% for new code

### Unit Test Structure

```python
# tests/unit/test_streaming_query_manager.py

import pytest
from unittest.mock import Mock, MagicMock, patch
from kindling.streaming_query_manager import (
    StreamingQueryManager,
    StreamingQueryInfo,
    StreamingQueryState
)

class TestStreamingQueryManager:
    """Test suite for StreamingQueryManager."""

    @pytest.fixture
    def mock_signal_provider(self):
        """Mock signal provider for testing."""
        provider = Mock()
        provider.create_signal = Mock()
        return provider

    @pytest.fixture
    def manager(self, mock_signal_provider):
        """Create manager instance for testing."""
        return StreamingQueryManager(
            signal_provider=mock_signal_provider,
            logger_provider=Mock()
        )

    def test_register_query(self, manager):
        """Test query registration."""
        builder = Mock()

        manager.register_query("test_query", builder)

        assert "test_query" in manager._registered_queries
        assert manager._registered_queries["test_query"] == builder

    def test_start_query_success(self, manager):
        """Test successful query start."""
        # Setup
        mock_query = Mock()
        builder = Mock(return_value=mock_query)
        manager.register_query("test_query", builder)

        # Execute
        result = manager.start_query("test_query")

        # Assert
        assert result.query_id == "test_query"
        assert result.status == StreamingQueryState.RUNNING
        builder.assert_called_once()

    def test_start_query_not_registered(self, manager):
        """Test starting unregistered query raises error."""
        with pytest.raises(ValueError, match="not registered"):
            manager.start_query("unknown_query")
```

### Integration Test Structure

```python
# tests/integration/test_streaming_lifecycle.py

import pytest
from pyspark.sql import SparkSession
from kindling.streaming_query_manager import StreamingQueryManager
from kindling.streaming_watchdog import StreamingWatchdog

@pytest.fixture(scope="module")
def spark():
    """Create Spark session for testing."""
    return SparkSession.builder \
        .master("local[2]") \
        .appName("streaming-test") \
        .getOrCreate()

class TestStreamingLifecycle:
    """Integration tests for streaming lifecycle."""

    def test_query_start_stop_lifecycle(self, spark):
        """Test complete query lifecycle."""
        # This would test with a real streaming query
        # using in-memory source/sink
        pass
```

### Running Tests

```bash
# Run all tests
pytest

# Run specific test file
pytest tests/unit/test_streaming_query_manager.py

# Run with coverage
pytest --cov=packages/kindling --cov-report=html

# Run only fast tests (skip integration)
pytest -m "not integration"
```

---

## Documentation Requirements

### Code Documentation

**Every public class/function needs:**

1. **Docstring** with description
2. **Args section** (Google style)
3. **Returns section**
4. **Raises section** (if applicable)
5. **Example** (for complex APIs)

```python
def start_query(
    self,
    query_id: str,
    config: Optional[StreamingQueryConfig] = None
) -> StreamingQueryInfo:
    """
    Start a registered streaming query.

    Args:
        query_id: Unique identifier for the query
        config: Optional configuration overrides

    Returns:
        StreamingQueryInfo with query state and metrics

    Raises:
        ValueError: If query_id not registered
        RuntimeError: If query fails to start

    Example:
        >>> manager.register_query("my_query", builder_fn)
        >>> info = manager.start_query("my_query")
        >>> assert info.status == StreamingQueryState.RUNNING
    """
```

### Feature Documentation

**After implementing a feature, update:**

1. **Module docstring** - Add overview of feature
2. **README.md** - Update if user-facing
3. **Proposal docs** - Mark sections as implemented

**Example update to README.md:**

```markdown
## Streaming Support

Kindling provides comprehensive streaming query lifecycle management:

- **Query Manager**: Register, start, stop, and restart queries
- **Health Monitoring**: Automatic detection of stale/failed queries
- **Auto-Recovery**: Exponential backoff retry with configurable limits
- **Event Processing**: Non-blocking listener for query events

See [Streaming Guide](docs/streaming.md) for details.
```

---

## Pull Request Process

### PR Title Format

```
<type>(<scope>): <description>

Examples:
feat(streaming): add StreamingQueryManager
feat(dag): implement dependency graph builder
fix(streaming): handle query timeout in watchdog
docs(streaming): add query manager examples
```

### PR Description Template

```markdown
## Summary
Brief description of what this PR does.

## Related Issues
Closes #18

## Implementation Details
- Key design decisions
- Notable code changes
- Any deviations from original plan

## Testing
- [ ] Unit tests added/updated
- [ ] Integration tests added/updated
- [ ] All tests passing locally
- [ ] Coverage maintained at 80%+

## Documentation
- [ ] Code docstrings added
- [ ] README updated (if needed)
- [ ] Proposal marked as implemented

## Checklist
- [ ] Pre-commit hooks passing
- [ ] No merge conflicts
- [ ] Branch up to date with main
- [ ] Self-review completed
- [ ] Breaking changes documented (if any)

## Screenshots/Output
(If applicable - test results, examples, etc.)
```

### PR Review Process

**Lightweight Self-Merge (Current Process):**

1. **Create PR** with template filled out
2. **Verify all checks pass:**
   - ✅ All tests pass
   - ✅ Coverage ≥80%
   - ✅ Pre-commit hooks pass
   - ✅ No merge conflicts
3. **Self-merge** using squash and merge
4. **Delete feature branch** after merge

**Future Enhancement:**
- PRs serve as checkpoint for potential automated review (e.g., CodeRabbit)
- Manual review can be requested if needed for complex changes

### Merge Strategy

**Use "Squash and Merge"** for clean history:
- All commits squashed into one
- Single commit message in main branch
- Co-author attribution preserved

---

## Updating Project Status

### After Starting Work

1. **Comment on issue**: "Starting work on this feature"
2. **Move to "In Progress"** in project board (if not auto-moved)

### After Creating PR

1. **Comment on issue**: "PR created: #[PR_NUMBER]"
2. **Link PR to issue**: Use "Closes #18" in PR description

### After Merging PR

1. **Issue auto-closes** when PR merges (if "Closes #" used)
2. **Update parent Capability issue**:
   - Check off the feature in task list
   - Example: `- [x] #18 - Streaming Query Manager`

3. **Project auto-moves to "Done"** (if automation enabled)

### Capability Completion

When all features in a Capability are complete:

1. **Verify all task list items checked**
2. **Close the Capability issue** with summary comment:
   ```
   All features complete:
   - ✅ #18 - Streaming Query Manager
   - ✅ #19 - Streaming Health Watchdog
   - ✅ #20 - Auto-Recovery Manager
   - ✅ #21 - Queue-Based Event Processing

   Total effort: 4-5 weeks (as estimated)
   ```

---

## Handling Blockers

### If You Encounter a Blocker

1. **Document the blocker** in issue comment:
   ```
   **Blocker identified:**

   Cannot implement auto-recovery without StreamingQueryManager (#18)
   being complete. Manager provides query state tracking needed for
   recovery decisions.

   **Status:** Pausing work until #18 is merged.
   ```

2. **Update project status** - Move back to "Blocked" or "Backlog"

3. **Pick different work** - Find unblocked feature to work on

### If Requirements Unclear

1. **Comment on issue** with specific questions:
   ```
   **Clarification needed:**

   The acceptance criteria mentions "exponential backoff" but doesn't
   specify:
   - Initial backoff duration (1s? 5s?)
   - Max backoff duration (5min? 10min?)
   - Backoff multiplier (2x? 1.5x?)

   **Proposal:** Use 1s initial, 300s max, 2x multiplier (matches proposal doc)

   Please confirm or provide guidance.
   ```

2. **Wait for response** or make reasonable assumption (document it)

### If You Find a Bug

1. **Create bug issue** with:
   - Clear description of bug
   - Steps to reproduce
   - Expected vs actual behavior
   - Error messages/stack traces

2. **Link to related feature** if applicable

3. **Fix in same PR** if small, or create separate PR for complex bugs

---

## Code Quality Standards

### Pre-commit Hooks

**Always run before committing:**

```bash
# Automatically run by git commit
# Or manually run:
pre-commit run --all-files
```

**Hooks enforce:**
- Black formatting
- isort import ordering
- flake8 linting
- Trailing whitespace removal
- YAML syntax validation

### Code Review Checklist

Before creating PR, self-review:

- [ ] **Functionality**: Does it meet all acceptance criteria?
- [ ] **Tests**: 80%+ coverage, all passing?
- [ ] **Documentation**: Docstrings, README updates?
- [ ] **Error Handling**: Graceful failures with logging?
- [ ] **Type Hints**: All functions typed?
- [ ] **DI Pattern**: Uses `@inject` for dependencies?
- [ ] **Signals**: Emits appropriate signals?
- [ ] **Config**: Uses ConfigService for settings?
- [ ] **Logging**: Uses injected logger, not print()?
- [ ] **No Hardcoding**: No magic numbers, use config/constants
- [ ] **Single Responsibility**: Each class/function has one job
- [ ] **DRY**: No duplicated code
- [ ] **Performance**: No obvious performance issues

### Anti-Patterns to Avoid

❌ **Don't:**
- Use `print()` - use injected logger
- Hardcode values - use config
- Skip tests - 80% coverage required
- Skip docstrings - all public APIs need docs
- Use `Any` type hints - be specific
- Catch bare `except:` - catch specific exceptions
- Ignore errors silently - log and emit signals
- Create circular imports - use dependency injection
- Mix concerns - separate business logic from infrastructure

✅ **Do:**
- Use dependency injection
- Emit signals for all lifecycle events
- Log important operations
- Handle errors gracefully
- Write descriptive commit messages
- Keep functions small and focused
- Follow existing code patterns
- Update documentation

---

## Quick Reference

### Essential Commands

```bash
# Pick work
gh issue list --label "feature,P0" --state open --repo sep/spark-kindling-framework

# Create branch
git checkout -b feature/18-streaming-query-manager

# Run tests
pytest --cov=packages/kindling

# Pre-commit
pre-commit run --all-files

# Create PR
gh pr create --title "feat(streaming): add StreamingQueryManager" --body "Closes #18"

# Check issue status
gh issue view 18 --repo sep/spark-kindling-framework
```

### File Locations

```
packages/kindling/          # Framework code
tests/unit/                 # Unit tests
tests/integration/          # Integration tests
docs/proposals/             # Design proposals
examples/                   # Example usage
```

### Getting Help

- **Technical questions**: Comment on related issue
- **Blocker**: Document in issue, pick different work
- **Bug found**: Create bug issue with reproduction steps
- **Unclear requirements**: Comment on issue asking for clarification

---

## Summary

**Remember:**
1. ✅ Check dependencies before starting
2. ✅ Follow coding standards (DI, signals, logging, types)
3. ✅ Write tests (80%+ coverage)
4. ✅ Document your code (docstrings, README)
5. ✅ Create descriptive PRs
6. ✅ Update project status
7. ✅ Handle errors gracefully
8. ✅ Ask questions when blocked

**Goal:** Deliver high-quality, well-tested, documented features that integrate seamlessly with the existing framework.

---

*Last updated: 2026-02-03*
*For questions or improvements to this guide, open an issue.*
