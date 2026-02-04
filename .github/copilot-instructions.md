---
description: Kindling framework code review guidelines
name: kindling-review
applies_to: pull_requests
---

# Code Review Guidelines for Kindling Framework

## Overview

Kindling is a multi-platform Spark data lakehouse framework supporting Azure Synapse Analytics, Databricks, and Microsoft Fabric. The framework uses dependency injection, platform abstraction patterns, and notebook-based orchestration.

## Critical Focus Areas

### 1. Platform Compatibility
- **Platform-agnostic code**: All framework features must work on Fabric, Synapse, and Databricks
- **Platform services pattern**: Use dependency injection and `PlatformServiceProvider` instead of if/else platform conditionals
- **No platform-specific imports**: Platform-specific code belongs in `platform_*.py` modules only
- **Test on all platforms**: System tests must pass on all three platforms

### 2. Poe Task Usage (CRITICAL)
⚠️ **NEVER call scripts directly** - always use poe tasks:
- ✅ `poe build` not `poetry build`
- ✅ `poe deploy --platform fabric` not `python scripts/deploy.py`
- ✅ `poe test-system --platform fabric` not `pytest -m fabric`
- ✅ `poe version --bump_type alpha --platform fabric` for version bump + build + deploy workflow

### 3. Dependency Injection
- **Use `@inject` decorator**: All services requiring dependencies must use DI
- **`@GlobalInjector.singleton_autobind()`**: For framework services
- **Service provider pattern**: Platform-specific logic goes in service implementations
- **No manual instantiation**: Services should be obtained via `get_kindling_service()`

### 4. Error Handling & Logging
- **SparkLogger supports `exc_info`**: Use `logger.error(msg, exc_info=True)` for stack traces
- **Structured logging**: Include context in log messages
- **Exception context**: Use `raise ... from e` to preserve exception chains
- **Fail fast**: Don't swallow exceptions in framework code

### 5. Version Management
- **Alpha versions for dev**: Use `poe version --bump_type alpha` to bust pip caching during development
- **Semantic versioning**: major.minor.patch for releases, add `aN` suffix for alpha
- **One-command workflow**: `poe version --bump_type alpha --platform fabric` handles version bump, build, and deployment

### 6. System Testing
- **Platform deployment pattern**: System tests deploy actual Spark jobs to cloud platforms
- **Test markers validation**: Tests must log structured markers for validation
- **Stdout monitoring**: Tests validate execution through stdout log streaming
- **Resource cleanup**: Tests must clean up jobs and data-apps after execution

### 7. Module Organization
- **Utilities stay in context**: Extract utilities within the module that uses them
- **Service vs utility**: Services use DI, utilities are pure functions with explicit parameters
- **Keep interfaces minimal**: Avoid over-abstraction

## Code Patterns to Watch For

### ❌ Anti-Patterns

```python
# DON'T: Platform conditionals
if platform == "fabric":
    do_fabric_thing()
elif platform == "synapse":
    do_synapse_thing()

# DON'T: Direct script execution
subprocess.run(["python", "scripts/deploy.py"])

# DON'T: Manual service instantiation
platform_service = FabricService(config, logger)

# DON'T: Logger without exc_info
try:
    ...
except Exception as e:
    logger.error(f"Error: {e}")  # Missing stack trace!
```

### ✅ Correct Patterns

```python
# DO: Platform service pattern
platform_service = get_kindling_service(PlatformServiceProvider)
platform_service.deploy_spark_job(app_files, config)

# DO: Use poe tasks
subprocess.run(["poe", "deploy", "--platform", "fabric"])

# DO: Dependency injection
@inject
class MyService:
    def __init__(self, platform: PlatformServiceProvider, logger: SparkLoggerProvider):
        self.platform = platform
        self.logger = logger.get_logger(__name__)

# DO: Logger with exc_info
try:
    ...
except Exception as e:
    logger.error(f"Error: {e}", exc_info=True)  # Includes stack trace
```

## Testing Requirements

### System Tests
- Must use `poe test-system` command
- Must test on actual platforms (not just local Spark)
- Must validate via stdout markers (format: `TEST_ID={id} test={name} status={PASSED|FAILED}`)
- Must include cleanup in `finally` blocks

### Unit/Integration Tests
- Run with `poe test-unit` or `poe test-integration`
- Mock external dependencies appropriately
- Test both success and failure paths

## Documentation Standards

- **Docstrings**: Required for public APIs and complex functions
- **Type hints**: Use where they add clarity
- **Comments**: Explain "why", not "what"
- **Markdown**: All docs in markdown format
- **Examples**: Include usage examples for non-trivial features

## Architecture Principles

1. **Platform abstraction**: Framework code must never import platform-specific modules
2. **Dependency injection**: Services compose through DI, not direct instantiation
3. **Single responsibility**: Each module/class has one clear purpose
4. **Extensibility**: Easy to add new platforms by implementing interfaces
5. **Testability**: All components can be tested in isolation with mocks

## File Organization

```
packages/kindling/
├── platform_provider.py          # Platform service interfaces
├── platform_fabric.py            # Fabric implementation
├── platform_synapse.py           # Synapse implementation
├── platform_databricks.py        # Databricks implementation
├── streaming_*.py                # Streaming components
├── injection.py                  # DI infrastructure
└── ...

tests/
├── unit/                         # Fast, isolated tests
├── integration/                  # Multi-component tests
└── system/                       # Platform deployment tests
    └── data-apps/                # Test applications
```

## Common Review Checklist

- [ ] No direct platform conditionals (use service pattern)
- [ ] All script calls use poe tasks
- [ ] Services use dependency injection
- [ ] Error logging includes `exc_info=True` where appropriate
- [ ] System tests validate via stdout markers
- [ ] Resources cleaned up in finally blocks
- [ ] Version bumps use alpha for development iterations
- [ ] Code works on all three platforms
- [ ] Tests pass on CI/CD
- [ ] Documentation updated if API changes

## Questions to Ask in Review

1. Does this code work identically on Fabric, Synapse, and Databricks?
2. Are platform-specific operations delegated to platform services?
3. Are poe tasks used instead of direct script execution?
4. Will pip caching issues require a version bump?
5. Do system tests properly clean up resources?
6. Are exceptions logged with full stack traces?
7. Is the dependency injection pattern used correctly?

---

For more details, see [/workspace/.github/instructions/project.instructions.md](../instructions/project.instructions.md)
