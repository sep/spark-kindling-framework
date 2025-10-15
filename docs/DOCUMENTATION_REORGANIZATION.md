# Documentation Naming Guidelines

## Purpose

This document defines the naming conventions and organizational structure for all documentation in the Spark Kindling Framework repository.

## Naming Conventions

### Root Directory Files
**Convention**: UPPERCASE with standard GitHub naming

- `README.md` - Project overview and getting started
- `CONTRIBUTING.md` - Contribution guidelines
- `LICENSE` - License information
- `DISCLAIMER.md` - Legal disclaimers

**Rationale**: Follows GitHub and open-source community standards for discoverability and convention.

### Subdirectory Documentation
**Convention**: lowercase_with_underscores

Examples:
- `docs/testing.md`
- `docs/platform_comparison.md`
- `tests/integration/run_tests.md`
- `tests/test_infrastructure_proposal.md`

**Exceptions**:
- `README.md` files remain uppercase even in subdirectories (standard convention)

**Rationale**: 
- Consistent with common documentation practices
- Easy to read and type
- Clear hierarchical distinction from root-level files

## Documentation Organization

### `/docs/` - Framework Documentation

Technical documentation for framework users and developers:

```
docs/
├── intro.md                      # Getting started with the framework
├── setup_guide.md                # Installation and configuration
├── data_entities.md              # Entity system documentation
├── data_pipes.md                 # Pipeline documentation
├── entity_providers.md           # Entity provider implementations
├── file_ingestion.md             # File ingestion patterns
├── logging_tracing.md            # Observability features
├── platform_comparison.md        # Platform differences and features
├── stage_processing.md           # Stage processor documentation
├── testing.md                    # Testing guide (comprehensive)
├── utilities.md                  # Utility functions
└── watermarking.md               # Incremental processing
```

### `/tests/` - Test Documentation

Documentation specific to testing infrastructure:

```
tests/
├── test_infrastructure_proposal.md    # Proposed test utilities and patterns
└── integration/
    ├── README.md                       # Integration test philosophy
    ├── run_tests.md                    # How to run integration tests
    └── spark_java_compatibility.md     # Java/Spark version compatibility
```

## Best Practices

### When Creating New Documentation

1. **Choose the right location**:
   - Framework documentation → `/docs/`
   - Test-specific documentation → `/tests/` or `/tests/integration/`
   - Root-level docs only for GitHub conventions (README, CONTRIBUTING, etc.)

2. **Use descriptive names**:
   - ✅ `testing.md` - Clear and comprehensive
   - ❌ `test_guide.md` + `testing_quickstart.md` - Redundant and fragmented

3. **Follow naming conventions**:
   - ✅ `platform_comparison.md` - Lowercase with underscores
   - ❌ `PLATFORM_COMPARISON.md` - Uppercase in subdirectory
   - ✅ `README.md` - Uppercase (standard convention)

4. **Avoid duplication**:
   - Consolidate overlapping content into comprehensive guides
   - Use cross-references instead of duplicating information
   - Example: Single `testing.md` instead of separate quickstart and strategy docs

### Consolidation Guidelines

When you find overlapping documentation:

1. **Identify the primary audience and purpose**
2. **Merge content into a single comprehensive document**
3. **Remove or archive redundant files**
4. **Update cross-references in other documents**

Example:
```
Before:
- docs/TESTING_QUICKSTART.md (platform_local specific)
- docs/testing_strategy.md (general strategy)

After:
- docs/testing.md (comprehensive guide covering both)
```

## File Naming Checklist

When adding new documentation, ask:

- [ ] Is this a root-level GitHub convention file? → Use UPPERCASE
- [ ] Is this in a subdirectory? → Use lowercase_with_underscores
- [ ] Does similar documentation already exist? → Consider consolidating
- [ ] Is the name descriptive and clear?
- [ ] Does it follow the established pattern in its directory?

## Rationale

### Why These Conventions?

1. **GitHub Standards**: Root-level uppercase files are instantly recognizable and expected
2. **Readability**: Lowercase subdirectory files are easier to read and type
3. **Consistency**: Predictable naming makes documentation easier to find
4. **Maintainability**: Clear guidelines prevent documentation sprawl
5. **Professionalism**: Follows common open-source practices

## Tools and Automation

### Validate Naming

```bash
# Check for uppercase files in subdirectories (excluding README.md)
find docs tests -type f -name "*.md" ! -name "README.md" -exec sh -c '
  for file; do
    basename="$(basename "$file")"
    if echo "$basename" | grep -q "[A-Z]"; then
      echo "Warning: $file contains uppercase letters"
    fi
  done
' sh {} +
```

### Bulk Rename

```bash
# Example: Rename uppercase docs to lowercase
cd docs
for file in *.md; do
  lowercase=$(echo "$file" | tr '[:upper:]' '[:lower:]' | tr ' ' '_')
  if [ "$file" != "$lowercase" ] && [ "$file" != "README.md" ]; then
    git mv "$file" "$lowercase"
  fi
done
```

## Summary

- **Root files**: UPPERCASE (README.md, CONTRIBUTING.md, LICENSE, DISCLAIMER.md)
- **Subdirectory docs**: lowercase_with_underscores
- **README files**: Always UPPERCASE, even in subdirectories
- **Consolidate**: Merge overlapping content into comprehensive guides
- **Be descriptive**: Use clear, meaningful names
