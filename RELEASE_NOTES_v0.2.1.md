# Release Notes - v0.2.1

**Release Date:** January 2025
**Type:** Minor version release (bug fix)

---

## Summary

Version 0.2.1 introduces **proper semantic versioning support for pre-release versions** across the framework. This critical fix ensures that version comparison and selection correctly handle alpha, beta, release candidate, and other PEP 440 pre-release identifiers.

---

## What's Fixed

### Version Comparison - Semantic Versioning Support

**Issue**: Version comparison used string/integer comparison instead of semantic versioning, causing incorrect sort order for pre-release versions.

**Impact**:
- Pre-release versions (e.g., `1.0.0-alpha`, `1.0.0-beta`) sorted incorrectly
- Lexicographic comparison treated `"1.0.0-alpha" > "1.0.0"` (wrong)
- Bootstrap script crashed on pre-release versions with parsing errors

**Fix**: Implemented `packaging.version.Version` for proper PEP 440 semantic versioning

**Benefits**:
- ✅ Correct pre-release ordering: `1.0.0a1 < 1.0.0a2 < 1.0.0b1 < 1.0.0rc1 < 1.0.0`
- ✅ Supports all PEP 440 versioning: alpha, beta, rc, dev, post releases
- ✅ Automatic normalization: `1.0.0-alpha-1` → `1.0.0a1`
- ✅ Consistent version handling across all framework components

---

## Files Changed

### Core Framework

1. **packages/kindling/data_apps.py**
   - Updated `WheelCandidate.sort_key` property
   - Returns `Tuple[int, Version]` instead of `Tuple[int, str]`
   - Added proper error handling for malformed versions

2. **runtime/scripts/kindling_bootstrap.py**
   - Updated `extract_version()` function
   - Uses `packaging.version.Version` for semantic comparison
   - Handles pre-release versions correctly during framework installation

### Tests

3. **tests/unit/test_app_framework.py**
   - Updated `test_sort_key` to expect `Version` objects
   - Added pre-release version test cases
   - Added malformed version handling tests
   - All 10 tests passing

---

## Technical Details

### Version Comparison Before

```python
# data_apps.py - OLD
def sort_key(self) -> Tuple[int, str]:
    return (self.priority, self.version or "0")

# Problem: "1.0.0-alpha" > "1.0.0" lexicographically (WRONG!)
```

### Version Comparison After

```python
# data_apps.py - NEW
from packaging.version import Version, InvalidVersion

def sort_key(self) -> Tuple[int, Version]:
    """Uses packaging.version.Version for proper semantic versioning"""
    try:
        version_obj = Version(self.version) if self.version else Version("0.0.0")
    except InvalidVersion:
        version_obj = Version("0.0.0")  # Fallback for malformed versions
    return (self.priority, version_obj)

# Correct: Version("1.0.0a1") < Version("1.0.0")
```

### Bootstrap Changes

```python
# kindling_bootstrap.py - NEW
def extract_version(filename):
    """Extract version using packaging.version for proper semantic versioning"""
    try:
        version_part = filename.split("-")[1]
        if Version is not None:
            return Version(version_part)  # Semantic versioning
        else:
            return tuple(map(int, version_part.split(".")))  # Fallback
    except (IndexError, ValueError, InvalidVersion):
        return Version("0.0.0") if Version is not None else (0, 0, 0)
```

---

## Validation

### Test Results

- ✅ **Unit Tests**: 10/10 passing (`test_app_framework.py`)
- ✅ **Alpha Version**: Successfully built and deployed `kindling_fabric-0.2.1a1-py3-none-any.whl`
- ✅ **System Tests**: 5/5 Fabric system tests passing with alpha version
- ✅ **Real-world Usage**: Framework correctly selects latest version (0.2.1a1 > 0.1.6.5)

### Pre-release Version Support

Now correctly handles:
- `1.0.0a1` - Alpha 1
- `1.0.0a2` - Alpha 2
- `1.0.0b1` - Beta 1
- `1.0.0rc1` - Release candidate 1
- `1.0.0.dev1` - Development release
- `1.0.0.post1` - Post release

All variations normalize correctly:
- `1.0.0-alpha-1` → `Version("1.0.0a1")`
- `1.0.0_alpha_1` → `Version("1.0.0a1")`
- `1.0.0alpha1` → `Version("1.0.0a1")`

---

## Upgrade Guide

### From 0.2.0 to 0.2.1

**No breaking changes** - this is a bug fix release.

1. **Update framework wheels** (recommended):
   ```bash
   # Build new wheels
   poe build

   # Deploy to your platform
   poe deploy-fabric      # For Fabric
   poe deploy-databricks  # For Databricks
   poe deploy-synapse     # For Synapse
   ```

2. **Version selection automatically improved** - No code changes needed in your data apps

3. **Test with pre-release versions** (optional):
   ```bash
   # Version in pyproject.toml
   version = "1.0.0a1"  # Alpha 1
   version = "1.0.0b1"  # Beta 1
   version = "1.0.0rc1" # Release candidate 1

   # Build and test
   poe build
   ```

### Compatibility

- ✅ **Backward compatible** - Existing version strings work unchanged
- ✅ **Forward compatible** - Pre-release versions now work correctly
- ✅ **Platform support** - All platforms (Fabric, Synapse, Databricks)
- ✅ **Python 3.10+** - No new dependencies (`packaging` already required)

---

## Known Issues

None

---

## Contributors

- SEP Engineering Team

---

## Documentation

- **Build System**: [docs/build_system.md](docs/build_system.md)
- **Release Process**: [docs/release_process.md](docs/release_process.md)
- **CI/CD Setup**: [docs/ci_cd_setup.md](docs/ci_cd_setup.md)

---

## Next Release (0.3.0)

Future enhancements planned:
- Configuration hot-reload (already implemented, pending release)
- Signal-based observability patterns
- Additional platform optimizations

---

**Full Changelog**: https://github.com/SEP-Engineering/kindling/compare/v0.2.0...v0.2.1
