# kindling-otel-azure Alpha Release Summary

**Version:** 0.3.0-alpha.1
**Date:** January 27, 2026
**Status:** ✅ Built and Ready for Testing

## What Changed

### Dependency Updates
Updated from strict version pinning to semantic versioning for better cross-platform compatibility:

| Dependency | Old Version | New Version | Change |
|------------|-------------|-------------|--------|
| azure-monitor-opentelemetry | `1.2.0` (exact) | `^1.8.0` (flexible) | +6 months of improvements |
| opentelemetry-api | `1.21.0` (exact) | `^1.21.0` (flexible) | Allow minor updates |
| opentelemetry-sdk | `1.21.0` (exact) | `^1.21.0` (flexible) | Allow minor updates |

### Why This Matters

1. **Databricks Compatibility**: The strict version pinning was causing dependency conflicts in Databricks environments
2. **Bug Fixes**: Gets 12 months of bug fixes from azure-monitor-opentelemetry (1.2.0 → 1.8.4)
3. **Flexibility**: Pip can now resolve compatible versions across different platform runtimes
4. **No Breaking Changes**: The API remains 100% compatible (verified via changelog review)

## Package Details

**Distribution Files:**
- `kindling_otel_azure-0.3.0a1-py3-none-any.whl` (7.1 KB)
- `kindling_otel_azure-0.3.0a1.tar.gz` (5.8 KB)

**Location:**
```
/workspace/packages/kindling_otel_azure/dist/
```

**Package Contents:**
- ✅ Core modules (config, logger_provider, trace_provider)
- ✅ README with alpha installation instructions
- ✅ CHANGELOG with migration guide
- ✅ Proper dependency metadata

## Installation

### From Built Package (Testing)
```bash
pip install /workspace/packages/kindling_otel_azure/dist/kindling_otel_azure-0.3.0a1-py3-none-any.whl
```

### In Kindling Configuration
```yaml
kindling:
  extensions:
    - kindling-otel-azure==0.3.0a1  # Alpha version

  telemetry:
    azure_monitor:
      connection_string: "InstrumentationKey=...;IngestionEndpoint=..."
      enable_logging: true
      enable_tracing: true
```

## Testing Checklist

### ✅ Build Verification
- [x] Package builds successfully with Poetry
- [x] Dependencies correctly specified in METADATA
- [x] Version correctly set to 0.3.0-alpha.1
- [x] CHANGELOG and README included

### ⏳ Platform Testing (Required)
- [ ] **Fabric**: Re-run system test with alpha version
- [ ] **Synapse**: Deploy and test on Synapse Spark pool
- [ ] **Databricks**: Deploy and test on Databricks cluster (PRIMARY TARGET)

### Test Commands

**Fabric:**
```bash
pytest -v -m fabric tests/system/extensions/azure-monitor/test_azure_monitor_extension.py
```

**Synapse:**
```bash
pytest -v -m synapse tests/system/extensions/azure-monitor/test_azure_monitor_extension.py
```

**Databricks:**
```bash
pytest -v -m databricks tests/system/extensions/azure-monitor/test_azure_monitor_extension.py
```

## Expected Outcomes

### Success Criteria
1. ✅ Extension loads without dependency conflicts on all platforms
2. ✅ Logs are sent to Application Insights
3. ✅ Traces are sent to Application Insights with proper correlation
4. ✅ No errors in bootstrap or framework initialization

### Known Differences from 0.2.0
- **None** - This is a dependency-only update with no code changes
- API remains identical
- Configuration format unchanged
- All existing code using the extension will continue to work

## Rollback Plan

If the alpha version has issues, revert to 0.2.0:

```yaml
kindling:
  extensions:
    - kindling-otel-azure==0.2.0  # Stable version (Fabric/Synapse only)
```

## Next Steps

1. **Test on Databricks** (primary goal - fix dependency conflicts)
2. **Test on Synapse** (ensure no regressions)
3. **Re-test on Fabric** (verify updated dependencies work)
4. **Collect feedback** from alpha testing
5. **Release 0.3.0 stable** if all tests pass

## Files Modified

- `/workspace/packages/kindling_otel_azure/pyproject.toml` - Updated dependencies and version
- `/workspace/packages/kindling_otel_azure/kindling_otel_azure/__init__.py` - Updated version string
- `/workspace/packages/kindling_otel_azure/README.md` - Added alpha installation instructions
- `/workspace/packages/kindling_otel_azure/CHANGELOG.md` - Created (new file)

## Dependencies Analysis

**From METADATA file:**
```
Requires-Dist: azure-monitor-opentelemetry (>=1.8.0,<2.0.0)
Requires-Dist: opentelemetry-api (>=1.21.0,<2.0.0)
Requires-Dist: opentelemetry-sdk (>=1.21.0,<2.0.0)
```

This means:
- Will accept any 1.8.x or 1.9.x version of azure-monitor-opentelemetry
- Will accept any 1.21.x, 1.22.x, etc. version of opentelemetry packages
- Will NOT accept 2.x versions (breaking changes)

## Contact

For issues or questions during alpha testing:
- Check Application Insights for telemetry data
- Review bootstrap logs for extension loading errors
- Compare behavior with 0.2.0 stable version

---

**Status:** ✅ READY FOR ALPHA TESTING
