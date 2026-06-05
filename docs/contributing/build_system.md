# Platform Wheel Build System

This document describes the Poetry-based build system that replaced the custom 400+ line build script.

## Overview

**Current Version:** 0.6.0
**Build Tool:** Poe the Poet (poethepoet) task runner with Poetry
**Platforms:** Microsoft Fabric, Azure Synapse Analytics, Databricks

## Build Commands

```bash
# Build all platform wheels
poe build

# Deploy to Azure storage (platform-specific during testing)
poe deploy --platform fabric      # Deploy ONLY Fabric wheel
poe deploy --platform databricks  # Deploy ONLY Databricks wheel
poe deploy --platform synapse     # Deploy ONLY Synapse wheel
poe deploy             # Deploy ALL wheels (avoid during testing)
```

**Important:** During testing, deploy only the platform wheel you're testing to avoid pip caching issues.

## ✅ **Architecture: Single Wheel Per Platform**

### **Before: Custom Build Script** ❌
- **File**: `scripts/build_platform_wheels.py` (408 lines)
- **Problems**:
  - Hardcoded dependency management
  - Manual pyproject.toml generation
  - Complex file copying logic
  - Non-standard build approach
  - Maintenance nightmare

### **After: Poetry + Simple Shell Script** ✅
- **File**: `scripts/build_platform_wheels.sh` (89 lines)
- **Benefits**:
  - Standard Poetry build system
  - Platform-specific pyproject.toml configs
  - Clean wheel post-processing
  - Maintainable and readable
  - Industry-standard approach

## **Architecture Achieved: Single Wheel Per Platform**

### **Wheel Output**
```bash
# Each wheel contains: Core + Platform-specific implementation + Dependencies
kindling_fabric-0.6.0-py3-none-any.whl      # Core + Fabric + Fabric SDKs
kindling_synapse-0.6.0-py3-none-any.whl     # Core + Synapse + Azure SDKs
kindling_databricks-0.6.0-py3-none-any.whl  # Core + Databricks + Databricks SDK
```

### **Platform Tag Compatibility**
- ✅ Platform-specific wheel names (kindling_{platform}-version)
- ✅ Auto-detection of platform in bootstrap
- ✅ Deployment workflow via `poe deploy --platform {platform}`

### **File Exclusion Strategy**
Each wheel contains **only its platform implementation**:
- **Synapse wheel**: `platform_synapse.py` only
- **Databricks wheel**: `platform_databricks.py` only
- **Fabric wheel**: `platform_fabric.py` only
- **All wheels**: Core framework (`data_apps.py`, `bootstrap.py`, etc.)

## **Cleanup Completed**
- ❌ **Removed**: `platform_*_simple.py` files (unnecessary complexity)
- ❌ **Removed**: `platform_provider_simple.py` (unused architecture)
- ❌ **Removed**: `platform_provider_unified.py` (unused architecture)
- ✅ **Kept**: Full-featured platform implementations only

## **Build Process Comparison**

### **Old Custom Script**
```bash
python scripts/build_platform_wheels.py --all
# 400+ lines of custom logic
# Manual dependency management
# Hardcoded pyproject.toml generation
```

### **New Poetry + Poe System**
```bash
poe build  # Build all platform wheels
# Standard Poetry build system
# Platform-specific configs in pyproject.toml
# Poe the Poet task automation
```

## **Key Configuration**

### **Poetry Configuration**
- `pyproject.toml` - Main project configuration with platform-specific extras
- `tool.poetry.extras` - Platform-specific dependencies
- `tool.poe.tasks` - Build and deployment automation

### **Poe Tasks**
See `pyproject.toml` `[tool.poe.tasks]` section for all available tasks:
- `build` - Build platform-specific wheels
- `deploy --platform {platform}` - Deploy selected platform to Azure storage
- `test` / `test-unit` / `test-integration` - Run tests
- `cleanup` - Clean test artifacts

## **Deployment Simplicity Achieved**

### **Single Wheel Installation**
```bash
# One command per platform
pip install kindling_fabric-0.6.0-py3-none-any.whl
pip install kindling_synapse-0.6.0-py3-none-any.whl
pip install kindling_databricks-0.6.0-py3-none-any.whl
```

### **Usage in Applications**
```python
# Platform auto-detection works seamlessly
from kindling import DataAppManager, initialize_framework

# Initialize framework (auto-detects platform)
initialize_framework(BOOTSTRAP_CONFIG)

# Use framework services
manager = DataAppManager()
manager.run_app("my_app")
```

## **Future Extensibility**

### **Platform Extensions** (Optional)
```bash
# Base platform wheels
pip install kindling_fabric-0.6.0-py3-none-any.whl
pip install kindling_synapse-0.6.0-py3-none-any.whl
pip install kindling_databricks-0.6.0-py3-none-any.whl

# Optional: Add observability features
pip install kindling-otel-azure-0.3.0-py3-none-any.whl

# Optional: Add Databricks Delta Live Tables
pip install kindling-databricks-dlt-0.1.0-py3-none-any.whl
```

## **Bottom Line: Success! 🎉**

✅ **Eliminated** 400+ line custom build script
✅ **Achieved** single wheel per platform deployment
✅ **Maintained** full compatibility with existing code
✅ **Used** standard Python packaging tools (Poetry + Poe)
✅ **Provided** clean extensibility path for platform-specific features
✅ **Reduced** wheel size by excluding unused platform files
✅ **Version** 0.6.0 with hierarchical config, data apps, job deployment

## **Build Process**

```bash
# Build all platform wheels
poe build

# Deploy to Azure storage
poe deploy --platform fabric      # For Fabric testing
poe deploy --platform databricks  # For Databricks testing
poe deploy --platform synapse     # For Synapse testing
```

Produces platform-specific wheels compatible with auto-detection and bootstrap system.
