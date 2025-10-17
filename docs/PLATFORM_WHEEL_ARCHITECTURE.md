# Platform-Specific Wheel Architecture Plan

## 🎯 Current Problem Analysis

### Issues with Current Monolithic Approach:
- **Conditional imports**: `if add_to_registry:` blocks create testing complexity
- **Bundled dependencies**: All platform libraries bundled even when only one is used
- **Import failures**: Missing platform libraries cause import errors
- **Testing complexity**: Need to mock platform availability for testing
- **Package bloat**: Single wheel contains code for all platforms

### Benefits of Platform-Specific Wheels:
- **Clean imports**: No conditional platform detection needed
- **Minimal dependencies**: Only platform-specific libraries included
- **Simplified testing**: Each wheel can be tested independently
- **Standard injection**: Single platform service per wheel = no registry needed
- **User choice**: Install only the platform you need

## 🏗️ Proposed Architecture

### Four Separate Wheels:
```
kindling-synapse==1.0.0    # Azure Synapse Analytics
kindling-fabric==1.0.0     # Microsoft Fabric  
kindling-databricks==1.0.0 # Databricks
kindling-local==1.0.0      # Local development (optional)
```

### Package Structure per Platform:
```
kindling-synapse/
├── kindling/
│   ├── __init__.py
│   ├── data_apps.py           # Core framework (same)
│   ├── data_entities.py       # Core framework (same)
│   ├── data_pipes.py          # Core framework (same)
│   ├── notebook_framework.py  # Core framework (same)
│   ├── platform_synapse.py    # Platform-specific
│   ├── platform_provider.py   # Modified for single platform
│   └── [other core modules]   # Core framework (same)
└── setup.py                   # Platform-specific dependencies
```

## 🔧 Implementation Plan

### Phase 1: Build System Modification

#### Create Platform-Specific Build Script:
```python
# scripts/build_platform_wheels.py
import os
import shutil
import subprocess
from pathlib import Path

PLATFORMS = {
    'synapse': {
        'keep_modules': ['platform_synapse.py'],
        'extra_deps': [
            'azure-core>=1.24.0',
            'azure-synapse-artifacts>=0.17.0', 
            'azure-identity>=1.12.0'
        ]
    },
    'fabric': {
        'keep_modules': ['platform_fabric.py'],
        'extra_deps': [
            'sempy>=0.5.0',
            'azure-identity>=1.12.0'
        ]
    },
    'databricks': {
        'keep_modules': ['platform_databricks.py'],
        'extra_deps': [
            'databricks-sdk>=0.8.0',
            'databricks-cli>=0.17.0'
        ]
    },
    'local': {
        'keep_modules': ['platform_local.py'],
        'extra_deps': []
    }
}

def build_platform_wheel(platform_name, config):
    """Build wheel for specific platform"""
    
    print(f"Building kindling-{platform_name} wheel...")
    
    # Create temporary build directory
    build_dir = Path(f"build/kindling-{platform_name}")
    build_dir.mkdir(parents=True, exist_ok=True)
    
    # Copy core framework files
    src_dir = Path("src/kindling")
    dest_dir = build_dir / "src" / "kindling"
    dest_dir.mkdir(parents=True, exist_ok=True)
    
    # Copy all core files
    for file in src_dir.glob("*.py"):
        if not file.name.startswith("platform_") or file.name in config['keep_modules']:
            shutil.copy2(file, dest_dir)
    
    # Create platform-specific pyproject.toml
    create_platform_pyproject(platform_name, config, build_dir)
    
    # Build wheel
    subprocess.run([
        "python", "-m", "build", "--wheel", "--outdir", "dist/", str(build_dir)
    ], check=True)
    
    print(f"✅ Built kindling-{platform_name} wheel")

def create_platform_pyproject(platform_name, config, build_dir):
    """Create platform-specific pyproject.toml"""
    
    pyproject_content = f'''[build-system]
requires = ["setuptools>=42", "wheel"]
build-backend = "setuptools.build_meta"

[project]
name = "kindling-{platform_name}"
version = "1.0.0"
description = "Kindling Framework for {platform_name.title()}"
readme = "README.md"
authors = [
    {{name = "SEP Engineering", email = "engineering@sep.com"}}
]
license = {{text = "MIT"}}
dependencies = [
    "pyspark>=3.4.0",
    "delta-spark>=2.4.0", 
    "pandas>=2.0.0",
    "pyarrow>=12.0.0",
    {','.join(f'"{dep}"' for dep in config['extra_deps'])}
]
requires-python = ">=3.10"

[tool.setuptools]
package-dir = {{"" = "src"}}

[tool.setuptools.packages.find]
where = ["src"]
'''
    
    with open(build_dir / "pyproject.toml", "w") as f:
        f.write(pyproject_content)

def main():
    """Build all platform wheels"""
    for platform_name, config in PLATFORMS.items():
        build_platform_wheel(platform_name, config)

if __name__ == "__main__":
    main()
```

### Phase 2: Platform Provider Simplification

#### Modify `platform_provider.py` for Single Platform:
```python
# src/kindling/platform_provider.py (modified)
from abc import ABC, abstractmethod
from typing import Optional

class PlatformServiceProvider(ABC):
    """Simplified provider for single platform per wheel"""
    
    @abstractmethod
    def get_service(self):
        """Get the platform service (only one per wheel)"""
        pass

class SinglePlatformServiceProvider(PlatformServiceProvider):
    """Provider for single platform wheel"""
    
    def __init__(self, platform_service):
        self._service = platform_service
    
    def get_service(self):
        return self._service
    
    def set_service(self, service):
        self._service = service

# Platform-specific implementations (one per wheel)
# In kindling-synapse wheel:
def create_synapse_provider(config, logger):
    from kindling.platform_synapse import SynapseService
    service = SynapseService(config, logger)
    return SinglePlatformServiceProvider(service)

# In kindling-fabric wheel:  
def create_fabric_provider(config, logger):
    from kindling.platform_fabric import FabricService
    service = FabricService(config, logger)
    return SinglePlatformServiceProvider(service)
```

### Phase 3: Remove Platform Registry

#### Simplify Platform Module Imports:
```python
# In kindling-synapse wheel: platform_synapse.py
from kindling.spark_session import *
from kindling.notebook_framework import *
from kindling.data_apps import AppDeploymentService

# No more conditional imports!
from azure.core.exceptions import *
from azure.synapse.artifacts import ArtifactsClient
from azure.synapse.artifacts.models import *
from azure.core.credentials import TokenCredential, AccessToken
from notebookutils import mssparkutils

class SynapseTokenCredential(TokenCredential):
    def __init__(self, expires_on=None):
        token = mssparkutils.credentials.getToken("Synapse")
        self.token = token
        self.expires_on = expires_on or (time.time() + 3600)

    def get_token(self, *scopes, **kwargs):
        return AccessToken(self.token, self.expires_on)

class SynapseAppDeploymentService(AppDeploymentService):
    """Synapse-specific implementation - always available in synapse wheel"""
    # ... implementation stays the same

class SynapseService(PlatformService):
    """Synapse platform service - always available"""
    # ... implementation stays the same
```

### Phase 4: Update CI/CD for Multi-Wheel

#### GitHub Actions Workflow:
```yaml
# .github/workflows/ci.yml (updated)
build-platform-wheels:
  name: Build Platform Wheels
  runs-on: ubuntu-latest
  strategy:
    matrix:
      platform: [synapse, fabric, databricks, local]
      
  steps:
    - name: Checkout code
      uses: actions/checkout@v4
      
    - name: Setup Python
      uses: actions/setup-python@v4
      with:
        python-version: '3.11'
        
    - name: Install build dependencies
      run: |
        pip install build
        
    - name: Build ${{ matrix.platform }} wheel
      run: |
        python scripts/build_platform_wheels.py --platform ${{ matrix.platform }}
        
    - name: Upload wheel artifacts
      uses: actions/upload-artifact@v3
      with:
        name: kindling-${{ matrix.platform }}-wheel
        path: dist/kindling_${{ matrix.platform }}-*.whl

test-platform-wheels:
  name: Test Platform Wheels
  runs-on: ubuntu-latest
  needs: build-platform-wheels
  strategy:
    matrix:
      platform: [synapse, fabric, databricks, local]
      
  steps:
    - name: Download ${{ matrix.platform }} wheel
      uses: actions/download-artifact@v3
      with:
        name: kindling-${{ matrix.platform }}-wheel
        
    - name: Install and test wheel
      run: |
        pip install kindling_${{ matrix.platform }}-*.whl
        python -c "
        from kindling.platform_provider import create_${matrix.platform}_provider
        from kindling.data_apps import DataAppManager
        print('✅ kindling-${{ matrix.platform }} wheel works!')
        "

publish-platform-wheels:
  name: Publish to PyPI
  runs-on: ubuntu-latest
  needs: [build-platform-wheels, test-platform-wheels]
  if: github.event_name == 'release'
  strategy:
    matrix:
      platform: [synapse, fabric, databricks, local]
      
  steps:
    - name: Download wheels
      uses: actions/download-artifact@v3
      with:
        name: kindling-${{ matrix.platform }}-wheel
        path: dist/
        
    - name: Publish kindling-${{ matrix.platform }} to PyPI
      uses: pypa/gh-action-pypi-publish@release/v1
      with:
        password: ${{ secrets.PYPI_API_TOKEN }}
```

## 🎯 User Experience

### Installation:
```bash
# Install only what you need
pip install kindling-synapse    # For Azure Synapse
pip install kindling-fabric     # For Microsoft Fabric  
pip install kindling-databricks # For Databricks
pip install kindling-local      # For local development
```

### Usage (same API):
```python
# Works the same regardless of which wheel is installed
from kindling.data_apps import DataAppManager
from kindling.platform_provider import create_platform_provider

# The provider automatically uses the installed platform
manager = DataAppManager()
kda_path = manager.package_app("./my-app", target_platform="synapse")
```

### Development:
```bash
# Test synapse wheel
pip install kindling-synapse
python tests/system/test_synapse_system.py

# Test fabric wheel  
pip uninstall kindling-synapse
pip install kindling-fabric
python tests/system/test_fabric_system.py
```

## 🚀 Migration Strategy

### Phase 1: Build Infrastructure (Week 1)
- [ ] Create `scripts/build_platform_wheels.py`
- [ ] Modify `platform_provider.py` for single platform
- [ ] Update CI/CD workflows for multi-wheel build

### Phase 2: Platform Module Cleanup (Week 2)
- [ ] Remove conditional imports from all platform modules
- [ ] Simplify `SynapseAppDeploymentService` (no more mocking)
- [ ] Update platform service initialization
- [ ] Remove platform registry complexity

### Phase 3: Testing Updates (Week 3)
- [ ] Create platform-specific test suites
- [ ] Update system tests for single-platform wheels
- [ ] Test installation and import of each wheel
- [ ] Validate KDA packaging works with each platform

### Phase 4: Documentation & Release (Week 4)
- [ ] Update installation documentation
- [ ] Create platform-specific usage guides
- [ ] Test public PyPI publication
- [ ] Release all four wheels simultaneously

## ✅ Benefits Achieved

### For Developers:
- **Simpler testing**: No more conditional import mocking
- **Cleaner code**: No platform detection logic needed
- **Faster development**: Install only the platform you're working on

### For Users:
- **Smaller installs**: Only platform-specific dependencies
- **Clearer choice**: Explicit platform selection
- **Better errors**: Import failures are obvious (wrong wheel installed)

### For CI/CD:
- **Parallel builds**: Each platform builds independently
- **Platform-specific tests**: Each wheel tested in isolation
- **Selective deployment**: Deploy wheels independently

This architecture aligns perfectly with your existing platform-specific wheel support in the data apps framework and eliminates the conditional import complexity that's been causing testing issues!