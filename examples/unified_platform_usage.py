"""
Examples of how users would install and use the unified approach.

This eliminates the need for separate wheels while providing clean platform separation.
"""

# Installation examples:
"""
# Install core framework only (local development)
pip install kindling

# Install with Synapse support
pip install kindling[synapse]

# Install with Fabric support  
pip install kindling[fabric]

# Install with Databricks support
pip install kindling[databricks]

# Install with all platform support
pip install kindling[all]

# Install for development
pip install kindling[dev]

# Install multiple extras
pip install kindling[synapse,dev]
"""

# Usage examples:
"""
from kindling.data_apps import DataAppManager

# Auto-detection (based on environment or available dependencies)
manager = DataAppManager()  # Will auto-detect platform

# Explicit platform specification
manager = DataAppManager(platform="synapse")

# Check available platforms
from kindling.platform_provider_unified import get_available_platforms
print(f"Available platforms: {get_available_platforms()}")

# Example output based on installed extras:
# - pip install kindling          -> ['local']
# - pip install kindling[synapse] -> ['local', 'synapse'] 
# - pip install kindling[all]     -> ['local', 'synapse', 'fabric', 'databricks']
"""

# Deployment examples:
"""
# For CI/CD pipelines targeting specific platforms:

# Dockerfile for Synapse deployment
FROM python:3.11
RUN pip install kindling[synapse]
COPY app/ /app/
WORKDIR /app
CMD ["python", "main.py"]

# Dockerfile for Fabric deployment  
FROM python:3.11
RUN pip install kindling[fabric]
COPY app/ /app/
WORKDIR /app
CMD ["python", "main.py"]

# Development container with all platforms
FROM python:3.11
RUN pip install kindling[all,dev]
# ... rest of dev setup
"""

# Advanced usage:
"""
# Check if specific platform is available
from kindling.platform_provider_unified import get_platform_provider

try:
    provider = get_platform_provider("synapse")
    print("Synapse platform available")
except RuntimeError as e:
    print(f"Synapse not available: {e}")
    # Error message will suggest: pip install kindling[synapse]

# Graceful fallback
available_platforms = get_available_platforms()
if "synapse" in available_platforms:
    manager = DataAppManager("synapse")
elif "fabric" in available_platforms:
    manager = DataAppManager("fabric")
else:
    manager = DataAppManager("local")  # Always available
"""

# GitHub Actions matrix example:
"""
# .github/workflows/test.yml
strategy:
  matrix:
    platform: [local, synapse, fabric, databricks]
    include:
      - platform: local
        install: "kindling[dev]"
      - platform: synapse  
        install: "kindling[synapse,dev]"
      - platform: fabric
        install: "kindling[fabric,dev]"
      - platform: databricks
        install: "kindling[databricks,dev]"

steps:
  - uses: actions/checkout@v4
  - uses: actions/setup-python@v4
  - run: pip install ${{ matrix.install }}
  - run: pytest tests/platform/${{ matrix.platform }}/
"""
