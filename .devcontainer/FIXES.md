# DevContainer Fixes Applied

## Changes Made

### 1. Fixed Dockerfile
- Added `# syntax=docker/dockerfile:1` directive at the top to ensure proper build syntax
- This helps with BuildKit compatibility

### 2. Updated devcontainer.json
- Changed `docker-in-docker` feature to use `moby: true` instead of `false`
- Added `dockerDashComposeVersion: "v2"` for better compatibility
- Added `DOCKER_BUILDKIT: "1"` environment variable

### 3. Updated docker-compose.yml
- Added explicit `version: '3.8'` declaration for better compatibility

### 4. Created Troubleshooting Scripts
- **fix-docker-credentials.sh**: Fixes WSL Docker credential issues
- **TROUBLESHOOTING.md**: Comprehensive guide for common issues

### 5. Fixed Docker Credentials
- Ran the fix script to remove `credsStore` from Docker config
- This resolves the `docker-credential-desktop.exe` error on WSL

## Next Steps

1. **Rebuild the DevContainer** in VS Code:
   - Press `F1` or `Ctrl+Shift+P`
   - Type "Dev Containers: Rebuild Container"
   - Select it and wait

2. **Verify Installation** once container is running:
   ```bash
   python --version
   java -version
   python -c "import pyspark; print(pyspark.__version__)"
   pytest tests/unit/ --collect-only
   ```

3. **If Issues Persist**:
   - Check `.devcontainer/TROUBLESHOOTING.md`
   - Try: `docker system prune -a` and rebuild

## Requirements Installation

The devcontainer will automatically run on creation:
```bash
pip install -r requirements.txt && pip install -e .[dev]
```

This installs:
- All dependencies from `requirements.txt`
- The kindling package in editable/development mode
- All dev dependencies

## Files Modified

- `.devcontainer/Dockerfile` - Added syntax directive
- `.devcontainer/devcontainer.json` - Updated Docker-in-Docker config
- `.devcontainer/docker-compose.yml` - Added version declaration
- `~/.docker/config.json` - Removed credential store (backup created)

## Files Created

- `.devcontainer/fix-docker-credentials.sh` - Docker credential fix script
- `.devcontainer/TROUBLESHOOTING.md` - Troubleshooting guide
- `.devcontainer/FIXES.md` - This file

## Rollback

If you need to rollback the Docker config:
```bash
mv ~/.docker/config.json.backup ~/.docker/config.json
```
