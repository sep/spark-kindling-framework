# DevContainer Troubleshooting

## Docker Credential Error on WSL

### Problem
```
ERROR resolve image config for docker.io/docker/dockerfile:1.4
error getting credentials - err: docker-credential-desktop.exe resolves to executable in current directory
```

### Solution 1: Fix Docker Config (Recommended)
Run the fix script before rebuilding:
```bash
cd /home/jtdossett/dev/spark-kindling-framework/.devcontainer
./fix-docker-credentials.sh
```

Then rebuild the container in VS Code:
- Press `F1` or `Ctrl+Shift+P`
- Type "Dev Containers: Rebuild Container"
- Select it and wait for the build to complete

### Solution 2: Manual Fix
Edit your `~/.docker/config.json` file and remove or comment out the `credsStore` line:

```json
{
  "auths": {},
  // "credsStore": "desktop.exe"  <- Remove this line
}
```

### Solution 3: Clean Build
If the above doesn't work, try a clean rebuild:

```bash
# Remove old containers and images
docker compose -f .devcontainer/docker-compose.yml down
docker system prune -a

# Then rebuild in VS Code
```

## Requirements Installation

The devcontainer is configured to automatically install requirements on creation via:
```json
"postCreateCommand": "pip install -r requirements.txt && pip install -e .[dev]"
```

### Manual Installation
If you need to manually install requirements:

```bash
# Inside the devcontainer
pip install -r requirements.txt
pip install -e .[dev]

# Or with Poetry (if you prefer)
pip install poetry
poetry install --with dev
```

## Verifying Installation

Once the container is running, verify everything is installed:

```bash
# Check Python
python --version  # Should be 3.11.x

# Check Java
java -version  # Should be 21.x

# Check PySpark
python -c "import pyspark; print(pyspark.__version__)"  # Should be 3.5.5

# Check Delta
python -c "import delta; print(delta.__version__)"  # Should be 3.3.2

# Run tests
pytest tests/unit/ -v
```

## Common Issues

### 1. Port 4040 Already in Use
The Spark UI port is forwarded by default. If you get a port conflict:
- Stop other Spark applications
- Or change the port in `devcontainer.json`

### 2. Spark Warehouse Permissions
If you get permission errors with `/spark-warehouse`:
```bash
sudo chown -R vscode:vscode /spark-warehouse
```

### 3. Module Not Found Errors
If Python can't find the `kindling` module:
```bash
# Ensure PYTHONPATH is set
export PYTHONPATH=/workspace
# Or reinstall in development mode
pip install -e .
```

### 4. Azure CLI Not Working
If `az` command is not found:
```bash
# Reinstall Azure CLI
curl -sL https://aka.ms/InstallAzureCLIDeb | bash
```

## Environment Variables

The devcontainer supports loading `.env` files. Create one at the workspace root:

```bash
cp .env.template .env
# Edit .env with your settings
```

The `postStartCommand` will automatically load it on container start.

## Rebuild vs Reload

- **Rebuild Container**: Recreates the container from scratch (use when Dockerfile changes)
- **Reload Window**: Reconnects to existing container (faster, preserves state)

## Getting Help

If issues persist:
1. Check the VS Code Output panel (View > Output > select "Dev Containers")
2. Check Docker logs: `docker logs <container-id>`
3. Ensure Docker Desktop is running and up to date
4. Try rebuilding with no cache: `docker compose build --no-cache`
