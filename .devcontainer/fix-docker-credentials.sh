#!/bin/bash
# Fix Docker credentials issue on WSL
# This removes docker-credential-desktop.exe from PATH temporarily during build

set -e

echo "Fixing Docker credentials for WSL..."

# Create a temporary docker config without credential helpers
mkdir -p ~/.docker
if [ -f ~/.docker/config.json ]; then
    echo "Backing up existing Docker config..."
    cp ~/.docker/config.json ~/.docker/config.json.backup
fi

# Create minimal config without credsStore
cat > ~/.docker/config.json << 'EOF'
{
  "auths": {}
}
EOF

echo "Docker credentials fixed. You can now rebuild the devcontainer."
echo "To restore your original config, run:"
echo "  mv ~/.docker/config.json.backup ~/.docker/config.json"
