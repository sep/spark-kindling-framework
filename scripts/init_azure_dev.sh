#!/bin/bash
#
# Azure Development Environment Initialization Script
# 
# This script:
# 1. Checks if user is logged into Azure
# 2. Performs device code login if needed
# 3. Detects current Azure cloud environment automatically
# 4. Validates storage account access
# 5. Securely retrieves and sets storage key as `AZURE_STORAGE_KEY`
# 6. Sets helper environment variables (account name, container, cloud)
#
# Usage:
#   source scripts/init_azure_dev.sh                               # Auto-detect cloud, use default storage
#   source scripts/init_azure_dev.sh --verify-account              # Prompt to verify/change account
#   source scripts/init_azure_dev.sh --cloud AzureCloud            # Explicitly set cloud (AzureCloud, AzureUSGovernment, AzureChinaCloud)
#   source scripts/init_azure_dev.sh --storage-account myaccount   # Override storage account name
#   source scripts/init_azure_dev.sh --container mycontainer       # Override container name
#
# Note: Must be sourced (not executed) to set environment variables in current shell
#

set -e  # Exit on error

# Colors for output (optional, comment out if terminal doesn't support)
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Default Configuration (can be overridden via arguments or auto-detected)
AZURE_CLOUD=""  # Auto-detect if not specified
STORAGE_ACCOUNT_NAME="rrdefcfesffawkes"
STORAGE_CONTAINER="dev-fawkes-prod-data"

# Parse command line arguments
VERIFY_ACCOUNT=false
while [[ $# -gt 0 ]]; do
    case "$1" in
        --verify-account)
            VERIFY_ACCOUNT=true
            shift
            ;;
        --cloud)
            AZURE_CLOUD="$2"
            shift 2
            ;;
        --storage-account)
            STORAGE_ACCOUNT_NAME="$2"
            shift 2
            ;;
        --container)
            STORAGE_CONTAINER="$2"
            shift 2
            ;;
        *)
            echo -e "${RED}Unknown argument: $1${NC}"
            return 1
            ;;
    esac
done

echo -e "${BLUE}🔧 Azure Development Environment Setup${NC}"
echo "=================================================="

# Function to check if logged in
check_azure_login() {
    if az account show &>/dev/null; then
        return 0
    else
        return 1
    fi
}

# Function to get current cloud
get_current_cloud() {
    az cloud show --query name -o tsv 2>/dev/null || echo "unknown"
}

# Function to get cloud endpoint suffix
get_cloud_endpoint_suffix() {
    local cloud_name="$1"
    case "$cloud_name" in
        AzureCloud)
            echo "dfs.core.windows.net"
            ;;
        AzureUSGovernment)
            echo "dfs.core.usgovcloudapi.net"
            ;;
        AzureChinaCloud)
            echo "dfs.core.chinacloudapi.cn"
            ;;
        *)
            echo "dfs.core.windows.net"  # Default to commercial
            ;;
    esac
}

# Function to map cloud name to simplified version
get_cloud_simple_name() {
    local cloud_name="$1"
    case "$cloud_name" in
        AzureCloud)
            echo "public"
            ;;
        AzureUSGovernment)
            echo "government"
            ;;
        AzureChinaCloud)
            echo "china"
            ;;
        *)
            echo "public"
            ;;
    esac
}

# Step 1: Check Azure Cloud Configuration
echo -e "\n${BLUE}Step 1:${NC} Checking Azure Cloud configuration..."
CURRENT_CLOUD=$(get_current_cloud)

if [ -z "$AZURE_CLOUD" ]; then
    # Auto-detect: use current cloud
    AZURE_CLOUD="$CURRENT_CLOUD"
    echo -e "${GREEN}✓ Auto-detected cloud: $AZURE_CLOUD${NC}"
elif [ "$CURRENT_CLOUD" != "$AZURE_CLOUD" ]; then
    echo -e "${YELLOW}⚠ Current cloud: $CURRENT_CLOUD${NC}"
    echo -e "${YELLOW}⚠ Setting cloud to: $AZURE_CLOUD${NC}"
    az cloud set --name "$AZURE_CLOUD" &>/dev/null
    echo -e "${GREEN}✓ Cloud set to $AZURE_CLOUD${NC}"
else
    echo -e "${GREEN}✓ Using $AZURE_CLOUD${NC}"
fi

# Get cloud-specific configuration
CLOUD_ENDPOINT_SUFFIX=$(get_cloud_endpoint_suffix "$AZURE_CLOUD")
CLOUD_SIMPLE_NAME=$(get_cloud_simple_name "$AZURE_CLOUD")

# Step 2: Check Login Status
echo -e "\n${BLUE}Step 2:${NC} Checking Azure login status..."

if check_azure_login; then
    ACCOUNT_INFO=$(az account show --query "{subscription:name, tenant:tenantDisplayName, user:user.name}" -o json 2>/dev/null)
    SUBSCRIPTION=$(echo "$ACCOUNT_INFO" | jq -r '.subscription')
    TENANT=$(echo "$ACCOUNT_INFO" | jq -r '.tenant')
    USER=$(echo "$ACCOUNT_INFO" | jq -r '.user')
    
    echo -e "${GREEN}✓ Already logged in${NC}"
    echo -e "  Subscription: ${BLUE}$SUBSCRIPTION${NC}"
    echo -e "  Tenant: ${BLUE}$TENANT${NC}"
    echo -e "  User: ${BLUE}$USER${NC}"
    
    # Only prompt if --verify-account flag is set
    if [ "$VERIFY_ACCOUNT" = true ]; then
        read -p "Continue with this account? (Y/n): " -n 1 -r
        echo
        if [[ ! $REPLY =~ ^[Yy]$ ]] && [[ ! -z $REPLY ]]; then
            echo -e "${YELLOW}Logging out and re-authenticating...${NC}"
            az logout &>/dev/null
            check_azure_login() { return 1; }
        fi
    fi
fi

if ! check_azure_login; then
    echo -e "${YELLOW}⚠ Not logged in to Azure${NC}"
    echo -e "\n${BLUE}Initiating device code login...${NC}"
    echo "A browser window will open for authentication."
    
    # Perform device code login
    if az login --use-device-code --output none; then
        echo -e "${GREEN}✓ Successfully logged in${NC}"
        
        # Display account info
        ACCOUNT_INFO=$(az account show --query "{subscription:name, tenant:tenantDisplayName}" -o json)
        SUBSCRIPTION=$(echo "$ACCOUNT_INFO" | jq -r '.subscription')
        TENANT=$(echo "$ACCOUNT_INFO" | jq -r '.tenant')
        echo -e "  Subscription: ${BLUE}$SUBSCRIPTION${NC}"
        echo -e "  Tenant: ${BLUE}$TENANT${NC}"
    else
        echo -e "${RED}✗ Login failed${NC}"
        return 1
    fi
fi

# Step 3: Verify Storage Account Access
echo -e "\n${BLUE}Step 3:${NC} Verifying storage account access..."

if az storage account show --name "$STORAGE_ACCOUNT_NAME" --query name -o tsv &>/dev/null; then
    echo -e "${GREEN}✓ Storage account '$STORAGE_ACCOUNT_NAME' accessible${NC}"
else
    echo -e "${RED}✗ Cannot access storage account '$STORAGE_ACCOUNT_NAME'${NC}"
    echo -e "${YELLOW}  Please verify:"
    echo -e "  1. You have permissions to access this storage account"
    echo -e "  2. The storage account name is correct${NC}"
    return 1
fi

# Step 4: Securely Retrieve Storage Key
echo -e "\n${BLUE}Step 4:${NC} Retrieving storage account key..."

# Disable command echo and history for this section
set +x 2>/dev/null
HISTFILE_BACKUP=$HISTFILE
unset HISTFILE

# Retrieve key silently (redirecting all output to /dev/null for security)
STORAGE_KEY=$(az storage account keys list \
    --account-name "$STORAGE_ACCOUNT_NAME" \
    --query "[0].value" \
    -o tsv 2>/dev/null)

# Restore history
export HISTFILE=$HISTFILE_BACKUP

if [ -z "$STORAGE_KEY" ]; then
    echo -e "${RED}✗ Failed to retrieve storage key${NC}"
    echo -e "${YELLOW}  Please check your permissions${NC}"
    return 1
fi

# Set environment variable (silently)
export AZURE_STORAGE_KEY="$STORAGE_KEY"
export AZURE_STORAGE_ACCOUNT="$STORAGE_ACCOUNT_NAME"
export AZURE_STORAGE_CONTAINER="$STORAGE_CONTAINER"
export AZURE_CLOUD_ENV="$AZURE_CLOUD"
export AZURE_CLOUD_SIMPLE="$CLOUD_SIMPLE_NAME"
export AZURE_STORAGE_ENDPOINT_SUFFIX="$CLOUD_ENDPOINT_SUFFIX"

# Also save to .env file for persistence
echo -e "\n${BLUE}Step 5:${NC} Saving configuration to .env file..."
ENV_FILE="/workspace/.env"

# Create .env file with current configuration
cat > "$ENV_FILE" << EOF
# Kindling Framework Environment Configuration
# Generated by Azure development setup script

AZURE_CLOUD=$AZURE_CLOUD
AZURE_STORAGE_ACCOUNT=$STORAGE_ACCOUNT_NAME
AZURE_CONTAINER=$STORAGE_CONTAINER
AZURE_BASE_PATH=kindling/tests
AZURE_STORAGE_KEY=$STORAGE_KEY
EOF

echo -e "${GREEN}✓ Configuration saved to .env file${NC}"
echo -e "${YELLOW}💡 Tip: The .env file will be automatically loaded in future sessions${NC}"

# Clear the key variable from memory
unset STORAGE_KEY

echo -e "${GREEN}✓ Storage key retrieved and set securely${NC}"
echo -e "${GREEN}✓ Environment variable AZURE_STORAGE_KEY is now set${NC}"

# Step 6: Load .env file into current session
echo -e "\n${BLUE}Step 6:${NC} Loading .env file into current session..."
if [ -f "$ENV_FILE" ]; then
    set -a  # automatically export all variables
    source "$ENV_FILE"
    set +a  # turn off automatic export
    echo -e "${GREEN}✓ Environment variables loaded from .env file${NC}"
else
    echo -e "${YELLOW}⚠ .env file not found, using exported variables${NC}"
fi

# Step 7: Summary
echo -e "\n${GREEN}=================================================="
echo -e "✓ Azure Development Environment Ready!"
echo -e "==================================================${NC}"
echo ""
echo -e "${BLUE}Environment Variables Set:${NC}"
echo -e "  AZURE_CLOUD_ENV=${AZURE_CLOUD_ENV}"
echo -e "  AZURE_CLOUD_SIMPLE=${AZURE_CLOUD_SIMPLE}"
echo -e "  AZURE_STORAGE_ACCOUNT=${AZURE_STORAGE_ACCOUNT}"
echo -e "  AZURE_STORAGE_CONTAINER=${AZURE_STORAGE_CONTAINER}"
echo -e "  AZURE_STORAGE_ENDPOINT_SUFFIX=${AZURE_STORAGE_ENDPOINT_SUFFIX}"
echo -e "  AZURE_STORAGE_KEY=[REDACTED - securely set]"
echo ""
echo -e "${BLUE}Storage Access:${NC}"
echo -e "  abfss://${AZURE_STORAGE_CONTAINER}@${AZURE_STORAGE_ACCOUNT}.${AZURE_STORAGE_ENDPOINT_SUFFIX}/"
echo ""
echo -e "${BLUE}Quick Start:${NC}"
echo -e "  python3 tests/integration/test_azure_storage_with_key.py"
echo ""
echo -e "${YELLOW}Note: Storage key is set for this shell session only${NC}"
echo -e "${YELLOW}      Run 'source scripts/init_azure_dev.sh' in each new terminal${NC}"
