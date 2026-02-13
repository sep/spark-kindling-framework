#!/usr/bin/env python3
"""
Test script to verify Azure authentication is working.

This script tests whether you can authenticate to Azure using either:
1. Service principal (environment variables)
2. Azure CLI (az login)
"""

import os
import sys

try:
    from azure.identity import ClientSecretCredential, DefaultAzureCredential

    print("✅ Azure Identity SDK available")
except ImportError as e:
    print(f"❌ Azure Identity SDK not installed: {e}")
    print("   Install with: pip install azure-identity")
    sys.exit(1)


def check_authentication():
    """Test Azure authentication"""
    print("\n" + "=" * 60)
    print("Azure Authentication Test")
    print("=" * 60)

    # Check for service principal credentials
    has_sp = all(
        [
            os.getenv("AZURE_TENANT_ID"),
            os.getenv("AZURE_CLIENT_ID"),
            os.getenv("AZURE_CLIENT_SECRET"),
        ]
    )

    if has_sp:
        print("\n📋 Found service principal environment variables")
        print(f"   AZURE_TENANT_ID: {os.getenv('AZURE_TENANT_ID')[:8]}...")
        print(f"   AZURE_CLIENT_ID: {os.getenv('AZURE_CLIENT_ID')[:8]}...")
        print(f"   AZURE_CLIENT_SECRET: {'*' * 20}")

        # Test service principal authentication
        print("\n🔐 Testing service principal authentication...")
        try:
            credential = ClientSecretCredential(
                tenant_id=os.getenv("AZURE_TENANT_ID"),
                client_id=os.getenv("AZURE_CLIENT_ID"),
                client_secret=os.getenv("AZURE_CLIENT_SECRET"),
            )
            # Try to get a token (for Azure Resource Manager)
            token = credential.get_token("https://management.azure.com/.default")
            print(f"✅ Service principal authentication successful!")
            print(f"   Token expires: {token.expires_on}")
            return True
        except Exception as e:
            print(f"❌ Service principal authentication failed: {e}")
            return False
    else:
        print("\n📋 No service principal credentials found")
        print("   Will try DefaultAzureCredential (az login, managed identity, etc.)")

    # Test DefaultAzureCredential
    print("\n🔐 Testing DefaultAzureCredential...")
    print("   This will try multiple auth methods in order:")
    print("   1. Environment variables (service principal)")
    print("   2. Managed identity")
    print("   3. Azure CLI (az login)")
    print("   4. Visual Studio Code")
    print("   5. Azure PowerShell")
    print("   6. Interactive browser")
    print()

    try:
        credential = DefaultAzureCredential()
        # Try to get a token
        token = credential.get_token("https://management.azure.com/.default")
        print(f"✅ DefaultAzureCredential authentication successful!")
        print(f"   Token expires: {token.expires_on}")

        # Try to determine which method worked
        print("\n💡 Authentication method used:")
        if has_sp:
            print("   → Service Principal (environment variables)")
        else:
            print("   → Likely Azure CLI (az login)")
            print("   → Run 'az account show' to verify")

        return True
    except Exception as e:
        print(f"❌ DefaultAzureCredential authentication failed: {e}")
        print("\n💡 Troubleshooting:")
        print("   1. Run 'az login' to authenticate with Azure CLI")
        print("   2. Or set service principal environment variables:")
        print("      export AZURE_TENANT_ID='...'")
        print("      export AZURE_CLIENT_ID='...'")
        print("      export AZURE_CLIENT_SECRET='...'")
        print("   3. Verify your account has access to Azure resources")
        return False


def check_fabric_config():
    """Test Fabric configuration"""
    print("\n" + "=" * 60)
    print("Fabric Configuration Test")
    print("=" * 60)

    workspace_id = os.getenv("FABRIC_WORKSPACE_ID")
    lakehouse_id = os.getenv("FABRIC_LAKEHOUSE_ID")

    if workspace_id and lakehouse_id:
        print(f"\n✅ Fabric configuration found")
        print(f"   Workspace ID: {workspace_id}")
        print(f"   Lakehouse ID: {lakehouse_id}")
        return True
    else:
        print(f"\n❌ Fabric configuration missing")
        if not workspace_id:
            print("   Missing: FABRIC_WORKSPACE_ID")
        if not lakehouse_id:
            print("   Missing: FABRIC_LAKEHOUSE_ID")

        print("\n💡 To set up:")
        print("   export FABRIC_WORKSPACE_ID='your-workspace-id'")
        print("   export FABRIC_LAKEHOUSE_ID='your-lakehouse-id'")
        return False


def main():
    """Main test runner"""
    auth_ok = check_authentication()
    config_ok = check_fabric_config()

    print("\n" + "=" * 60)
    print("Summary")
    print("=" * 60)

    if auth_ok and config_ok:
        print("✅ All checks passed! You're ready to run system tests.")
        print("\nRun tests with:")
        print("   pytest tests/system/ -v -s -m fabric")
        sys.exit(0)
    else:
        print("❌ Some checks failed. See troubleshooting above.")
        sys.exit(1)


if __name__ == "__main__":
    main()
