#!/usr/bin/env python3
"""
Verify Azure CLI and Python packages are properly installed
"""
import sys


def check_azure_cli():
    """Check if Azure CLI is installed"""
    import subprocess

    try:
        result = subprocess.run(["az", "--version"], capture_output=True, text=True)
        if result.returncode == 0:
            version = result.stdout.split("\n")[0]
            print(f"✅ Azure CLI: {version}")
            return True
        else:
            print("❌ Azure CLI: Not installed or error running")
            return False
    except FileNotFoundError:
        print("❌ Azure CLI: Not found in PATH")
        return False


def check_azure_identity():
    """Check if azure-identity is installed"""
    try:
        import azure.identity

        print(f"✅ azure-identity: {azure.identity.__version__}")
        return True
    except ImportError:
        print("❌ azure-identity: Not installed")
        return False


def check_azure_storage():
    """Check if azure-storage-blob is installed"""
    try:
        import azure.storage.blob

        version = getattr(azure.storage.blob, "__version__", "unknown")
        print(f"✅ azure-storage-blob: {version}")
        return True
    except ImportError:
        print("❌ azure-storage-blob: Not installed")
        return False


def check_azure_core():
    """Check if azure-core is installed"""
    try:
        import azure.core

        print(f"✅ azure-core: {azure.core.__version__}")
        return True
    except ImportError:
        print("❌ azure-core: Not installed")
        return False


def test_azure_cli_login():
    """Test if user is logged in to Azure CLI"""
    import subprocess

    try:
        result = subprocess.run(
            ["az", "account", "show"], capture_output=True, text=True, timeout=5
        )
        if result.returncode == 0:
            import json

            account = json.loads(result.stdout)
            print(f"✅ Azure Login: Logged in as {account.get('user', {}).get('name', 'unknown')}")
            print(f"   Cloud: {account.get('environmentName', 'unknown')}")
            return True
        else:
            print("⚠️  Azure Login: Not logged in")
            print("   Run: az login")
            return False
    except subprocess.TimeoutExpired:
        print("⚠️  Azure Login: Command timed out")
        return False
    except Exception as e:
        print(f"⚠️  Azure Login: Error - {e}")
        return False


def test_default_credential():
    """Test if DefaultAzureCredential works"""
    try:
        from azure.identity import DefaultAzureCredential

        credential = DefaultAzureCredential()
        token = credential.get_token("https://storage.azure.com/.default")
        print(f"✅ DefaultAzureCredential: Working (token expires: {token.expires_on})")
        return True
    except Exception as e:
        print(f"⚠️  DefaultAzureCredential: Error - {e}")
        print("   Make sure you're logged in with: az login")
        return False


def main():
    """Run all checks"""
    print("=" * 60)
    print("Azure CLI and Python Packages Verification")
    print("=" * 60)
    print()

    checks = [
        ("Azure CLI", check_azure_cli),
        ("azure-identity", check_azure_identity),
        ("azure-storage-blob", check_azure_storage),
        ("azure-core", check_azure_core),
    ]

    results = []
    for name, check_func in checks:
        try:
            results.append(check_func())
        except Exception as e:
            print(f"❌ {name}: Exception - {e}")
            results.append(False)

    print()
    print("=" * 60)
    print("Azure Login Status")
    print("=" * 60)
    print()

    # Check login status
    login_ok = test_azure_cli_login()
    results.append(login_ok)

    if login_ok:
        print()
        credential_ok = test_default_credential()
        results.append(credential_ok)

    print()
    print("=" * 60)
    print("Summary")
    print("=" * 60)

    if all(results[:4]):  # Check only required packages
        print("✅ All required packages installed!")
    else:
        print("❌ Some required packages missing")
        print("\nTo install missing packages:")
        print("  pip install azure-identity azure-storage-blob azure-core")

    if not login_ok:
        print("\n⚠️  Not logged in to Azure")
        print("\nTo login:")
        print("  Commercial Cloud: az login")
        print("  Government Cloud: az cloud set --name AzureUSGovernment && az login")
        print("  China Cloud:      az cloud set --name AzureChinaCloud && az login")

    print()

    # Return exit code
    return 0 if all(results[:4]) else 1


if __name__ == "__main__":
    sys.exit(main())
