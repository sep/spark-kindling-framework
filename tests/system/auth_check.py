#!/usr/bin/env python3
"""
Preflight checks for Azure-backed system tests.

Validates:
1. Explicit service principal credentials when present
2. Default/system Azure credential fallback otherwise
3. ADLS Gen2 access used for app uploads
4. Platform-specific configuration required by system tests
"""

import argparse
import hashlib
import os
import sys
from typing import Iterable, Optional


ALL_PLATFORMS = ("fabric", "synapse", "databricks")


def _masked(value: str) -> str:
    return f"{value[:8]}..." if value else "<missing>"


def _should_print_hashes() -> bool:
    return (os.getenv("KINDLING_AUTH_DEBUG_HASHES") or "").strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }


def _fingerprint(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _print_hash_diagnostics() -> None:
    if not _should_print_hashes():
        return

    print("\n🔎 Credential fingerprints")
    for key in ("AZURE_TENANT_ID", "AZURE_CLIENT_ID", "AZURE_CLIENT_SECRET"):
        value = os.getenv(key) or ""
        fingerprint = _fingerprint(value) if value else "<missing>"
        print(f"   {key}: len={len(value)} sha256={fingerprint}")

    secret = os.getenv("AZURE_CLIENT_SECRET") or ""
    print(
        "   AZURE_CLIENT_SECRET flags: "
        f"has_newline={'yes' if chr(10) in secret else 'no'} "
        f"has_carriage_return={'yes' if chr(13) in secret else 'no'} "
        f"leading_whitespace={'yes' if secret[:1].isspace() else 'no'} "
        f"trailing_whitespace={'yes' if secret[-1:].isspace() else 'no'}"
    )


def _create_credential():
    from azure.identity import ClientSecretCredential, DefaultAzureCredential

    tenant_id = (os.getenv("AZURE_TENANT_ID") or "").strip()
    client_id = (os.getenv("AZURE_CLIENT_ID") or "").strip()
    client_secret = (os.getenv("AZURE_CLIENT_SECRET") or "").strip()

    has_sp = bool(tenant_id and client_id and client_secret)
    if has_sp:
        print("\n📋 Found service principal environment variables")
        print(f"   AZURE_TENANT_ID: {_masked(tenant_id)}")
        print(f"   AZURE_CLIENT_ID: {_masked(client_id)}")
        print("   AZURE_CLIENT_SECRET: ********************")
        print("\n🔐 Using explicit ClientSecretCredential...")
        return (
            ClientSecretCredential(
                tenant_id=tenant_id,
                client_id=client_id,
                client_secret=client_secret,
            ),
            "service_principal",
        )

    print("\n📋 Service principal environment variables not fully set")
    print("🔐 Falling back to DefaultAzureCredential...")
    return DefaultAzureCredential(), "default"


def check_authentication() -> bool:
    """Validate Azure credential acquisition for ARM and Storage."""
    print("\n" + "=" * 60)
    print("Azure Authentication Test")
    print("=" * 60)

    try:
        from azure.identity import ClientSecretCredential, DefaultAzureCredential  # noqa: F401

        print("✅ Azure Identity SDK available")
    except ImportError as exc:
        print(f"❌ Azure Identity SDK not installed: {exc}")
        return False

    try:
        credential, credential_mode = _create_credential()
        arm_token = credential.get_token("https://management.azure.com/.default")
        storage_token = credential.get_token("https://storage.azure.com/.default")
        mode_label = (
            "Service principal authentication successful"
            if credential_mode == "service_principal"
            else "DefaultAzureCredential authentication successful"
        )
        print(f"✅ {mode_label}!")
        print(f"   ARM token expires: {arm_token.expires_on}")
        print(f"   Storage token expires: {storage_token.expires_on}")
        return True
    except Exception as exc:
        print(f"❌ Azure authentication failed: {exc}")
        return False


def check_storage_access() -> bool:
    """Validate ADLS Gen2 access used for deploy_app uploads."""
    print("\n" + "=" * 60)
    print("Storage Access Test")
    print("=" * 60)

    storage_account = (os.getenv("AZURE_STORAGE_ACCOUNT") or "").strip()
    container = (os.getenv("AZURE_CONTAINER") or "artifacts").strip()

    if not storage_account:
        print("❌ Missing AZURE_STORAGE_ACCOUNT")
        return False

    try:
        from azure.storage.filedatalake import DataLakeServiceClient
    except ImportError as exc:
        print(f"❌ azure-storage-file-datalake not installed: {exc}")
        return False

    try:
        credential, _ = _create_credential()
        account_url = f"https://{storage_account}.dfs.core.windows.net"
        service_client = DataLakeServiceClient(account_url=account_url, credential=credential)
        file_system_client = service_client.get_file_system_client(file_system=container)
        props = file_system_client.get_file_system_properties()
        print("✅ ADLS access successful!")
        print(f"   Storage account: {storage_account}")
        print(f"   Container: {container}")
        print(f"   ETag: {props.get('etag')}")
        return True
    except Exception as exc:
        print(f"❌ ADLS access failed: {exc}")
        return False


def check_platform_config(platform: str) -> bool:
    """Validate platform-specific environment variables used by system tests."""
    print("\n" + "=" * 60)
    print(f"{platform.capitalize()} Configuration Test")
    print("=" * 60)

    if platform == "fabric":
        required = ["FABRIC_WORKSPACE_ID", "FABRIC_LAKEHOUSE_ID"]
    elif platform == "synapse":
        required = ["SYNAPSE_WORKSPACE_NAME"]
    elif platform == "databricks":
        required = ["DATABRICKS_HOST", "DATABRICKS_CLUSTER_ID"]
    else:
        print(f"❌ Unsupported platform: {platform}")
        return False

    missing = [key for key in required if not (os.getenv(key) or "").strip()]
    if missing:
        print(f"❌ Missing {platform} configuration: {', '.join(missing)}")
        return False

    print(f"✅ {platform.capitalize()} configuration found")
    for key in required:
        value = (os.getenv(key) or "").strip()
        print(f"   {key}: {value}")

    if platform in {"fabric", "synapse"}:
        key_vault_url = (
            os.getenv(f"CONFIG__platform_{platform}__kindling__secrets__key_vault_url") or ""
        ).strip() or (os.getenv("SYSTEM_TEST_KEY_VAULT_URL") or "").strip()
        if not key_vault_url:
            print(
                f"❌ Missing runtime secret provider config for {platform}: "
                f"CONFIG__platform_{platform}__kindling__secrets__key_vault_url or SYSTEM_TEST_KEY_VAULT_URL"
            )
            return False
        print(f"   key_vault_url: {key_vault_url}")

    return True


def _resolve_platforms(requested: Optional[str]) -> Iterable[str]:
    if requested:
        return (requested.strip().lower(),)
    return ALL_PLATFORMS


def main() -> int:
    parser = argparse.ArgumentParser(description="Validate auth/config for system tests")
    parser.add_argument(
        "--platform",
        choices=list(ALL_PLATFORMS),
        default="",
        help="Validate config for a single platform. Defaults to all platforms.",
    )
    args = parser.parse_args()

    _print_hash_diagnostics()

    auth_ok = check_authentication()
    storage_ok = check_storage_access()
    platform_results = [
        check_platform_config(platform) for platform in _resolve_platforms(args.platform)
    ]
    platform_ok = all(platform_results)

    print("\n" + "=" * 60)
    print("Summary")
    print("=" * 60)

    if auth_ok and storage_ok and platform_ok:
        print("✅ All checks passed! You're ready to run system tests.")
        return 0

    print("❌ Some checks failed. Fix the reported auth/config issues before running system tests.")
    return 1


if __name__ == "__main__":
    sys.exit(main())
