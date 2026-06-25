#!/usr/bin/env python3
"""
Manage Unity Catalog Volumes for Databricks cluster logs
"""

import sys

from databricks.sdk import WorkspaceClient

from kindling.injection import get_kindling_service
from kindling.platform_databricks import DatabricksAPI


def list_volumes(catalog: str = "medallion", schema: str = "default"):
    """List all volumes in a catalog.schema"""
    print(f"\n📋 Listing volumes in {catalog}.{schema}...")

    try:
        db = get_kindling_service(DatabricksAPI)
        client: WorkspaceClient = db.client

        volumes = list(client.volumes.list(catalog_name=catalog, schema_name=schema))

        if not volumes:
            print(f"   ⚠️  No volumes found in {catalog}.{schema}")
            return []

        print(f"\n✅ Found {len(volumes)} volume(s):\n")
        for vol in volumes:
            print(f"   📁 {vol.full_name}")
            print(f"      Type: {vol.volume_type}")
            if vol.storage_location:
                print(f"      Storage: {vol.storage_location}")
            print()

        return volumes

    except Exception as e:
        print(f"   ❌ Error listing volumes: {e}")
        return []


def create_logs_volume(
    catalog: str = "medallion",
    schema: str = "default",
    volume_name: str = "logs",
    storage_location: str = None,
):
    """Create a volume for cluster logs"""
    print(f"\n🔨 Creating volume {catalog}.{schema}.{volume_name}...")

    try:
        db = get_kindling_service(DatabricksAPI)
        client: WorkspaceClient = db.client

        # Default storage location to artifacts container logs path
        if not storage_location and db.storage_account and db.container:
            storage_location = (
                f"abfss://{db.container}@{db.storage_account}.dfs.core.windows.net/logs"
            )

        print(f"   Storage location: {storage_location}")

        from databricks.sdk.service.catalog import VolumeType

        volume = client.volumes.create(
            catalog_name=catalog,
            schema_name=schema,
            name=volume_name,
            volume_type=VolumeType.EXTERNAL,
            storage_location=storage_location,
            comment="Cluster logs for Kindling system tests",
        )

        print(f"\n✅ Volume created successfully!")
        print(f"   Full name: {volume.full_name}")
        print(f"   Volume ID: {volume.volume_id}")
        print(f"   Storage: {volume.storage_location}")

        return volume

    except Exception as e:
        print(f"\n❌ Error creating volume: {e}")
        print(f"\nNote: You may need to:")
        print(f"  1. Ensure catalog '{catalog}' exists")
        print(f"  2. Ensure schema '{schema}' exists")
        print(f"  3. Grant CREATE EXTERNAL VOLUME permission")
        print(f"  4. Ensure storage location is accessible")
        return None


def check_catalog_schema(catalog: str = "medallion", schema: str = "default"):
    """Check if catalog and schema exist"""
    print(f"\n🔍 Checking catalog and schema...")

    try:
        db = get_kindling_service(DatabricksAPI)
        client: WorkspaceClient = db.client

        # Check catalog
        try:
            cat = client.catalogs.get(catalog)
            print(f"   ✅ Catalog '{catalog}' exists")
        except Exception:
            print(f"   ❌ Catalog '{catalog}' not found")
            print(f"      Create with: databricks catalogs create {catalog}")
            return False

        # Check schema
        try:
            sch = client.schemas.get(f"{catalog}.{schema}")
            print(f"   ✅ Schema '{catalog}.{schema}' exists")
        except Exception:
            print(f"   ❌ Schema '{catalog}.{schema}' not found")
            print(f"      Create with: databricks schemas create {catalog}.{schema}")
            return False

        return True

    except Exception as e:
        print(f"   ❌ Error checking catalog/schema: {e}")
        return False


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Manage UC Volumes for Databricks")
    parser.add_argument("action", choices=["list", "create", "check"], help="Action to perform")
    parser.add_argument("--catalog", default="medallion", help="Catalog name")
    parser.add_argument("--schema", default="default", help="Schema name")
    parser.add_argument("--volume", default="logs", help="Volume name (for create)")
    parser.add_argument("--storage", help="Storage location (for create)")

    args = parser.parse_args()

    if args.action == "list":
        list_volumes(args.catalog, args.schema)

    elif args.action == "check":
        if check_catalog_schema(args.catalog, args.schema):
            print("\n✅ Catalog and schema are ready")
            list_volumes(args.catalog, args.schema)
        else:
            print("\n❌ Catalog or schema missing")
            sys.exit(1)

    elif args.action == "create":
        if not check_catalog_schema(args.catalog, args.schema):
            print("\n❌ Cannot create volume - catalog or schema missing")
            sys.exit(1)

        result = create_logs_volume(args.catalog, args.schema, args.volume, args.storage)
        sys.exit(0 if result else 1)
