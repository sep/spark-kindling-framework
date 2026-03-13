#!/usr/bin/env python3
"""Run a Databricks cluster-side UC visibility preflight."""

from __future__ import annotations

import io
import json
import os
import sys
import time
import uuid

from azure.identity import ClientSecretCredential
from azure.storage.filedatalake import DataLakeServiceClient
from databricks.sdk import WorkspaceClient


REMOTE_SCRIPT_TEMPLATE = r"""
import json
import sys

from pyspark.sql import SparkSession


def _rows_to_values(rows):
    preferred_keys = (
        "volume_name",
        "name",
        "schema_name",
        "namespace",
        "catalog_name",
        "catalog",
        "database",
    )
    values = []
    for row in rows:
        if hasattr(row, "asDict"):
            data = row.asDict()
            if data:
                for key in preferred_keys:
                    if key in data and data[key] is not None:
                        values.append(str(data[key]))
                        break
                else:
                    values.append(str(next(iter(data.values()))))
                continue
        try:
            values.append(str(row[0]))
        except Exception:
            values.append(str(row))
    return values


def _get_dbutils(spark):
    try:
        return dbutils  # type: ignore[name-defined]
    except NameError:
        from pyspark.dbutils import DBUtils

        return DBUtils(spark)


catalog = sys.argv[1]
schema = sys.argv[2]
volume = sys.argv[3]
volume_path = sys.argv[4]

spark = SparkSession.builder.getOrCreate()
dbutils = _get_dbutils(spark)

result = {
    "catalog": catalog,
    "schema": schema,
    "volume": volume,
    "volume_path": volume_path,
}

try:
    result["current_user"] = spark.sql("SELECT current_user() AS user").first()["user"]
except Exception as exc:
    result["current_user_error"] = str(exc)

try:
    result["catalogs"] = _rows_to_values(spark.sql(f"SHOW CATALOGS LIKE '{catalog}'").collect())
except Exception as exc:
    result["catalogs_error"] = str(exc)

try:
    result["schemas"] = _rows_to_values(
        spark.sql(f"SHOW SCHEMAS IN `{catalog}` LIKE '{schema}'").collect()
    )
except Exception as exc:
    result["schemas_error"] = str(exc)

try:
    result["volumes"] = _rows_to_values(
        spark.sql(f"SHOW VOLUMES IN `{catalog}`.`{schema}` LIKE '{volume}'").collect()
    )
except Exception as exc:
    result["volumes_error"] = str(exc)

try:
    listing = dbutils.fs.ls(volume_path)
    result["volume_ls_count"] = len(listing)
except Exception as exc:
    result["volume_ls_error"] = str(exc)

ok = (
    catalog in result.get("catalogs", [])
    and schema in result.get("schemas", [])
    and volume in result.get("volumes", [])
    and "volume_ls_error" not in result
)

print("KINDLING_DATABRICKS_PREFLIGHT " + json.dumps(result, sort_keys=True))
if not ok:
    raise RuntimeError("Databricks UC preflight checks failed")
"""


def _require_env(name: str) -> str:
    value = (os.getenv(name) or "").strip()
    if not value:
        raise SystemExit(f"Missing required environment variable: {name}")
    return value


def _workspace_client() -> WorkspaceClient:
    return WorkspaceClient(
        host=_require_env("DATABRICKS_HOST"),
        azure_tenant_id=_require_env("AZURE_TENANT_ID"),
        azure_client_id=_require_env("AZURE_CLIENT_ID"),
        azure_client_secret=_require_env("AZURE_CLIENT_SECRET"),
        auth_type="azure-client-secret",
    )


def _volume_bits() -> tuple[str, str, str, str]:
    catalog = (os.getenv("KINDLING_DATABRICKS_RUNTIME_VOLUME_CATALOG") or "kindling").strip()
    schema = (os.getenv("KINDLING_DATABRICKS_RUNTIME_VOLUME_SCHEMA") or "kindling").strip()
    volume = (os.getenv("KINDLING_DATABRICKS_RUNTIME_TEMP_VOLUME") or "artifacts").strip()
    return catalog, schema, volume, f"/Volumes/{catalog}/{schema}/{volume}"


def _storage_bits() -> tuple[str, str]:
    account = (os.getenv("AZURE_STORAGE_ACCOUNT") or "").strip()
    container = (os.getenv("AZURE_CONTAINER") or "").strip()
    if not account or not container:
        raise SystemExit(
            "Missing required environment variables for ADLS staging: "
            "AZURE_STORAGE_ACCOUNT and AZURE_CONTAINER"
        )
    return account, container


def _upload_remote_script(script_contents: str) -> str:
    storage_account, container = _storage_bits()
    tenant_id = _require_env("AZURE_TENANT_ID")
    client_id = _require_env("AZURE_CLIENT_ID")
    client_secret = _require_env("AZURE_CLIENT_SECRET")
    path = f"scripts/databricks_uc_preflight_{uuid.uuid4().hex}.py"

    credential = ClientSecretCredential(
        tenant_id=tenant_id,
        client_id=client_id,
        client_secret=client_secret,
    )
    account_url = f"https://{storage_account}.dfs.core.windows.net"
    storage_client = DataLakeServiceClient(account_url=account_url, credential=credential)
    file_system = storage_client.get_file_system_client(container)
    file_client = file_system.get_file_client(path)
    file_client.upload_data(io.BytesIO(script_contents.encode("utf-8")), overwrite=True)

    return f"abfss://{container}@{storage_account}.dfs.core.windows.net/{path}"


def _delete_remote_script(script_path: str) -> None:
    try:
        storage_account, container = _storage_bits()
        tenant_id = _require_env("AZURE_TENANT_ID")
        client_id = _require_env("AZURE_CLIENT_ID")
        client_secret = _require_env("AZURE_CLIENT_SECRET")
        credential = ClientSecretCredential(
            tenant_id=tenant_id,
            client_id=client_id,
            client_secret=client_secret,
        )
        account_url = f"https://{storage_account}.dfs.core.windows.net"
        storage_client = DataLakeServiceClient(account_url=account_url, credential=credential)
        file_system = storage_client.get_file_system_client(container)
        prefix = f"abfss://{container}@{storage_account}.dfs.core.windows.net/"
        if script_path.startswith(prefix):
            relative_path = script_path[len(prefix) :]
            file_system.delete_file(relative_path)
    except Exception:
        pass


def _submit_run(client: WorkspaceClient, cluster_id: str, python_file: str, parameters: list[str]) -> int:
    payload = {
        "run_name": "kindling-databricks-uc-preflight",
        "tasks": [
            {
                "task_key": "preflight",
                "existing_cluster_id": cluster_id,
                "spark_python_task": {
                    "python_file": python_file,
                    "parameters": parameters,
                },
            }
        ],
    }
    response = client.api_client.do("POST", "/api/2.1/jobs/runs/submit", body=payload)
    return int(response["run_id"])


def _wait_for_run(client: WorkspaceClient, run_id: int, timeout_seconds: int = 900) -> dict:
    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        run = client.api_client.do("GET", "/api/2.1/jobs/runs/get", query={"run_id": run_id})
        state = run.get("state", {})
        lifecycle = (state.get("life_cycle_state") or "").upper()
        if lifecycle in {
            "TERMINATED",
            "SKIPPED",
            "INTERNAL_ERROR",
            "BLOCKED",
        }:
            return run
        time.sleep(10)
    raise TimeoutError(f"Timed out waiting for Databricks preflight run {run_id}")


def _get_task_run_output(client: WorkspaceClient, run: dict, default_run_id: int) -> dict:
    tasks = run.get("tasks") or []
    task_run_id = tasks[0].get("run_id") if tasks else default_run_id
    return client.api_client.do("GET", "/api/2.1/jobs/runs/get-output", query={"run_id": task_run_id})


def main() -> int:
    cluster_id = _require_env("DATABRICKS_CLUSTER_ID")
    client = _workspace_client()
    catalog, schema, volume, volume_path = _volume_bits()
    remote_script = _upload_remote_script(REMOTE_SCRIPT_TEMPLATE)

    print(
        json.dumps(
            {
                "host": _require_env("DATABRICKS_HOST"),
                "cluster_id": cluster_id,
                "volume_path": volume_path,
                "remote_script": remote_script,
            },
            indent=2,
        )
    )

    try:
        run_id = _submit_run(client, cluster_id, remote_script, [catalog, schema, volume, volume_path])
        print(f"Submitted Databricks UC preflight run: {run_id}")
        run = _wait_for_run(client, run_id)
        state = run.get("state", {})
        print(json.dumps({"run_id": run_id, "state": state}, indent=2))
        output = _get_task_run_output(client, run, run_id)
        logs = output.get("logs") or output.get("error_trace") or ""
        if logs:
            print(logs)
        result_state = (state.get("result_state") or "").upper()
        return 0 if result_state == "SUCCESS" else 1
    finally:
        _delete_remote_script(remote_script)


if __name__ == "__main__":
    sys.exit(main())
