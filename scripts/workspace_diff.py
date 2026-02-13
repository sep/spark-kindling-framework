#!/usr/bin/env python3
"""
Databricks Workspace Diff Tool
Compares two workspaces to capture all configuration differences.
"""
import json
import os
import subprocess
import sys
from datetime import datetime


def get_token():
    """Get fresh AAD token for Databricks."""
    result = subprocess.run(
        [
            "az",
            "account",
            "get-access-token",
            "--resource",
            "2ff814a6-3304-4ab8-85cb-cd0e6f879c1d",
            "--query",
            "accessToken",
            "-o",
            "tsv",
        ],
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        print(f"ERROR getting token: {result.stderr}")
        sys.exit(1)
    return result.stdout.strip()


def api_call(host, endpoint, token, method="GET", data=None):
    """Make Databricks REST API call."""
    import urllib.error
    import urllib.request

    url = f"{host}{endpoint}"
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

    req = urllib.request.Request(url, headers=headers, method=method)
    if data:
        req.data = json.dumps(data).encode()

    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            return json.loads(resp.read().decode())
    except urllib.error.HTTPError as e:
        body = e.read().decode() if e.fp else ""
        return {"_error": f"HTTP {e.code}", "_body": body, "_endpoint": endpoint}
    except Exception as e:
        return {"_error": str(e), "_endpoint": endpoint}


def query_workspace(host, token, label):
    """Query all configuration from a workspace."""
    print(f"\n{'='*60}")
    print(f"Querying: {label} ({host})")
    print(f"{'='*60}")

    results = {}

    # 1. Clusters
    print("  -> Clusters...")
    results["clusters"] = api_call(host, "/api/2.0/clusters/list", token)

    # 2. Cluster Policies
    print("  -> Cluster Policies...")
    results["cluster_policies"] = api_call(host, "/api/2.0/policies/clusters/list", token)

    # 3. Jobs
    print("  -> Jobs...")
    results["jobs"] = api_call(host, "/api/2.1/jobs/list?limit=100", token)

    # 4. Instance Pools
    print("  -> Instance Pools...")
    results["instance_pools"] = api_call(host, "/api/2.0/instance-pools/list", token)

    # 5. Workspace files (top-level)
    print("  -> Workspace root contents...")
    results["workspace_root"] = api_call(host, "/api/2.0/workspace/list", token, "GET")
    # Try with path param
    results["workspace_root"] = api_call(host, "/api/2.0/workspace/list?path=/", token)

    # 6. Workspace /Users
    print("  -> Workspace /Users...")
    results["workspace_users_dir"] = api_call(host, "/api/2.0/workspace/list?path=/Users", token)

    # 7. Workspace /Shared
    print("  -> Workspace /Shared...")
    results["workspace_shared"] = api_call(host, "/api/2.0/workspace/list?path=/Shared", token)

    # 8. Workspace /Repos
    print("  -> Workspace /Repos...")
    results["workspace_repos"] = api_call(host, "/api/2.0/workspace/list?path=/Repos", token)

    # 9. Repos (Git)
    print("  -> Git Repos...")
    results["repos"] = api_call(host, "/api/2.0/repos?next_page_token=", token)

    # 10. Secret Scopes
    print("  -> Secret Scopes...")
    results["secret_scopes"] = api_call(host, "/api/2.0/secrets/scopes/list", token)

    # 11. If scopes exist, list secrets in each scope
    scopes = results.get("secret_scopes", {}).get("scopes", [])
    results["secrets_by_scope"] = {}
    for scope in scopes:
        scope_name = scope.get("name", "")
        print(f"    -> Secrets in scope '{scope_name}'...")
        results["secrets_by_scope"][scope_name] = api_call(
            host, f"/api/2.0/secrets/list?scope={scope_name}", token
        )

    # 12. Token management (personal access tokens)
    print("  -> Tokens...")
    results["tokens"] = api_call(host, "/api/2.0/token/list", token)

    # 13. SQL Warehouses
    print("  -> SQL Warehouses...")
    results["sql_warehouses"] = api_call(host, "/api/2.0/sql/warehouses", token)

    # 14. Unity Catalog - Catalogs
    print("  -> UC Catalogs...")
    results["uc_catalogs"] = api_call(host, "/api/2.1/unity-catalog/catalogs", token)

    # 15. Unity Catalog - External Locations
    print("  -> UC External Locations...")
    results["uc_external_locations"] = api_call(
        host, "/api/2.1/unity-catalog/external-locations", token
    )

    # 16. Unity Catalog - Storage Credentials
    print("  -> UC Storage Credentials...")
    results["uc_storage_credentials"] = api_call(
        host, "/api/2.1/unity-catalog/storage-credentials", token
    )

    # 17. Unity Catalog - Schemas (try for each catalog)
    catalogs = results.get("uc_catalogs", {}).get("catalogs", [])
    results["uc_schemas_by_catalog"] = {}
    for cat in catalogs:
        cat_name = cat.get("name", "")
        print(f"    -> Schemas in catalog '{cat_name}'...")
        results["uc_schemas_by_catalog"][cat_name] = api_call(
            host, f"/api/2.1/unity-catalog/schemas?catalog_name={cat_name}", token
        )

    # 18. Unity Catalog - Volumes (per schema)
    results["uc_volumes_by_schema"] = {}
    for cat_name, schema_data in results.get("uc_schemas_by_catalog", {}).items():
        schemas = schema_data.get("schemas", [])
        for schema in schemas:
            schema_name = schema.get("name", "")
            full_name = f"{cat_name}.{schema_name}"
            print(f"    -> Volumes in '{full_name}'...")
            results["uc_volumes_by_schema"][full_name] = api_call(
                host,
                f"/api/2.1/unity-catalog/volumes?catalog_name={cat_name}&schema_name={schema_name}",
                token,
            )

    # 19. Global Init Scripts
    print("  -> Global Init Scripts...")
    results["global_init_scripts"] = api_call(host, "/api/2.0/global-init-scripts", token)

    # 20. DBFS root listing
    print("  -> DBFS root...")
    results["dbfs_root"] = api_call(host, "/api/2.0/dbfs/list?path=/", token)

    # 21. DBFS /FileStore
    print("  -> DBFS /FileStore...")
    results["dbfs_filestore"] = api_call(host, "/api/2.0/dbfs/list?path=/FileStore", token)

    # 22. Workspace settings / admin config
    print("  -> Workspace config...")
    results["workspace_conf"] = api_call(
        host,
        "/api/2.0/workspace-conf?keys=enableDcs,enableProjectTypeInWorkspace,enableDbfsFileBrowser,enableExportNotebook,maxTokenLifetimeDays,enableTokensConfig,enableDeprecatedGlobalInitScripts,enableIpAccessLists",
        token,
    )

    # 23. IP Access Lists
    print("  -> IP Access Lists...")
    results["ip_access_lists"] = api_call(host, "/api/2.0/ip-access-lists", token)

    # 24. Service Principals
    print("  -> Service Principals...")
    results["service_principals"] = api_call(
        host, "/api/2.0/preview/scim/v2/ServicePrincipals", token
    )

    # 25. Groups
    print("  -> Groups...")
    results["groups"] = api_call(host, "/api/2.0/preview/scim/v2/Groups?count=100", token)

    # 26. Libraries on clusters
    print("  -> Cluster Libraries...")
    results["cluster_libraries"] = api_call(host, "/api/2.0/libraries/all-cluster-statuses", token)

    # 27. Cluster details (for each cluster)
    clusters = results.get("clusters", {}).get("clusters", [])
    results["cluster_details"] = {}
    for c in clusters:
        cid = c.get("cluster_id", "")
        cname = c.get("cluster_name", "")
        print(f"    -> Cluster detail: '{cname}'...")
        results["cluster_details"][cid] = api_call(
            host, f"/api/2.0/clusters/get?cluster_id={cid}", token
        )

    # 28. Permissions on clusters
    results["cluster_permissions"] = {}
    for c in clusters:
        cid = c.get("cluster_id", "")
        cname = c.get("cluster_name", "")
        print(f"    -> Cluster permissions: '{cname}'...")
        results["cluster_permissions"][cid] = api_call(
            host, f"/api/2.0/permissions/clusters/{cid}", token
        )

    # 29. Delta Live Tables Pipelines
    print("  -> DLT Pipelines...")
    results["dlt_pipelines"] = api_call(host, "/api/2.0/pipelines", token)

    # 30. Model Registry
    print("  -> MLflow Registered Models...")
    results["mlflow_models"] = api_call(host, "/api/2.0/mlflow/registered-models/list", token)

    # 31. Workspace /Workspace folder (newer DBX)
    print("  -> Workspace /Workspace dir...")
    results["workspace_workspace_dir"] = api_call(
        host, "/api/2.0/workspace/list?path=/Workspace", token
    )

    print(f"  Done querying {label}.")
    return results


def summarize_clusters(data):
    """Summarize cluster configuration."""
    clusters = data.get("clusters", {}).get("clusters", [])
    if not clusters:
        return ["No clusters found"]

    lines = []
    for c in clusters:
        lines.append(f"\n  ### Cluster: {c.get('cluster_name', 'unnamed')}")
        lines.append(f"  - **Cluster ID**: {c.get('cluster_id', 'N/A')}")
        lines.append(f"  - **State**: {c.get('state', 'N/A')}")
        lines.append(f"  - **Spark Version**: {c.get('spark_version', 'N/A')}")
        lines.append(f"  - **Node Type**: {c.get('node_type_id', 'N/A')}")
        lines.append(
            f"  - **Driver Node Type**: {c.get('driver_node_type_id', c.get('node_type_id', 'N/A'))}"
        )
        lines.append(f"  - **Autoscale**: {c.get('autoscale', 'N/A')}")
        lines.append(f"  - **Num Workers**: {c.get('num_workers', 'N/A')}")
        lines.append(f"  - **Auto-termination (min)**: {c.get('autotermination_minutes', 'N/A')}")
        lines.append(f"  - **Data Security Mode**: {c.get('data_security_mode', 'N/A')}")
        lines.append(f"  - **Runtime Engine**: {c.get('runtime_engine', 'N/A')}")
        lines.append(f"  - **Single User Name**: {c.get('single_user_name', 'N/A')}")

        # Spark conf
        spark_conf = c.get("spark_conf", {})
        if spark_conf:
            lines.append(f"  - **Spark Config**:")
            for k, v in sorted(spark_conf.items()):
                lines.append(f"    - `{k}`: `{v}`")

        # Spark env vars
        spark_env = c.get("spark_env_vars", {})
        if spark_env:
            lines.append(f"  - **Spark Env Vars**:")
            for k, v in sorted(spark_env.items()):
                lines.append(f"    - `{k}`: `{v}`")

        # Custom tags
        tags = c.get("custom_tags", {})
        if tags:
            lines.append(f"  - **Custom Tags**:")
            for k, v in sorted(tags.items()):
                lines.append(f"    - `{k}`: `{v}`")

        # Init scripts
        init_scripts = c.get("init_scripts", [])
        if init_scripts:
            lines.append(f"  - **Init Scripts**:")
            for s in init_scripts:
                lines.append(f"    - {json.dumps(s)}")

        # Libraries
        libraries = c.get("libraries", [])
        if libraries:
            lines.append(f"  - **Libraries**:")
            for lib in libraries:
                lines.append(f"    - {json.dumps(lib)}")

        # Cluster log conf
        log_conf = c.get("cluster_log_conf", {})
        if log_conf:
            lines.append(f"  - **Cluster Log Config**: {json.dumps(log_conf)}")

        # Policy
        if c.get("policy_id"):
            lines.append(f"  - **Policy ID**: {c.get('policy_id')}")

        # Azure attributes
        azure_attrs = c.get("azure_attributes", {})
        if azure_attrs:
            lines.append(f"  - **Azure Attributes**: {json.dumps(azure_attrs, indent=4)}")

    return lines


def summarize_jobs(data):
    """Summarize jobs."""
    jobs = data.get("jobs", {}).get("jobs", [])
    if not jobs:
        return ["No jobs found"]

    lines = []
    for j in jobs:
        settings = j.get("settings", {})
        lines.append(f"\n  ### Job: {settings.get('name', 'unnamed')}")
        lines.append(f"  - **Job ID**: {j.get('job_id', 'N/A')}")
        lines.append(f"  - **Creator**: {j.get('creator_user_name', 'N/A')}")

        # Schedule
        schedule = settings.get("schedule", {})
        if schedule:
            lines.append(
                f"  - **Schedule**: `{schedule.get('quartz_cron_expression', 'N/A')}` (tz: {schedule.get('timezone_id', 'N/A')})"
            )

        # Tasks
        tasks = settings.get("tasks", [])
        if tasks:
            lines.append(f"  - **Tasks** ({len(tasks)}):")
            for t in tasks:
                task_key = t.get("task_key", "N/A")
                lines.append(f"    - `{task_key}`")
                if t.get("spark_python_task"):
                    lines.append(
                        f"      - Python: `{t['spark_python_task'].get('python_file', '')}`"
                    )
                    params = t["spark_python_task"].get("parameters", [])
                    if params:
                        lines.append(f"      - Params: {params}")
                if t.get("notebook_task"):
                    lines.append(
                        f"      - Notebook: `{t['notebook_task'].get('notebook_path', '')}`"
                    )
                if t.get("existing_cluster_id"):
                    lines.append(f"      - Cluster: `{t['existing_cluster_id']}`")
                if t.get("new_cluster"):
                    nc = t["new_cluster"]
                    lines.append(
                        f"      - New Cluster: spark={nc.get('spark_version')}, node={nc.get('node_type_id')}, workers={nc.get('num_workers')}"
                    )
                if t.get("libraries"):
                    lines.append(f"      - Libraries: {json.dumps(t['libraries'])}")

        # Email notifications
        notifs = settings.get("email_notifications", {})
        if notifs and any(notifs.values()):
            lines.append(f"  - **Email Notifications**: {json.dumps(notifs)}")

        # Timeout
        if settings.get("timeout_seconds"):
            lines.append(f"  - **Timeout**: {settings['timeout_seconds']}s")

        # Max concurrent runs
        lines.append(f"  - **Max Concurrent Runs**: {settings.get('max_concurrent_runs', 'N/A')}")

    return lines


def summarize_secrets(data):
    """Summarize secret scopes and keys."""
    scopes = data.get("secret_scopes", {}).get("scopes", [])
    secrets_by_scope = data.get("secrets_by_scope", {})

    if not scopes:
        return ["No secret scopes found"]

    lines = []
    for scope in scopes:
        name = scope.get("name", "unnamed")
        backend = scope.get("backend_type", "N/A")
        lines.append(f"\n  ### Scope: {name} (backend: {backend})")

        secrets = secrets_by_scope.get(name, {}).get("secrets", [])
        if secrets:
            for s in secrets:
                lines.append(
                    f"  - Key: `{s.get('key', 'N/A')}` (last updated: {s.get('last_updated_timestamp', 'N/A')})"
                )
        else:
            lines.append(f"  - (no secrets listed)")

    return lines


def summarize_uc(data):
    """Summarize Unity Catalog resources."""
    lines = []

    # Catalogs
    catalogs = data.get("uc_catalogs", {}).get("catalogs", [])
    lines.append(f"\n  ### Catalogs ({len(catalogs)})")
    for cat in catalogs:
        lines.append(
            f"  - **{cat.get('name')}** (owner: {cat.get('owner', 'N/A')}, type: {cat.get('catalog_type', 'N/A')})"
        )
        if cat.get("comment"):
            lines.append(f"    - Comment: {cat['comment']}")
        if cat.get("storage_root"):
            lines.append(f"    - Storage Root: `{cat['storage_root']}`")

    # Schemas
    schemas_by_cat = data.get("uc_schemas_by_catalog", {})
    for cat_name, schema_data in sorted(schemas_by_cat.items()):
        schemas = schema_data.get("schemas", [])
        if schemas:
            lines.append(f"\n  ### Schemas in `{cat_name}` ({len(schemas)})")
            for s in schemas:
                lines.append(f"  - **{s.get('name')}** (owner: {s.get('owner', 'N/A')})")
                if s.get("comment"):
                    lines.append(f"    - Comment: {s['comment']}")
                if s.get("storage_root"):
                    lines.append(f"    - Storage Root: `{s['storage_root']}`")

    # Volumes
    volumes_by_schema = data.get("uc_volumes_by_schema", {})
    has_volumes = False
    for schema_name, vol_data in sorted(volumes_by_schema.items()):
        volumes = vol_data.get("volumes", [])
        if volumes:
            has_volumes = True
            lines.append(f"\n  ### Volumes in `{schema_name}` ({len(volumes)})")
            for v in volumes:
                lines.append(f"  - **{v.get('name')}** (type: {v.get('volume_type', 'N/A')})")
                if v.get("storage_location"):
                    lines.append(f"    - Location: `{v['storage_location']}`")
    if not has_volumes:
        lines.append(f"\n  ### Volumes: None found")

    # External Locations
    ext_locs = data.get("uc_external_locations", {}).get("external_locations", [])
    lines.append(f"\n  ### External Locations ({len(ext_locs)})")
    for loc in ext_locs:
        lines.append(f"  - **{loc.get('name')}**: `{loc.get('url', 'N/A')}`")
        lines.append(f"    - Credential: `{loc.get('credential_name', 'N/A')}`")
        lines.append(f"    - Owner: {loc.get('owner', 'N/A')}")

    # Storage Credentials
    creds = data.get("uc_storage_credentials", {}).get("storage_credentials", [])
    lines.append(f"\n  ### Storage Credentials ({len(creds)})")
    for cred in creds:
        lines.append(f"  - **{cred.get('name')}** (owner: {cred.get('owner', 'N/A')})")
        if cred.get("azure_managed_identity"):
            ami = cred["azure_managed_identity"]
            lines.append(f"    - Managed Identity: `{ami.get('access_connector_id', 'N/A')}`")
        if cred.get("azure_service_principal"):
            sp = cred["azure_service_principal"]
            lines.append(
                f"    - Service Principal: directory={sp.get('directory_id')}, app={sp.get('application_id')}"
            )

    return lines


def summarize_workspace_contents(data):
    """Summarize workspace directory contents."""
    lines = []

    for key, label in [
        ("workspace_root", "/"),
        ("workspace_users_dir", "/Users"),
        ("workspace_shared", "/Shared"),
        ("workspace_repos", "/Repos"),
        ("workspace_workspace_dir", "/Workspace"),
    ]:
        items = data.get(key, {}).get("objects", [])
        if items:
            lines.append(f"\n  ### Directory: {label} ({len(items)} items)")
            for item in items:
                obj_type = item.get("object_type", "N/A")
                path = item.get("path", "N/A")
                lines.append(f"  - [{obj_type}] `{path}`")
        elif "_error" in data.get(key, {}):
            lines.append(f"\n  ### Directory: {label} - Error: {data[key]['_error']}")

    return lines


def summarize_groups_and_sps(data):
    """Summarize groups and service principals."""
    lines = []

    # Groups
    groups = data.get("groups", {}).get("Resources", [])
    lines.append(f"\n  ### Groups ({len(groups)})")
    for g in groups:
        display = g.get("displayName", "unnamed")
        members = g.get("members", [])
        lines.append(f"  - **{display}** ({len(members)} members)")
        for m in members[:10]:
            lines.append(f"    - {m.get('display', m.get('value', 'N/A'))}")
        if len(members) > 10:
            lines.append(f"    - ... and {len(members) - 10} more")

    # Service Principals
    sps = data.get("service_principals", {}).get("Resources", [])
    lines.append(f"\n  ### Service Principals ({len(sps)})")
    for sp in sps:
        lines.append(
            f"  - **{sp.get('displayName', 'unnamed')}** (appId: {sp.get('applicationId', 'N/A')}, active: {sp.get('active', 'N/A')})"
        )

    return lines


def summarize_libraries(data):
    """Summarize installed libraries across clusters."""
    statuses = data.get("cluster_libraries", {}).get("statuses", [])
    if not statuses:
        return ["No cluster library statuses found"]

    lines = []
    for status in statuses:
        cid = status.get("cluster_id", "N/A")
        libs = status.get("library_statuses", [])
        lines.append(f"\n  ### Cluster: {cid} ({len(libs)} libraries)")
        for lib in libs:
            lib_info = lib.get("library", {})
            lib_status = lib.get("status", "N/A")
            if lib_info.get("pypi"):
                lines.append(f"  - PyPI: `{lib_info['pypi'].get('package', 'N/A')}` [{lib_status}]")
            elif lib_info.get("jar"):
                lines.append(f"  - JAR: `{lib_info['jar']}` [{lib_status}]")
            elif lib_info.get("whl"):
                lines.append(f"  - Wheel: `{lib_info['whl']}` [{lib_status}]")
            elif lib_info.get("maven"):
                lines.append(
                    f"  - Maven: `{lib_info['maven'].get('coordinates', 'N/A')}` [{lib_status}]"
                )
            else:
                lines.append(f"  - {json.dumps(lib_info)} [{lib_status}]")

    return lines


def summarize_misc(data):
    """Summarize miscellaneous config."""
    lines = []

    # Instance Pools
    pools = data.get("instance_pools", {}).get("instance_pools", [])
    lines.append(f"\n  ### Instance Pools ({len(pools)})")
    for p in pools:
        lines.append(
            f"  - **{p.get('instance_pool_name', 'unnamed')}** (node: {p.get('node_type_id')}, min: {p.get('min_idle_instances', 0)}, max: {p.get('max_capacity', 'N/A')})"
        )

    # SQL Warehouses
    warehouses = data.get("sql_warehouses", {}).get("warehouses", [])
    lines.append(f"\n  ### SQL Warehouses ({len(warehouses)})")
    for w in warehouses:
        lines.append(
            f"  - **{w.get('name', 'unnamed')}** (size: {w.get('cluster_size', 'N/A')}, state: {w.get('state', 'N/A')}, type: {w.get('warehouse_type', 'N/A')})"
        )
        if w.get("spot_instance_policy"):
            lines.append(f"    - Spot Policy: {w['spot_instance_policy']}")
        if w.get("channel"):
            lines.append(f"    - Channel: {w['channel'].get('name', 'N/A')}")

    # Global init scripts
    scripts = data.get("global_init_scripts", {}).get("scripts", [])
    lines.append(f"\n  ### Global Init Scripts ({len(scripts)})")
    for s in scripts:
        lines.append(
            f"  - **{s.get('name', 'unnamed')}** (enabled: {s.get('enabled', 'N/A')}, position: {s.get('position', 'N/A')})"
        )

    # DLT Pipelines
    pipelines = data.get("dlt_pipelines", {}).get("statuses", [])
    lines.append(f"\n  ### DLT Pipelines ({len(pipelines)})")
    for p in pipelines:
        lines.append(
            f"  - **{p.get('name', 'unnamed')}** (state: {p.get('state', 'N/A')}, id: {p.get('pipeline_id', 'N/A')})"
        )

    # MLflow models
    models = data.get("mlflow_models", {}).get("registered_models", [])
    lines.append(f"\n  ### MLflow Registered Models ({len(models)})")
    for m in models:
        lines.append(f"  - **{m.get('name', 'unnamed')}**")

    # Workspace conf
    conf = data.get("workspace_conf", {})
    if conf and "_error" not in conf:
        lines.append(f"\n  ### Workspace Configuration")
        for k, v in sorted(conf.items()):
            lines.append(f"  - `{k}`: `{v}`")

    # IP Access Lists
    ip_lists = data.get("ip_access_lists", {}).get("ip_access_lists", [])
    lines.append(f"\n  ### IP Access Lists ({len(ip_lists)})")
    for ipl in ip_lists:
        lines.append(
            f"  - **{ipl.get('label', 'unnamed')}** (type: {ipl.get('list_type', 'N/A')}, enabled: {ipl.get('enabled', 'N/A')})"
        )
        for addr in ipl.get("ip_addresses", [])[:5]:
            lines.append(f"    - {addr}")

    # DBFS
    dbfs_root = data.get("dbfs_root", {}).get("files", [])
    lines.append(f"\n  ### DBFS Root ({len(dbfs_root)} items)")
    for f in dbfs_root:
        lines.append(
            f"  - {'[DIR]' if f.get('is_dir') else '[FILE]'} `{f.get('path', 'N/A')}` ({f.get('file_size', 0)} bytes)"
        )

    dbfs_filestore = data.get("dbfs_filestore", {}).get("files", [])
    if dbfs_filestore:
        lines.append(f"\n  ### DBFS /FileStore ({len(dbfs_filestore)} items)")
        for f in dbfs_filestore:
            lines.append(f"  - {'[DIR]' if f.get('is_dir') else '[FILE]'} `{f.get('path', 'N/A')}`")

    # Cluster permissions
    perms = data.get("cluster_permissions", {})
    if perms:
        lines.append(f"\n  ### Cluster Permissions")
        for cid, perm_data in perms.items():
            acls = perm_data.get("access_control_list", [])
            if acls:
                cname = data.get("cluster_details", {}).get(cid, {}).get("cluster_name", cid)
                lines.append(f"\n  #### Cluster: {cname}")
                for acl in acls:
                    principal = (
                        acl.get("user_name")
                        or acl.get("group_name")
                        or acl.get("service_principal_name")
                        or "unknown"
                    )
                    all_perms = acl.get("all_permissions", [])
                    for p in all_perms:
                        lines.append(
                            f"  - {principal}: {p.get('permission_level', 'N/A')} (inherited: {p.get('inherited', False)})"
                        )

    return lines


def generate_report(old_data, new_data, old_host, new_host):
    """Generate the diff report."""
    report = []
    report.append(f"# Databricks Workspace Diff Report")
    report.append(f"")
    report.append(f"**Generated**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S UTC')}")
    report.append(f"")
    report.append(f"**Original (Dev)**: {old_host}")
    report.append(f"**New (Target)**: {new_host}")
    report.append(f"")
    report.append(f"---")
    report.append(f"")

    # ============================================================
    report.append(f"## 1. Clusters")
    report.append(f"")
    report.append(f"### Original Workspace")
    report.extend(summarize_clusters(old_data))
    report.append(f"")
    report.append(f"### New Workspace")
    report.extend(summarize_clusters(new_data))
    report.append(f"")

    # ============================================================
    report.append(f"---")
    report.append(f"## 2. Jobs")
    report.append(f"")
    report.append(f"### Original Workspace")
    report.extend(summarize_jobs(old_data))
    report.append(f"")
    report.append(f"### New Workspace")
    report.extend(summarize_jobs(new_data))
    report.append(f"")

    # ============================================================
    report.append(f"---")
    report.append(f"## 3. Secret Scopes & Secrets")
    report.append(f"")
    report.append(f"### Original Workspace")
    report.extend(summarize_secrets(old_data))
    report.append(f"")
    report.append(f"### New Workspace")
    report.extend(summarize_secrets(new_data))
    report.append(f"")

    # ============================================================
    report.append(f"---")
    report.append(f"## 4. Unity Catalog")
    report.append(f"")
    report.append(f"### Original Workspace")
    report.extend(summarize_uc(old_data))
    report.append(f"")
    report.append(f"### New Workspace")
    report.extend(summarize_uc(new_data))
    report.append(f"")

    # ============================================================
    report.append(f"---")
    report.append(f"## 5. Workspace Contents")
    report.append(f"")
    report.append(f"### Original Workspace")
    report.extend(summarize_workspace_contents(old_data))
    report.append(f"")
    report.append(f"### New Workspace")
    report.extend(summarize_workspace_contents(new_data))
    report.append(f"")

    # ============================================================
    report.append(f"---")
    report.append(f"## 6. Groups & Service Principals")
    report.append(f"")
    report.append(f"### Original Workspace")
    report.extend(summarize_groups_and_sps(old_data))
    report.append(f"")
    report.append(f"### New Workspace")
    report.extend(summarize_groups_and_sps(new_data))
    report.append(f"")

    # ============================================================
    report.append(f"---")
    report.append(f"## 7. Installed Libraries")
    report.append(f"")
    report.append(f"### Original Workspace")
    report.extend(summarize_libraries(old_data))
    report.append(f"")
    report.append(f"### New Workspace")
    report.extend(summarize_libraries(new_data))
    report.append(f"")

    # ============================================================
    report.append(f"---")
    report.append(f"## 8. Other Resources")
    report.append(f"")
    report.append(f"### Original Workspace")
    report.extend(summarize_misc(old_data))
    report.append(f"")
    report.append(f"### New Workspace")
    report.extend(summarize_misc(new_data))
    report.append(f"")

    # ============================================================
    # Git Repos
    report.append(f"---")
    report.append(f"## 9. Git Repos")
    report.append(f"")
    for label, data in [("Original", old_data), ("New", new_data)]:
        repos = data.get("repos", {}).get("repos", [])
        report.append(f"### {label} Workspace ({len(repos)} repos)")
        for r in repos:
            report.append(
                f"  - **{r.get('path', 'N/A')}** → `{r.get('url', 'N/A')}` (branch: {r.get('branch', 'N/A')})"
            )
        if not repos:
            report.append(f"  No repos found")
        report.append(f"")

    return "\n".join(report)


def main():
    print("Getting fresh AAD token...")
    token = get_token()

    old_host = "https://adb-3961952571126106.6.azuredatabricks.net"
    new_host = "https://adb-7405613692056733.13.azuredatabricks.net"

    old_data = query_workspace(old_host, token, "ORIGINAL (Dev)")

    # Refresh token in case the first round took a while
    print("\nRefreshing token for new workspace...")
    token = get_token()

    new_data = query_workspace(new_host, token, "NEW (Target)")

    # Save raw data
    with open("/workspace/workspace_diff_raw.json", "w") as f:
        json.dump({"original": old_data, "new": new_data}, f, indent=2, default=str)
    print(f"\nRaw data saved to /workspace/workspace_diff_raw.json")

    # Generate report
    report = generate_report(old_data, new_data, old_host, new_host)

    with open("/workspace/workspace_diff_report.md", "w") as f:
        f.write(report)

    print(f"Report saved to /workspace/workspace_diff_report.md")
    print(f"\nDone!")


if __name__ == "__main__":
    main()
