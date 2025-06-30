# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "e2a89496-5a18-4104-ac7a-4bfe4f325065",
# META       "default_lakehouse_name": "ent_datalake_np",
# META       "default_lakehouse_workspace_id": "ab18d43b-50de-4b41-b44b-f513a6731b99",
# META       "known_lakehouses": [
# META         {
# META           "id": "e2a89496-5a18-4104-ac7a-4bfe4f325065"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

BOOTSTRAP_CONFIG = {
    'log_level': 'INFO',
    'is_interactive': True,
    'use_lake_packages' : False,
    'load_local_packages' : False,
    'workspace_endpoint': "059d44a0-c01e-4491-beed-b528c9eca9e8",
    'workspace_id': "059d44a0-c01e-4491-beed-b528c9eca9e8",    
    'platform_environment': 'fabric',
    'artifacts_storage_path': "Files/artifacts",
    'required_packages': ["injector", "dynaconf", "pytest"],
    'ignored_folders': [],
    'spark_configs': {
        'spark.databricks.delta.schema.autoMerge.enabled': 'true'
    }
}

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

%run environment_bootstrap

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

%run test_framework

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

test_env = setup_global_test_environment()
results = run_tests_in_folder('kindling-tests')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

max_key_length = max(len(key) for key in results.keys())
max_result_length = max(len(f"[{suite['passed']}/{suite['passed'] + suite['failed']}]") for suite in results.values())
total_width = max_key_length + 2 + max_result_length  # +2 for ": "

for key, suite in results.items():
    passed = suite['passed']
    failed = suite['failed']
    total = passed + failed
    marker = "" if failed == 0 else " *"
    result = f"[{passed}/{total}]"
    print(f"{key}: {result:>{total_width - len(key) - 2}}{marker}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
