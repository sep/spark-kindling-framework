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
    'is_interactive': False,
    'use_lake_packages' : False,
    'load_local_packages' : False,
    'workspace_id': "059d44a0-c01e-4491-beed-b528c9eca9e8",
    'platform_environment': 'fabric',
    'artifacts_storage_path': "Files/artifacts",
    'required_packages': [],
    'ignored_folders': ["test-package-one", "test-package-two", "test-package-three"],
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

framework.notebook_loader.publish_notebook_folder_as_package( "kindling", "kindling", "abfss://artifacts@sepstdatalakedev.dfs.core.windows.net/packages/", "0.27.0" )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
