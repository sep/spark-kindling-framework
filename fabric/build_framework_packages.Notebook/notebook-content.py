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
    'required_packages': ['pytest'],
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

%run notebook_framework

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

%run backend_fabric

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

es = FabricService(BOOTSTRAP_CONFIG, create_console_logger(BOOTSTRAP_CONFIG))
nm = NotebookLoader(es, BOOTSTRAP_CONFIG)
nm.publish_notebook_folder_as_package( "kindling", "kindling", "abfss://artifacts@sepstdatalakedev.dfs.core.windows.net/packages/latest", "0.5.0" )#

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
