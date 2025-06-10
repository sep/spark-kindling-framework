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

def execute_remote_py_file(abfss_path, **kwargs):
    """Execute a Python file directly from ABFSS with parameters"""
    content = mssparkutils.fs.head(abfss_path, max_bytes=1000000)
    current_globals = globals()
    current_globals.update(kwargs)
    exec(compile(content, abfss_path, 'exec'), current_globals)

def bootstrap_environment():
    execute_remote_py_file(f"{BOOTSTRAP_CONFIG['package_storage_path']}/kindling-bootstrap.py", bootstrap_config=BOOTSTRAP_CONFIG)   

bootstrap_environment()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
