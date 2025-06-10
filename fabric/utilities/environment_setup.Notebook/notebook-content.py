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

def execute_remote_py_file(abfss_path):
    """Execute a Python file directly from ABFSS"""
    content = mssparkutils.fs.head(abfss_path, max_bytes=1000000)  # Adjust size as needed
    exec(compile(content, abfss_path, 'exec'), globals())

def boostrap_environment(path):
    execute_remote_py_file(f"{path}/kindling-bootstrap.py")   


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
