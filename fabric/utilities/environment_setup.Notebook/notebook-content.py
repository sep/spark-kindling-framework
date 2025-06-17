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

def execute_remote_py_files(abfss_paths, **kwargs):
    """Execute multiple Python files by concatenating them first"""
    combined_content = []
    
    for abfss_path in abfss_paths:
        content = mssparkutils.fs.head(abfss_path, max_bytes=1000000)
        combined_content.append(content)
    
    full_content = "\n\n".join(combined_content)
    current_globals = globals()
    current_globals.update(kwargs)
    exec(compile(full_content, "concatenated_files", 'exec'), current_globals)

def bootstrap_environment():

    if BOOTSTRAP_CONFIG.get('package_storage_path', None) != None:
        BOOTSTRAP_CONFIG['artifacts_storage_path'] = BOOTSTRAP_CONFIG.get('package_storage_path').rsplit('/', 2)[0]

    execute_remote_py_files([
        f"{BOOTSTRAP_CONFIG['artifacts_storage_path']}/scripts/bootstrap_base.py",
        f"{BOOTSTRAP_CONFIG['artifacts_storage_path']}/scripts/bootstrap_fabric.py",   
        f"{BOOTSTRAP_CONFIG['artifacts_storage_path']}/scripts/bootstrap_kindling.py"
    ], bootstrap_config=BOOTSTRAP_CONFIG)
                          
bootstrap_environment()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
