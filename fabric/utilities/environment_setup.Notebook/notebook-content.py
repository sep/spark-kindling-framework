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

def get_storage_utils():
    import __main__
    return getattr(__main__, 'mssparkutils', None) or getattr(__main__, 'dbutils', None)

def execute_remote_py_files(abfss_paths, **kwargs):
    """Execute multiple Python files by concatenating them first"""
    stgutil = get_storage_utils()

    combined_content = []
    
    for abfss_path in abfss_paths:
        content = stgutil.fs.head(abfss_path, max_bytes=1000000)
        combined_content.append(content)
    
    full_content = "\n\n".join(combined_content)
    current_globals = globals()
    current_globals.update(kwargs)
    print(f"Spark in globals = {'spark' in current_globals}")
    exec(compile(full_content, "concatenated_files", 'exec'), current_globals)

def is_kindling_available():
    avail = False
    try:
        from kindling.notebook_framework import bootstrap_framework
        avail = True
    except ImportError as e:
        pass
    return avail

def bootstrap_environment():

    if BOOTSTRAP_CONFIG.get('package_storage_path', None) != None:
        BOOTSTRAP_CONFIG['artifacts_storage_path'] = BOOTSTRAP_CONFIG.get('package_storage_path').rsplit('/', 2)[0]
    if BOOTSTRAP_CONFIG.get('platform_environment', None) == None:
        BOOTSTRAP_CONFIG['platform_environment'] = "fabric"
    if BOOTSTRAP_CONFIG.get('workspace_id', None) == None:
        BOOTSTRAP_CONFIG['workspace_id'] = BOOTSTRAP_CONFIG.get('workspace_endpoint', '')

    kindling_available = is_kindling_available()

    if kindling_available == False:
        print("Kindling not available, executing remote bootstap ...")

        execute_remote_py_files([
            f"{BOOTSTRAP_CONFIG['artifacts_storage_path']}/scripts/kindling_bootstrap.py"
        ], bootstrap_config=BOOTSTRAP_CONFIG)

                    
bootstrap_environment()

if is_kindling_available() == True:
    from kindling.notebook_framework import bootstrap_framework
    from kindling.injection import GlobalInjector
    from kindling.spark_log_provider import PythonLoggerProvider
    import types
    bootstrap_framework(BOOTSTRAP_CONFIG, GlobalInjector.get(PythonLoggerProvider).get_logger("bootstrap", session = spark) )
else:
    bootstrap_framework(BOOTSTRAP_CONFIG, create_console_logger(BOOTSTRAP_CONFIG))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
