# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   }
# META }

# CELL ********************

globals()["is_interactive"] = False
globals()["use_lake_packages"] = False
if "frameworkPackageGuard" in globals():
    print("Removing package guard")
    del globals()["frameworkPackageGuard"]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

%run environment_setup

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

publish_notebook_folder_as_package( "kindling", "kindling", "abfss://artifacts@sepstdatalakedev.dfs.core.windows.net/packages/latest" )#

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
