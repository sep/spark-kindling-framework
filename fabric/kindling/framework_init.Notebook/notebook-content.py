# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   }
# META }

# CELL ********************

NotebookPackages.register(
    name = "kindling",
    dependencies = ["injector ~= 0.22.0","dynaconf ~= 3.2.11", "pytest ~= 7.4.0"],
    tags = {}
)  

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
