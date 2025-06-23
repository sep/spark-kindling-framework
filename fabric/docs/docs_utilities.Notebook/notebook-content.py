# Fabric notebook source


# MARKDOWN ********************

# # Common Transforms
# 
# The Common Transforms module provides a set of reusable transformation functions to standardize common data manipulation operations across your Spark pipelines.
# 
# ## Available Transforms
# 
# ### remove_duplicates
# 
# Removes duplicate rows from a DataFrame based on specified key columns, keeping only the most recent version according to the `SourceTimestamp` field.
# 
# ```python
# def remove_duplicates(df, keycolumns):
#     """
#     Remove duplicate rows from a DataFrame based on key columns,
#     keeping only the most recent version by SourceTimestamp.
#     
#     Args:
#         df (DataFrame): The source DataFrame
#         keycolumns (list): List of column names to use as the unique key
#         
#     Returns:
#         DataFrame: DataFrame with duplicates removed
#     """
# ```
# 
# #### Example
# 
# ```python
# from kindling.common_transforms import remove_duplicates
# 
# # Keep only the latest customer record per customer_id
# customer_df = remove_duplicates(customer_df, ["customer_id"])
# ```

# MARKDOWN ********************

# ### drop_if_exists
# 
# Drops a column from a DataFrame if it exists, otherwise returns the original DataFrame unchanged.
# 
# ```python
# def drop_if_exists(df, column_name):
#     """
#     Drop a column if it exists, otherwise return the original DataFrame.
#     
#     Args:
#         df (DataFrame): The source DataFrame
#         column_name (str): Name of the column to drop if it exists
#         
#     Returns:
#         DataFrame: DataFrame with the column dropped if it existed
#     """
# ```
# 
# #### Example
# 
# ```python
# from kindling.common_transforms import drop_if_exists
# 
# # Safely remove a column that might not exist in all source files
# clean_df = drop_if_exists(raw_df, "temporary_column")
# ```
# 
# ## Best Practices
# 
# 1. **Standardize Transformations**: Use these common transforms across your codebase to ensure consistent data handling.
# 
# 2. **Extend with Care**: When adding new common transforms, ensure they are generalized and reusable across multiple use cases.
# 
# 3. **Performance Considerations**: The `remove_duplicates` function uses window functions, which can be expensive on large datasets. Consider partitioning your data appropriately when using this function.
# 
# 4. **Testing**: All common transforms should have comprehensive unit tests to verify correct behavior across edge cases.

# MARKDOWN ********************

# ## Adding New Transforms
# 
# To add new transforms to this module, follow these guidelines:
# 
# 1. Create a function with clear, descriptive name
# 2. Add proper docstrings explaining parameters and return values
# 3. Implement error handling for common edge cases
# 4. Write unit tests covering normal usage and edge cases
# 5. Document the new function in this guide
