def remove_duplicates(df, keycolumns):
    from pyspark.sql.functions import col, row_number
    from pyspark.sql.window import Window

    window_spec = Window.partitionBy(*keycolumns).orderBy(col("SourceTimestamp").desc())
    df_with_row_num = df.withColumn("row_num", row_number().over(window_spec))
    return df_with_row_num.filter(col("row_num") == 1).drop("row_num")

def drop_if_exists(df, column_name):
    """Drop a column if it exists, otherwise return the original DataFrame."""
    if column_name in df.columns:
        return df.drop(column_name)
    else: 
        return df