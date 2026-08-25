def remove_duplicates(df, keycolumns):
    """Keep the latest row per key, by ``SourceTimestamp``.

    For append-only/keyless entities (``keycolumns=[]``) there is no
    business key to deduplicate by -- ``Window.partitionBy()`` with no
    columns collapses to a single global partition, so ranking would
    silently keep only one arbitrary row from the entire dataset (and
    emit Spark's "No Partition Defined for Window operation" warning) --
    so no window/row-number ranking is applied at all when ``keycolumns``
    is empty; every row passes through.
    """
    if not keycolumns:
        return df

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
