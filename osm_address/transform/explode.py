import random

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, expr, posexplode
from pyspark.sql.types import ArrayType, StructType


def explode_col(
        df_in: DataFrame,
        column: str,
        explode_prefix: str = "way_",
        position_column_name=None
) -> DataFrame:
    """

    Args:
        df_in: input data frame
        column: column of type array
        explode_prefix: prefix for all exploded columns if input column is a struct. If it is a single value in the
                        array the explode_prefix will be the new column name
        position_column_name: When given this column will return the position of the entry in the array

    Returns:
        A dataframe exploding all entries in the array as single rows in the result

    """
    try:
        input_col_index = df_in.columns.index(column)
    except ValueError as e:
        raise ValueError(f"Column {column} is not in list of columns: {df_in.columns}")
    new_columns = []

    column_type = df_in.schema[column].dataType

    if not isinstance(column_type, ArrayType):
        raise ValueError(f"Column {column} is not of type array: {column_type}")

    explode_col_prefix = f"exp_{random.randint(1,999999)}"

    df_exploded = df_in.select(
        "*",
        posexplode(col(column)).alias(f"{explode_col_prefix}_col_pos", f"{explode_col_prefix}_col_val")
    )

    if position_column_name:
        df_exploded = df_exploded.withColumnRenamed(f"{explode_col_prefix}_col_pos", position_column_name)
        new_columns.append(position_column_name)

    if isinstance(column_type.elementType, StructType):
        nested_columns = df_in.schema[column].dataType.elementType.names

        for nested_column in nested_columns:
            df_exploded = df_exploded.withColumn(
                f"{explode_prefix}{nested_column}",
                expr(f"{explode_col_prefix}_col_val['{nested_column}']")
            )
            new_columns.append(f"{explode_prefix}{nested_column}")
    else:
        df_exploded = df_exploded.withColumn(
            explode_prefix,
            col(f"{explode_col_prefix}_col_val")
        )
        new_columns.append(explode_prefix)

    df_exploded = df_exploded.select(
        df_in.columns[:input_col_index]+new_columns+df_in.columns[input_col_index+1:]
    )

    return df_exploded

