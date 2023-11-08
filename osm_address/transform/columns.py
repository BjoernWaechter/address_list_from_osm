from typing import List

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, collect_list, first, sort_array, struct


def add_column_prefix(
        df_in: DataFrame,
        column_prefix: str
) -> DataFrame:
    """

    Args:
        df_in: input dataframe
        column_prefix: a prefix for all column names

    Returns:
        DataFrame: original dataframe with all column names prefixed with column_prefix

    """
    return df_in.select([col(col_name).alias(f"{column_prefix}{col_name}") for col_name in df_in.columns])


def wrap_columns_into_struct(
        df_in: DataFrame,
        columns: List[str],
        wrap_column: str
) -> DataFrame:
    """

    Args:
        df_in: input dataframe
        columns: list of columns which will be prepped into a struct
        wrap_column: result column name

    Returns:
        All columns in argument 'columns' are wrapped into a struct preserving the original
        column names inside the struct,
    """
    return df_in.withColumn(
        wrap_column,
        struct(*[col(col_name) for col_name in columns])
    ).drop(
        *columns
    )


def aggregate_column_to_list(
        df_in: DataFrame,
        group_by_columns: List[str],
        collect_column: str,
        result_column: str,
        sort_list=True
) -> DataFrame:
    """

    Args:
        df_in: input data frame
        group_by_columns: list of columns to group by
        collect_column: this column will be collected into a list
        result_column: name of the collect list result column
        sort_list: If set to True the collected elements will be sorted inside the list

    Returns:
        the result dataframe has three types of columns:
        - The columns grouped by in the aggregation
        - The columns collected into a list
        - All other columns from the original dataframe will return any value (using first aggregation)

    """
    agg_cols = [first(x.name).alias(x.name) for x in df_in.schema.fields if x.name not in
                group_by_columns + [collect_column]
                ]

    if sort_list:
        agg_cols.append(sort_array(collect_list(collect_column)).alias(result_column))
    else:
        agg_cols.append(collect_list(collect_column).alias(result_column))

    return df_in.groupBy(group_by_columns).agg(*agg_cols)
