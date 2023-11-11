from pyspark.sql import DataFrame, Window
from pyspark.sql.functions import col, row_number


def remove_duplicate_rows(
        df_input: DataFrame,
        unique_col: str,
        decision_col: str,
        decision_max_first: bool = True
) -> DataFrame:
    """

    Args:
        df_input: input data frame
        unique_col: this column will be unique in the result
        decision_col: if there are multiple rows with the same value in the unique_col this column decides
                      which row will be returned.
        decision_max_first:
            True: The row with the greatest value in decision_col will be returned
            False: The row with the lowest value in decision_col will be returned

    Returns:
        Dataframe with the same schema as the input with removed duplicates

    """

    if decision_max_first:
        order_col = col(decision_col).desc_nulls_last()
    else:
        order_col = col(decision_col).asc_nulls_last()

    w = Window.partitionBy(unique_col).orderBy(order_col)

    df_input_ordered = df_input.withColumn(
        f'{unique_col.replace(".","_")}_row_no', row_number().over(w)
    )

    df_result = df_input_ordered\
        .where(f'{unique_col.replace(".","_")}_row_no = 1')\
        .drop(f'{unique_col.replace(".","_")}_row_no')

    return df_result
