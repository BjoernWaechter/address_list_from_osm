
from pyspark.sql import DataFrame


def check_for_duplicate_columns(
        df_1: DataFrame,
        df_2: DataFrame
):

    assert isinstance(df_1, DataFrame), "df_1 is not a pyspark Dataframe"
    assert isinstance(df_2, DataFrame), "df_2 is not a pyspark Dataframe"

    intersect_cols = (set(df_1.columns) & set(df_2.columns))

    if len(intersect_cols) > 0:
        raise RuntimeError(f"Dataframes have these columns in common: \"{', '.join(intersect_cols)}\"")
