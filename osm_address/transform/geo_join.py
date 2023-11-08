from pyspark.sql import DataFrame
from pyspark.sql.functions import expr
from sedona.sql.types import GeometryType


def join_point_in_multipolygon(
        df_in: DataFrame,
        point_column: str,
        df_polygon: DataFrame,
        polygon_column: str,
        partition_count=1000
):
    """

    Args:
        df_in: data frame containing points
        point_column: name of the column containing the points
        df_polygon: dataframe containing polygons
        polygon_column: name of the column containing the polygons
        partition_count: The join is executed on partitions. Based on the amount of data the number can be changed

    Returns:
        dataframe join where points are inside the polygons
    """

    assert point_column in df_in.schema.fieldNames(), f"'{point_column}' not in data frame df_in"
    assert polygon_column in df_polygon.schema.fieldNames(), f"'{polygon_column}' not in data frame df_polygon"

    assert df_in.schema[point_column].dataType == GeometryType(), \
        f"'{point_column}' in data frame df_in is of type '{df_in.schema[point_column].dataType}'"
    assert df_polygon.schema[polygon_column].dataType == GeometryType(), \
        f"'{polygon_column}' in data frame df_polygon is of type '{df_polygon.schema[polygon_column].dataType}'"

    df_result = df_polygon.repartition(partition_count).alias("poly").join(
        df_in.alias("point"),
        expr(f"ST_Contains(poly.{polygon_column}, point.{point_column})")
    )

    return df_result
