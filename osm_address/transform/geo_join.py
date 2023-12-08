import random

from pyspark.sql import DataFrame
from pyspark.sql.functions import expr
from sedona.sql.types import GeometryType

from osm_address.transform import remove_duplicate_rows
from osm_address.utils.schema import check_for_duplicate_columns


random.seed(42)


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


def join_nearest_geometry(
        df_in_1: DataFrame,
        df_in_2: DataFrame,
        column_name_1: str,
        column_name_2: str,
        epsg_1="epsg:4326",
        epsg_2="epsg:4326",
        epsg_meter_based="epsg:25832",
        partition_count=1000,
        distance_meter_column="distance",
        max_meter=5000,
        join_type="leftouter",
        closest_point_column_1=None,
        closest_point_column_2=None,
        s2_level=None
):
    check_for_duplicate_columns(
        df_1=df_in_1,
        df_2=df_in_2
    )

    unique_id_col = f"uniqueid_{random.randint(1,999999)}"

    epsg_col_1 = f"{column_name_1}_{epsg_meter_based.replace(':','_')}"
    epsg_col_2 = f"{column_name_2}_{epsg_meter_based.replace(':', '_')}"

    df_in_1_epsg = df_in_1.withColumn(
        epsg_col_1,
        expr(f"ST_Transform({column_name_1}, '{epsg_1}', '{epsg_meter_based}')")
    ).withColumn(
        unique_id_col,
        expr("monotonically_increasing_id()")
    ).withColumn(
        f"{epsg_col_1}_buffer",
        expr(f"ST_Buffer({epsg_col_1}, {max_meter})")
    )

    df_in_2_epsg = df_in_2.withColumn(
        epsg_col_2,
        expr(f"ST_Transform({column_name_2}, '{epsg_2}', '{epsg_meter_based}')")
    )

    if s2_level:

        df_in_1_buffer = df_in_1_epsg.withColumn(
            f"{column_name_1}_buffer",
            expr(f"ST_Transform({epsg_col_1}_buffer, '{epsg_meter_based}', '{epsg_1}')")
        )

        df_in_1_epsg_s2 = df_in_1_buffer.withColumn(
            f"{column_name_1}_buffer_s2",
            expr(f"explode(ST_S2CellIDs({column_name_1}_buffer, {s2_level}))")
        )

        df_in_2_epsg_s2 = df_in_2_epsg.withColumn(
            f"{column_name_2}_s2",
            expr(f"explode(ST_S2CellIDs({column_name_2}, {s2_level}))")
        )

        df_buffer_join = df_in_1_epsg_s2.repartition(partition_count).join(
            other=df_in_2_epsg_s2,
            on=expr(
                f"{column_name_1}_buffer_s2 = {column_name_2}_s2 AND "
                f"ST_Intersects({epsg_col_2}, {epsg_col_1}_buffer)"),
            how=join_type
        )

    else:

        df_buffer_join = df_in_1_epsg.repartition(partition_count).join(
            other=df_in_2_epsg,
            on=expr(f"ST_Intersects({epsg_col_2}, {epsg_col_1}_buffer)"),
            how=join_type
        )

    df_distance = df_buffer_join.withColumn(
        distance_meter_column,
        expr(
            f"CASE "
            f"   WHEN {epsg_col_2} IS NOT NULL THEN "
            f"      ST_Distance("
            f"{epsg_col_1}, "
            f"{epsg_col_2}) "
            f"   ELSE NULL "
            f"END")
    )

    df_dist_result = df_distance.filter(f"{distance_meter_column} <= {max_meter} OR {distance_meter_column} IS NULL")

    if closest_point_column_1:
        df_dist_result = df_dist_result.withColumn(
            closest_point_column_1,
            expr(f"ST_Transform(ST_ClosestPoint({epsg_col_1}, {epsg_col_2}), '{epsg_meter_based}', '{epsg_1}')")
        )

    if closest_point_column_2:
        df_dist_result = df_dist_result.withColumn(
            closest_point_column_2,
            expr(f"ST_Transform(ST_ClosestPoint({epsg_col_2}, {epsg_col_1}), '{epsg_meter_based}', '{epsg_2}')")
        )

    df_nearest_only = remove_duplicate_rows(
        df_input=df_dist_result,
        unique_col=unique_id_col,
        decision_col=distance_meter_column,
        decision_max_first=False
    ).drop(
        unique_id_col,
        epsg_col_1,
        epsg_col_2
    )

    return df_nearest_only
