from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import col, lit

from osm_address.transform import (add_column_prefix, aggregate_column_to_list,
                                   explode_col, wrap_columns_into_struct)
from osm_address.udf import udf_create_polygon

NODE_COL_PREFIX = "node_"


def get_polygon_from_nodes(
        df_way: DataFrame,
        df_node: DataFrame,
        output_col: str,
        way_id_column_name="id"
) -> DataFrame:
    """

    Args:
        df_way: dataframe with a column containing an array of nodes
        df_node: dataframe with columns id, latitude, longitude
        output_col: name of the result column
        way_id_column_name: unique id in the df_way data frame

    Returns:
        DataFrame: with columns
            id - from the df_way DataFrame
            output_col - contains the polygon described by the nodes of the way

    """
    df_way_with_node_ids = explode_col(
        df_in=df_way,
        column="nodes",
        explode_prefix="way_node_id",
        position_column_name=f"{NODE_COL_PREFIX}pos"
    )

    df_node_simple = add_column_prefix(
        df_in=df_node.selectExpr("id", "latitude", "longitude"),
        column_prefix=NODE_COL_PREFIX
    )

    df_with_pos = df_way_with_node_ids.alias("way").join(
        df_node_simple.alias("node"),
        col("way.way_node_id") == col(f"node.{NODE_COL_PREFIX}id")
    ).drop(
        "way_node_id"
    )

    df_wrapped_node = wrap_columns_into_struct(
        df_in=df_with_pos,
        columns=[
            f"{NODE_COL_PREFIX}pos",
            f"{NODE_COL_PREFIX}id",
            f"{NODE_COL_PREFIX}latitude",
            f"{NODE_COL_PREFIX}longitude"
        ],
        wrap_column="node_struct"
    )

    df_node_list = aggregate_column_to_list(
        df_in=df_wrapped_node,
        collect_column="node_struct",
        group_by_columns=[way_id_column_name],
        result_column="node_list"
    )

    df_result = df_node_list.withColumn(
        output_col,
        udf_create_polygon(col("node_list"), lit(NODE_COL_PREFIX))
    ).drop(
        "node_list"
    )

    return df_result
