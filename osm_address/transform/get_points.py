import random

from pyspark.sql import DataFrame
from pyspark.sql.functions import expr

from osm_address.osm import OsmData, get_polygon_from_nodes


def get_points_from_nodes_and_ways(
        osm_data: OsmData,
        osm_filter: str,
        additional_columns=None,
        id_column=None,
        geometry_column=None,
        centroid_column=None
) -> DataFrame:
    """

    Args:
        osm_data: input of all osm data of a specific region
        osm_filter: this filter will be applied to nodes and ways
        additional_columns: these columns will be added for both nodes and ways
        id_column:  If not None a column named id_column containing the source id will be added
                    for nodes (e.g. N98765) and ways (e.g. W12345)
        geometry_column: Name of the geometry column in the result dataframe. Can be None
        centroid_column: Name of the centroid column in the result dataframe. Can be None

    Returns:
        dataframe with points from nodes plus the center of polygons from ways

    """
    df_raw_address_node = osm_data.nodes.where(osm_filter)
    df_raw_address_way = osm_data.ways.where(osm_filter)

    if additional_columns is None:
        extra_node_columns = []
        extra_way_columns = []
    else:
        extra_node_columns = [f"{x[1]} as {x[0]}" for x in additional_columns.items()]
        extra_way_columns = [f"{x[1]} as {x[0]}" for x in additional_columns.items()]

    if id_column:
        id_column_name = id_column
        extra_node_columns += [id_column_name]
        extra_way_columns += [id_column_name]
    else:
        id_column_name = f"id_{random.randint(1,999999)}"

    node_geom_columns = []
    way_geom_columns = []
    geom_col_names = []

    if centroid_column:
        node_geom_columns.append(
            f"ST_Point(CAST(longitude AS Decimal(24,20)), CAST(latitude AS Decimal(24,20))) as {centroid_column}"
        )
        way_geom_columns.append(f"ST_Centroid(way_polygon) as {centroid_column}")
        geom_col_names.append(centroid_column)

    if geometry_column:
        node_geom_columns.append(
            f"ST_Point(CAST(longitude AS Decimal(24,20)), CAST(latitude AS Decimal(24,20))) as {geometry_column}"
        )
        way_geom_columns.append(f"way_polygon as {geometry_column}")
        geom_col_names.append(geometry_column)

    df_raw_address_node = df_raw_address_node.withColumnRenamed(
        "id",
        "temp_id_col"
    ).withColumn(
        id_column_name,
        expr(f"concat('N', temp_id_col)")
    ).drop(
        "temp_id_col"
    ).selectExpr(
        "*",
        *node_geom_columns
    ).selectExpr(
        *extra_node_columns,
        *geom_col_names
    )

    df_raw_address_way = df_raw_address_way.withColumn(
        "temp_id_col",
        expr(f"concat('W', id) as {id_column_name}")
    ).drop(
        "id"
    )

    df_raw_address_way_geo = get_polygon_from_nodes(
        df_way=df_raw_address_way,
        df_node=osm_data.nodes,
        output_col="way_polygon",
        way_id_column_name="temp_id_col"
    ).withColumnRenamed(
        "temp_id_col", id_column_name
    ).selectExpr(
        "*",
        *way_geom_columns
    ).selectExpr(
        *extra_way_columns,
        *geom_col_names
    )

    df_raw_address = df_raw_address_node.union(df_raw_address_way_geo)

    return df_raw_address
