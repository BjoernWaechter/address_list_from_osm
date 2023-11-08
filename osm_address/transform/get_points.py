from pyspark.sql import DataFrame

from osm_address.osm import OsmData, get_polygon_from_nodes


def get_points_from_nodes_and_ways(
        osm_data: OsmData,
        osm_filter: str,
        additional_columns=None,
        add_type_based_ids=True,
        point_column="address_point"
) -> DataFrame:
    """

    Args:
        osm_data: input of all osm data of a specific region
        osm_filter: this filter will be applied to nodes and ways
        additional_columns: these columns will be added for both nodes and ways
        add_type_based_ids: If True ids for nodes (e.g. N98765) and ways (e.g. W12345) will be added
        point_column: Name of the column in the result dataframe

    Returns:
        dataframe with points from nodes plus the center of polygons from ways

    """
    df_raw_address_node = osm_data.nodes.where(osm_filter)
    df_raw_address_way = osm_data.ways.where(osm_filter)

    if additional_columns is None:
        extra_node_columns = []
        extra_way_columns = []
        extra_col_names = []
    else:
        extra_node_columns = [f"{x[1]} as {x[0]}" for x in additional_columns.items()]
        extra_way_columns = [f"{x[1]} as {x[0]}" for x in additional_columns.items()]
        extra_col_names = list(additional_columns.keys())

        if add_type_based_ids:
            extra_node_columns += ["concat('N', id) as id"]
            extra_way_columns += ["concat('W', id) as id"]
            extra_col_names += ["id"]

    df_raw_address_node = df_raw_address_node.selectExpr(
        *extra_node_columns,
        "longitude",
        "latitude",
        "tags",
        f"ST_Point(CAST(longitude AS Decimal(24,20)), CAST(latitude AS Decimal(24,20))) as {point_column}"
    )

    df_raw_address_way = df_raw_address_way.selectExpr(
        "nodes",
        "tags",
        *extra_way_columns
    )

    df_raw_address_way_geo = get_polygon_from_nodes(
        df_way=df_raw_address_way,
        df_node=osm_data.nodes,
        output_col="way_polygon"
    ).selectExpr(
        "*",
        f"ST_Centroid(way_polygon) as {point_column}"
    ).selectExpr(
        *extra_col_names,
        "ST_X(address_point) as longitude",
        "ST_Y(address_point) as latitude",
        "tags",
        f"{point_column}"
    )

    df_raw_address = df_raw_address_node.union(df_raw_address_way_geo)

    return df_raw_address
