import random

from pyspark.sql import DataFrame

from osm_address.osm import OsmData, get_polygon_from_nodes


def get_points_from_nodes_and_ways(
        osm_data: OsmData,
        osm_filter: str,
        additional_columns=None,
        id_column=None,
        point_column="address_point",
        centroids_only=True
) -> DataFrame:
    """

    Args:
        osm_data: input of all osm data of a specific region
        osm_filter: this filter will be applied to nodes and ways
        additional_columns: these columns will be added for both nodes and ways
        id_column:  If not None a column named id_column containing the source id will be added
                    for nodes (e.g. N98765) and ways (e.g. W12345)
        point_column: Name of the column in the result dataframe
        centroids_only: True: return centroids only, False: return complete geometries of the ways

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

    if id_column:
        id_column_name = id_column
    else:
        id_column_name = f"id_{random.randint(1,999999)}"

    extra_col_names += [id_column_name]
    extra_node_columns += [f"concat('N', id) as {id_column_name}"]
    extra_way_columns += [f"concat('W', id) as {id_column_name}"]

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

    if centroids_only:
        node_geometry = f"ST_Centroid(way_polygon) as {point_column}"
    else:
        node_geometry = f"way_polygon as {point_column}"

    df_raw_address_way_geo = get_polygon_from_nodes(
        df_way=df_raw_address_way,
        df_node=osm_data.nodes,
        output_col="way_polygon",
        way_id_column_name=id_column_name
    ).selectExpr(
        "*",
        node_geometry
    ).selectExpr(
        *extra_col_names,
        f"ST_X({point_column}) as longitude",
        f"ST_Y({point_column}) as latitude",
        "tags",
        f"{point_column}"
    )

    df_raw_address = df_raw_address_node.union(df_raw_address_way_geo)

    if not id_column:
        return df_raw_address.drop(id_column_name)
    else:
        return df_raw_address
