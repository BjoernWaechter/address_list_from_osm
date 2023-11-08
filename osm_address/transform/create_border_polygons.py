import logging

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit

from osm_address.osm import OsmData
from osm_address.transform import add_column_prefix, wrap_columns_into_struct, aggregate_column_to_list, explode_col
from osm_address.udf import udf_node_lists_to_polygon

log = logging.getLogger(__name__)

NODE_COL_PREFIX = "node_"
WAY_COL_PREFIX = "way_"


def create_border_polygons(
        osm_data: OsmData,
        relation_filter,
        additional_columns=None,
        add_debug_columns=False,
        result_polygon_name="multipolygon"
) -> DataFrame:
    """

    Args:
        osm_data: input of all osm data of a specific region
        relation_filter: filter to use on all relations of osm_data to get the polygons
        additional_columns: Additional column e.g. columns created by reading specific tags
        add_debug_columns: If set to True these columns will be added:
            id (id of the original relation)
            outer_polygon_count
            inner_polygon_count
            outer_point_count
            inner_point_count
            invalidity_explain
        result_polygon_name: name of the result column

    Returns:
        A Data Frame with multi polygons in a column named from argument result_polygon_name plus all
        columns defined in additional_columns.
        The polygons a constructed from relations filtered by relation_filter, linked to the related ways and nodes

    """
    if additional_columns is None:
        extra_columns = []
        extra_col_names = []
    else:
        extra_columns = [f"{x[1]} as {x[0]}" for x in additional_columns.items()]
        extra_col_names = additional_columns.keys()

    df_relation = osm_data.relations.where(relation_filter)

    df_relation = df_relation.selectExpr(
        "id",
        "relations",
        *extra_columns
    )

    # Explode the ways stored in the relations column and take only inner and outer ways
    df_exploded_way_id = explode_col(
        df_in=df_relation,
        column="relations",
        explode_prefix=WAY_COL_PREFIX
    ).where(
        f"{WAY_COL_PREFIX}role in ('inner','outer')"
    )

    # Select required columns from the ways data and prefix the columns with WAY_COL_PREFIX
    df_way_simple = add_column_prefix(
        df_in=osm_data.ways.select("id", "nodes"),
        column_prefix=WAY_COL_PREFIX
    )

    # Join the ways to the relations
    df_joined_way = df_exploded_way_id.join(
        df_way_simple,
        [f"{WAY_COL_PREFIX}id"]
    )

    # Explode the nodes stored in the nodes column with {NODE_COL_PREFIX}col_pos keeping the order information
    df_explode_node_id = explode_col(
        df_in=df_joined_way,
        column=f"{WAY_COL_PREFIX}nodes",
        explode_prefix=f"{NODE_COL_PREFIX}id",
        position_column_name=f"{NODE_COL_PREFIX}col_pos"
    )

    # Select required columns from the nodes data and prefix the columns with NODE_COL_PREFIX
    df_node_simple = add_column_prefix(
        df_in=osm_data.nodes.select("id", "longitude", "latitude"),
        column_prefix=NODE_COL_PREFIX
    )

    # Join the nodes to the ways
    df_joined_node = df_explode_node_id.join(
        df_node_simple,
        [f"{NODE_COL_PREFIX}id"]
    )

    # Wrap all nodes information into a column node_struct
    df_wrapped_node = wrap_columns_into_struct(
        df_in=df_joined_node,
        columns=[
            f"{NODE_COL_PREFIX}col_pos",
            f"{NODE_COL_PREFIX}id",
            f"{NODE_COL_PREFIX}latitude",
            f"{NODE_COL_PREFIX}longitude"
        ],
        wrap_column="node_struct"
    )

    # Aggregate all nodes in one way into an array ordered by the first value in the struct the col_pos
    df_node_list = aggregate_column_to_list(
        df_in=df_wrapped_node,
        collect_column="node_struct",
        group_by_columns=["id", f"{WAY_COL_PREFIX}id"],
        result_column="node_list"
    )

    # Wrap the list of nodes together with its role into node_list_struct
    df_wrapped_node_list = wrap_columns_into_struct(
        df_in=df_node_list,
        columns=[
            f"{WAY_COL_PREFIX}role",
            "node_list"
        ],
        wrap_column="node_list_struct"
    )

    # Aggregate all lists of nodes (ways) into a list of lists per relation
    df_list_of_node_list = aggregate_column_to_list(
        df_in=df_wrapped_node_list,
        collect_column="node_list_struct",
        group_by_columns=["id"],
        result_column="node_list_list",
        sort_list=False
    )

    # Use the udf to join the list of ways into a valid multi polygon
    df_polygons = df_list_of_node_list.withColumn(
        "polygon",
        udf_node_lists_to_polygon(col('node_list_list'), lit(NODE_COL_PREFIX))
    )

    if add_debug_columns:
        debug_columns = [
            "id",
            "polygon.outer_polygon_count as outer_polygon_count",
            "polygon.inner_polygon_count as inner_polygon_count",
            "polygon.outer_point_count   as outer_point_count",
            "polygon.inner_point_count   as inner_point_count",
            "polygon.invalidity_explain  as invalidity_explain"
        ]
    else:
        debug_columns = []

    # Remove empty polygons. This can happen at the boarder of a region where polygons might be cut and incomplete
    # Make the polygons valid. This an invalid case seen in the OSM data:
    # --------A--------C-------B---------D-------
    # All nodes are on a line and the order is A,B,C,D
    df_result = df_polygons.where(
        "polygon is not null"
    ).selectExpr(
        *extra_col_names,
        f"polygon.multipolygon as {result_polygon_name}",
        *debug_columns
    )

    return df_result
