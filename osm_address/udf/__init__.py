import re

import pandas as pd
from pyspark.sql.functions import udf
from pyspark.sql.pandas.functions import pandas_udf
from pyspark.sql.types import (ArrayType, IntegerType, StringType, StructField,
                               StructType)
from sedona.spark import GeometryType
from shapely.geometry import LineString, MultiPolygon, Point, Polygon
from shapely.validation import explain_validity, make_valid

schema_polygon_from_relations = StructType(
    [
        StructField("way_count", IntegerType(), True),
        StructField("outer_polygon_count", IntegerType(), True),
        StructField("inner_polygon_count", IntegerType(), True),
        StructField("outer_point_count", IntegerType(), True),
        StructField("inner_point_count", IntegerType(), True),
        StructField("invalidity_explain", StringType(), True),
        StructField("multipolygon", GeometryType(), True)
    ]
)


def node_lists_to_polygon(list_of_node_list, node_prefix):
    """

    Args:
        list_of_node_list: List of inner and outer polygons snippets as rows of points.
                e.g.:
                [
                    [
                        "outer",
                        [
                            Row(node_col_pos=1, node_id=1, node_longitude=0, node_latitude=0),
                            Row(node_col_pos=1, node_id=2, node_longitude=1, node_latitude=0),
                            Row(node_col_pos=1, node_id=3, node_longitude=1, node_latitude=1)
                        ],
                    ],
                    [
                        "outer",
                        [
                            Row(node_col_pos=1, node_id=3, node_longitude=1, node_latitude=1),
                            Row(node_col_pos=1, node_id=4, node_longitude=0, node_latitude=1),
                            Row(node_col_pos=1, node_id=1, node_longitude=0, node_latitude=0)
                        ]
                    ]
                ]
        node_prefix: prefix of the column names in the Row objects (e.g. _node)

    Returns:
        matching snippets of ways will be matched by the points at the beginning and end
        The ordered inner and outer snippets are returned as multipolygons

    """
    def remove_key(d, key):
        d.pop(key)
        return d

    way_count = len(list_of_node_list)

    todo = []
    ordered_list = {'outer': [], 'inner': []}
    result_lists = {'outer': [], 'inner': []}

    end_node = {'outer': None, 'inner': None}
    start_node = {'outer': None, 'inner': None}

    no_action = 0

    while (todo or list_of_node_list) and no_action <= len(todo):

        if list_of_node_list:
            todo.insert(0, list_of_node_list.pop(0))

        node_lis_dict = todo.pop(0)
        way_role = node_lis_dict[0]
        node_lis = [remove_key(x.asDict(), f"{node_prefix}col_pos") for x in node_lis_dict[1]]
        # raise RuntimeError(node_lis)

        if not ordered_list[way_role]:
            ordered_list[way_role] = [node_lis]
            start_node[way_role] = node_lis[0]
            end_node[way_role] = node_lis[-1]
            no_action = 0

        elif node_lis[0] == end_node[way_role]:
            ordered_list[way_role].append(node_lis)
            end_node[way_role] = node_lis[-1]
            no_action = 0

        elif node_lis[-1] == start_node[way_role]:
            ordered_list[way_role].insert(0, node_lis)
            start_node[way_role] = node_lis[0]
            no_action = 0

        elif node_lis[0] == start_node[way_role]:
            node_lis.reverse()
            ordered_list[way_role].insert(0, node_lis)
            start_node[way_role] = node_lis[0]
            no_action = 0

        elif node_lis[-1] == end_node[way_role]:
            node_lis.reverse()
            ordered_list[way_role].append(node_lis)
            end_node[way_role] = node_lis[-1]
            no_action = 0

        else:
            todo.append(node_lis_dict)
            no_action += 1

        if ordered_list[way_role][0][0] == ordered_list[way_role][-1][-1]:
            result_lists[way_role].append(ordered_list[way_role])
            ordered_list[way_role] = []

    if todo or no_action > len(todo) or len(result_lists['outer']) == 0:
        return None

    inner_polygons = []
    outer_polygons = []

    for inner_poly in result_lists['inner']:
        pos = []
        for way in inner_poly:
            for node in way[:-1]:
                pos.append((node[f"{node_prefix}longitude"], node[f"{node_prefix}latitude"],))
        inner_polygons.append(LineString(pos))

    for outer_poly in result_lists['outer']:
        pos = []
        for way in outer_poly:
            for node in way[:-1]:
                pos.append((node[f"{node_prefix}longitude"], node[f"{node_prefix}latitude"]))

        matching_inner_polygons = []

        outer_polygon = Polygon(pos)

        for inner_poly in inner_polygons:

            if outer_polygon.contains(inner_poly):
                matching_inner_polygons.append(inner_poly)

        outer_polygons.append(Polygon(shell=pos, holes=matching_inner_polygons))

    outer_point_count = 0
    inner_point_count = 0

    for polys in outer_polygons:
        outer_point_count += len(polys.exterior.coords)

        for interior in polys.interiors:
            inner_point_count += len(interior.coords)

    multi_poly = MultiPolygon(outer_polygons)

    validity_problems = explain_validity(multi_poly)
    validity_problems = validity_problems if validity_problems != "Valid Geometry" else None

    return way_count, len(outer_polygons), len(inner_polygons), \
        outer_point_count, inner_point_count, validity_problems, make_valid(multi_poly)


udf_node_lists_to_polygon = udf(node_lists_to_polygon, schema_polygon_from_relations)


def get_integer_num(n: str):
    """

    Args:
        n: string representing a potential integer value

    Returns:
        Integer value if the input can be parsed as an integer.
        None if the input is not an integer

    """
    try:
        i = int(n)
        return i
    except (ValueError, TypeError):
        return None


def is_odd(num):
    """

    Args:
        num: a number

    Returns:
        0 if the input is even
        1 if the input is odd
        -1 if the input is not an integer

    """
    try:
        return int(num) % 2
    except (ValueError, TypeError):
        return -1


@pandas_udf(ArrayType(StringType()))
def udf_explode_housenumber(s: pd.Series) -> pd.Series:
    """

    Args:
        s: Spark column containing a string representing a house number

    Returns:
        all possible house numbers in an array represented by the input string
        e.g.
           1-5: [1, 3, 5, 1-5]
           13a-c: [13a, 13b, 13c, 13a-c]

    """
    pattern_num_with_chars = re.compile(
        r"^([0-9]+)([a-z])-([a-z])"
    )
    pattern_num_twice_with_chars = re.compile(
        r"^([0-9]+)([a-z])-([0-9]+)([a-z])"
    )

    def hsnr_2_array(hnr):
        if "-" in hnr:
            hnr_array = hnr.split("-")

            if len(hnr_array) == 2:
                nr1 = get_integer_num(hnr_array[0])
                nr2 = get_integer_num(hnr_array[1])

                if nr1 and nr2:
                    if nr1 < nr2:
                        if is_odd(nr1) == is_odd(nr2):
                            return [str(x) for x in range(nr1, nr2 + 2, 2)] + [hnr]
                        else:
                            return [str(x) for x in range(nr1, nr2 + 1)] + [hnr]
                    else:
                        return [str(nr1), str(nr2)] + [hnr]

                match_num_with_chars = pattern_num_with_chars.match(hnr)

                if match_num_with_chars:
                    res = [match_num_with_chars.group(1) + chr(i)
                           for i in range(
                            ord(match_num_with_chars.group(2)),
                            ord(match_num_with_chars.group(3)) + 1)]
                    if res:
                        return res + [hnr]
                    else:
                        return [hnr]

                match_num_twice_with_chars = pattern_num_twice_with_chars.match(hnr)

                if match_num_twice_with_chars and \
                        match_num_twice_with_chars.group(1) == match_num_twice_with_chars.group(3):
                    res = [match_num_twice_with_chars.group(1) + chr(i)
                           for i in range(
                            ord(match_num_twice_with_chars.group(2)),
                            ord(match_num_twice_with_chars.group(4)) + 1)]
                    if res:
                        return res + [hnr]
                    else:
                        return [hnr]

        elif "," in hnr:
            return [x.strip() for x in hnr.split(",")] + [hnr]
        elif "/" in hnr:
            return [x.strip() for x in hnr.split("/")] + [hnr]
        elif "+" in hnr:
            return [x.strip() for x in hnr.split("+")] + [hnr]
        elif ";" in hnr:
            return [x.strip() for x in hnr.split(";")] + [hnr]
        return [hnr]

    return s.map(lambda x: hsnr_2_array(x.lstrip('0').lower()))


def create_polygon(points, pos_prefix):
    """

    Args:
        points: list of dict containing longitude and latitude values
        pos_prefix: longitude and latitude values will be accessed in the struct with this prefix

    Returns:
        Geometry created by the input points.

    """
    if len(points) == 0:
        raise RuntimeError("Provide at least one point as input")

    raw_points = [(
        h[f"{pos_prefix}longitude"],
        h[f"{pos_prefix}latitude"]
    ) for h in points]

    if len(raw_points) >= 4 and raw_points[0] == raw_points[-1]:
        return Polygon(shell=raw_points)
    elif len(raw_points) == 1:
        return Point(raw_points[0])
    else:
        return LineString(coordinates=raw_points)


udf_create_polygon = udf(create_polygon, GeometryType())
