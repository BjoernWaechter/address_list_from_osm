import random

import pandas as pd
import pytest
from pyspark import Row
from pyspark.sql.functions import col
from pyspark.sql.types import ArrayType, StringType, StructField, StructType
from shapely.geometry import MultiPolygon, Polygon, Point, LineString
from utils import generic_row_with_schema

from osm_address.udf import (get_integer_num, is_odd, node_lists_to_polygon,
                             schema_polygon_from_relations, udf_explode_housenumber, create_polygon)


class TestUdf:

    def test_simple_two_ways(self):
        res = node_lists_to_polygon(
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
            ],
            node_prefix="node_"
        )

        result_row = generic_row_with_schema(res, schema_polygon_from_relations)

        assert result_row["way_count"] == 2  # way count
        assert result_row["outer_polygon_count"] == 1  # outer polygon count
        assert result_row["inner_polygon_count"] == 0  # inner polygon count
        assert result_row["outer_point_count"] == 5  # outer node count (first and last are the same)
        assert result_row["inner_point_count"] == 0  # inner node count (first and last are the same)
        assert result_row["invalidity_explain"] is None
        assert result_row["multipolygon"] == MultiPolygon(
            polygons=[Polygon(shell=[(0, 0), (1, 0), (1, 1), (0, 1), (0, 0)])]
        )

    def test_simple_inner_and_outer(self):
        res = node_lists_to_polygon(
            [
                [
                    "inner",
                    [
                         Row(node_col_pos=1, node_id=1, node_longitude=1, node_latitude=1),
                         Row(node_col_pos=1, node_id=2, node_longitude=2, node_latitude=1),
                         Row(node_col_pos=1, node_id=3, node_longitude=2, node_latitude=2),
                         Row(node_col_pos=1, node_id=4, node_longitude=1, node_latitude=2),
                         Row(node_col_pos=1, node_id=1, node_longitude=1, node_latitude=1)
                    ],
                ],
                [
                    "outer",
                    [
                         Row(node_col_pos=1, node_id=11, node_longitude=0, node_latitude=0),
                         Row(node_col_pos=1, node_id=12, node_longitude=3, node_latitude=0),
                         Row(node_col_pos=1, node_id=13, node_longitude=3, node_latitude=3),
                         Row(node_col_pos=1, node_id=14, node_longitude=0, node_latitude=3),
                         Row(node_col_pos=1, node_id=11, node_longitude=0, node_latitude=0)
                    ],
                ]
            ],
            node_prefix="node_"
        )

        result_row = generic_row_with_schema(res, schema_polygon_from_relations)

        assert result_row["way_count"] == 2  # way count
        assert result_row["outer_polygon_count"] == 1  # outer polygon count
        assert result_row["inner_polygon_count"] == 1  # inner polygon count
        assert result_row["outer_point_count"] == 5  # outer node count (first and last are the same)
        assert result_row["inner_point_count"] == 5  # inner node count (first and last are the same)
        assert result_row["invalidity_explain"] is None
        assert result_row["multipolygon"].is_valid
        assert result_row["multipolygon"].equals(MultiPolygon(
            polygons=[Polygon(
                shell=[(0, 0), (3, 0), (3, 3), (0, 3), (0, 0)],
                holes=[[(1, 1), (2, 1), (2, 2), (1, 2), (1, 1)]]
            )]
        ))

    def test_out_of_order(self):
        res = node_lists_to_polygon(
            [
                [
                    "outer",
                    [
                         Row(node_col_pos=1, node_id=2, node_longitude=1, node_latitude=0),
                         Row(node_col_pos=1, node_id=1, node_longitude=0, node_latitude=0)
                    ]
                ],
                [
                    "outer",
                    [
                         Row(node_col_pos=1, node_id=2, node_longitude=1, node_latitude=0),
                         Row(node_col_pos=1, node_id=3, node_longitude=1, node_latitude=1)
                    ]
                ],
                [
                    "outer",
                    [
                         Row(node_col_pos=1, node_id=1, node_longitude=0, node_latitude=0),
                         Row(node_col_pos=1, node_id=4, node_longitude=0, node_latitude=1)
                    ]
                ],
                [
                    "outer",
                    [
                         Row(node_col_pos=1, node_id=3, node_longitude=1, node_latitude=1),
                         Row(node_col_pos=1, node_id=4, node_longitude=0, node_latitude=1)
                    ]
                ]
            ],
            node_prefix="node_"
        )

        result_row = generic_row_with_schema(res, schema_polygon_from_relations)

        assert result_row["way_count"] == 4  # way count
        assert result_row["outer_polygon_count"] == 1  # outer polygon count
        assert result_row["inner_polygon_count"] == 0  # inner polygon count
        assert result_row["outer_point_count"] == 5  # outer node count (first and last are the same)
        assert result_row["inner_point_count"] == 0  # inner node count (first and last are the same)
        assert result_row["invalidity_explain"] is None
        assert result_row["multipolygon"].is_valid
        assert result_row["multipolygon"].equals(
            MultiPolygon(polygons=[Polygon(shell=[(0, 0), (1, 0), (1, 1), (0, 1), (0, 0)])])
        )

    def test_out_of_order_inner_and_outer(self):

        ways = [
                [
                    "inner",
                    [
                         Row(node_col_pos=1, node_id=2, node_longitude=2, node_latitude=1),
                         Row(node_col_pos=1, node_id=1, node_longitude=1, node_latitude=1)
                    ]
                ],
                [
                    "inner",
                    [
                         Row(node_col_pos=1, node_id=2, node_longitude=2, node_latitude=1),
                         Row(node_col_pos=1, node_id=3, node_longitude=2, node_latitude=2)
                    ]
                ],
                [
                    "inner",
                    [
                         Row(node_col_pos=1, node_id=1, node_longitude=1, node_latitude=1),
                         Row(node_col_pos=1, node_id=4, node_longitude=1, node_latitude=2)
                    ]
                ],
                [
                    "inner",
                    [
                         Row(node_col_pos=1, node_id=3, node_longitude=2, node_latitude=2),
                         Row(node_col_pos=1, node_id=4, node_longitude=1, node_latitude=2)
                    ]
                ],
                [
                    "outer",
                    [
                         Row(node_col_pos=1, node_id=12, node_longitude=3, node_latitude=0),
                         Row(node_col_pos=1, node_id=11, node_longitude=0, node_latitude=0)
                    ]
                ],
                [
                    "outer",
                    [
                         Row(node_col_pos=1, node_id=12, node_longitude=3, node_latitude=0),
                         Row(node_col_pos=1, node_id=13, node_longitude=3, node_latitude=3)
                    ]
                ],
                [
                    "outer",
                    [
                         Row(node_col_pos=1, node_id=11, node_longitude=0, node_latitude=0),
                         Row(node_col_pos=1, node_id=14, node_longitude=0, node_latitude=3)
                    ]
                ],
                [
                    "outer",
                    [
                         Row(node_col_pos=1, node_id=13, node_longitude=3, node_latitude=3),
                         Row(node_col_pos=1, node_id=14, node_longitude=0, node_latitude=3)
                    ]
                ]
            ]

        random.Random(4).shuffle(ways)

        res = node_lists_to_polygon(ways, node_prefix="node_")

        result_row = generic_row_with_schema(res, schema_polygon_from_relations)

        assert result_row["way_count"] == 8  # way count
        assert result_row["outer_polygon_count"] == 1  # outer polygon count
        assert result_row["inner_polygon_count"] == 1  # inner polygon count
        assert result_row["outer_point_count"] == 5  # outer node count (first and last are the same)
        assert result_row["inner_point_count"] == 5  # inner node count (first and last are the same)
        assert result_row["invalidity_explain"] is None
        assert result_row["multipolygon"].is_valid
        assert result_row["multipolygon"].equals(
            MultiPolygon(polygons=[Polygon(
                shell=[(0, 0), (3, 0), (3, 3), (0, 3), (0, 0)],
                holes=[[(1, 1), (2, 1), (2, 2), (1, 2), (1, 1)]]
            )])
        )

    def test_incomplete(self):
        res = node_lists_to_polygon(
            [
                [
                    "outer",
                    [
                         Row(node_col_pos=1, node_id=2, node_longitude=1, node_latitude=0),
                         Row(node_col_pos=1, node_id=1, node_longitude=0, node_latitude=0)
                    ]
                ],
                [
                    "outer",
                    [
                         Row(node_col_pos=1, node_id=2, node_longitude=1, node_latitude=0),
                         Row(node_col_pos=1, node_id=3, node_longitude=1, node_latitude=1)
                    ]
                ],
                [
                    "outer",
                    [
                         Row(node_col_pos=1, node_id=1, node_longitude=0, node_latitude=0),
                         Row(node_col_pos=1, node_id=4, node_longitude=0, node_latitude=1)
                    ]
                ]
            ],
            node_prefix="node_"
        )

        assert res is None

    def test_get_integer_num(self):

        assert get_integer_num("1") == 1
        assert get_integer_num("0.1") is None
        assert get_integer_num(None) is None
        assert get_integer_num("") is None

    def test_is_odd(self):

        assert is_odd("2") == 0
        assert is_odd("0") == 0
        assert is_odd("1") == 1
        assert is_odd("99") == 1
        assert is_odd(float(10)) == 0
        assert is_odd(None) == -1

    def test_udf_explode_housenumber(self, test_context):
        data = [
            ("12", ["12"]),
            ("12-16", ["12", "14", "16", "12-16"]),
            ("1-9", ["1", "3", "5", "7", "9", "1-9"]),
            ("1-4", ["1", "2", "3", "4", "1-4"]),
            ("9-1", ["9", "1", "9-1"]),
            ("13a-c", ["13a", "13b", "13c", "13a-c"]),
            ("99t-99v", ["99t", "99u", "99v", "99t-99v"]),
            ("1x-a", ["1x-a"]),
            ("1x-1a", ["1x-1a"]),
            ("11,12", ["11", "12", "11,12"]),
            ("11/12", ["11", "12", "11/12"]),
            ("11+12", ["11", "12", "11+12"]),
            ("11;12", ["11", "12", "11;12"])
        ]
        data_df = test_context.spark.createDataFrame(
            data=data,
            schema=StructType([
                StructField("hnr", StringType(), True),
                StructField("hnr_array_expect", ArrayType(StringType(), True), True),
            ])
        )

        wrong_results = data_df.withColumn(
            "hnr_array", udf_explode_housenumber(col("hnr"))
        ).filter("hnr_array != hnr_array_expect")

        assert wrong_results.count() == 0, wrong_results.show()

        # Test the original functions since testing via udf is not shown in coverage
        udf_explode_housenumber_orig = udf_explode_housenumber.__wrapped__

        for hnr in data:
            res = udf_explode_housenumber_orig(pd.Series(hnr[0]))
            assert res[0] == hnr[1]

    def test_create_polygon(self):
        # Test the original functions since testing via udf is not shown in coverage
        with pytest.raises(RuntimeError):
            res = create_polygon([
            ], "node_")

        res = create_polygon([
            {"node_longitude": 0, "node_latitude": 0}
        ], "node_")
        assert res == Point(0, 0)

        res = create_polygon([
            {"node_longitude": 0, "node_latitude": 0},
            {"node_longitude": 1, "node_latitude": 1}
        ], "node_")
        assert res == LineString([(0, 0), (1, 1)])

        res = create_polygon([
            {"node_longitude": 0, "node_latitude": 0},
            {"node_longitude": 1, "node_latitude": 1},
            {"node_longitude": 2, "node_latitude": 2}
        ], "node_")
        assert res == LineString(coordinates=[(0, 0), (1, 1), (2, 2)])

        res = create_polygon([
            {"node_longitude": 0, "node_latitude": 0},
            {"node_longitude": 0, "node_latitude": 1},
            {"node_longitude": 1, "node_latitude": 1},
            {"node_longitude": 1, "node_latitude": 0},
            {"node_longitude": 0, "node_latitude": 0}
        ], "node_")
        assert res == Polygon(shell=[(0, 0), (0, 1), (1, 1), (1, 0), (0, 0)])
