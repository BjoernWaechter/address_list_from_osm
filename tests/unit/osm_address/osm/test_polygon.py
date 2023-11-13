from pyspark.sql.types import (ArrayType, DoubleType, LongType, StringType,
                               StructField, StructType)
from sedona.sql.types import GeometryType
from shapely.geometry import LineString, Point, Polygon

from osm_address.osm.polygon import get_polygon_from_nodes


class TestPolygon:

    def test_get_polygon_from_nodes(self, test_context):
        ways_data = [
            (1, [11, 12, 13, 14, 11]),
            (2, [11, 12, 13]),
            (3, [11])
        ]

        df_ways = test_context.spark.createDataFrame(
            data=ways_data,
            schema=StructType([
                StructField("id", StringType(), True),
                StructField("nodes", ArrayType(LongType(), True), True)
            ]))

        nodes_data = [
            (11, 0.0, 0.0),
            (12, 1.0, 0.0),
            (13, 1.0, 1.0),
            (14, 0.0, 1.0),
        ]

        df_nodes = test_context.spark.createDataFrame(
            data=nodes_data,
            schema=StructType([
                StructField("id", StringType(), True),
                StructField("latitude", DoubleType(), True),
                StructField("longitude", DoubleType(), True),
            ]))

        df_res = get_polygon_from_nodes(
            df_way=df_ways,
            df_node=df_nodes,
            output_col="test_geo"
        ).orderBy("id")

        geo_data = df_res.collect()

        assert isinstance(geo_data[0]["test_geo"], Polygon)
        assert isinstance(geo_data[1]["test_geo"], LineString)
        assert isinstance(geo_data[2]["test_geo"], Point)

    def test_get_polygon_from_nodes_schema(self, test_context):

        df_ways = test_context.spark.createDataFrame(
            data=[],
            schema=StructType([
                StructField("id", StringType(), True),
                StructField("nodes", ArrayType(LongType(), True), True)
            ]))

        df_nodes = test_context.spark.createDataFrame(
            data=[],
            schema=StructType([
                StructField("id", StringType(), True),
                StructField("latitude", DoubleType(), True),
                StructField("longitude", DoubleType(), True),
            ]))

        df_res = get_polygon_from_nodes(
            df_way=df_ways,
            df_node=df_nodes,
            output_col="test_geo"
        )

        assert df_res.schema == StructType(
            [
                StructField('id', StringType(), True),
                StructField('test_geo', GeometryType(), True)
            ]
        )
