import os

import pytest
from pyspark.sql.types import (ArrayType, BooleanType, ByteType, DoubleType,
                               IntegerType, LongType, MapType, StringType,
                               StructField, StructType, TimestampType)

from osm_address.osm import osm_pbf_2_parquet


class TestOsmPbf:
    def test_osm_pbf_spark_none(self, test_context):
        with pytest.raises(AssertionError):
            osm_pbf_2_parquet(
                spark=None,
                osm_pbf_path="None"
            )

    def test_osm_pbf_url_none(self, test_context):
        with pytest.raises(AssertionError):
            osm_pbf_2_parquet(
                spark=test_context,
                osm_pbf_path=None
            )

    def test_osm_pbf_read_data(self, test_context):
        resource_path = os.path.abspath(
            f"{os.path.dirname(__file__)}"
            f"{os.sep}..{os.sep}..{os.sep}..{os.sep}..{os.sep}tests{os.sep}resources{os.sep}test.osm.pbf"
        )

        res = osm_pbf_2_parquet(
            spark=test_context.spark,
            osm_pbf_path=f"file:///{resource_path}"
        )

        assert res.nodes.count() == 204727
        assert res.ways.count() == 9319
        assert res.relations.count() == 237

        schema = StructType(
            [
                StructField("id", LongType(), True),
                StructField("type", ByteType(), True),
                StructField("latitude", DoubleType(), True),
                StructField("longitude", DoubleType(), True),
                StructField("nodes", ArrayType(LongType(), True), True),
                StructField("relations",
                            ArrayType(StructType(
                                [
                                    StructField("id", LongType(), True),
                                    StructField("relationType", ByteType(), True),
                                    StructField("role", StringType(), True)
                                ]), True), True
                            ),
                StructField("tags", MapType(StringType(), StringType(), True), True),
                StructField("info",
                            StructType(
                                [
                                    StructField("version", IntegerType(), True),
                                    StructField("timestamp", TimestampType(), True),
                                    StructField("changeset", LongType(), True),
                                    StructField("userId", IntegerType(), True),
                                    StructField("userName", StringType(), True),
                                    StructField("visible", BooleanType(), True)
                                ]),
                            True)
            ]
        )

        assert res.nodes.schema == schema
        assert res.ways.schema == schema
        assert res.relations.schema == schema
