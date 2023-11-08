import pytest
from pyspark.sql.types import (ArrayType, BooleanType, ByteType, DoubleType,
                               IntegerType, LongType, MapType, StringType,
                               StructField, StructType, TimestampType, Row)

from osm_address.transform import explode_col


class TestExplode:

    def test_explode(self, test_context):

        df_res = explode_col(
            df_in=test_context.osm_data.relations,
            column="relations",
            explode_prefix="way_",
            position_column_name="the_position"
        )

        assert df_res.schema == StructType([
            StructField("id", LongType(), True),
            StructField("type", ByteType(), True),
            StructField("latitude", DoubleType(), True),
            StructField("longitude", DoubleType(), True),
            StructField("nodes", ArrayType(LongType(), True), True),
            StructField("the_position", IntegerType(), False),
            StructField("way_id", LongType(), True),
            StructField("way_relationType", ByteType(), True),
            StructField("way_role", StringType(), True),
            StructField("tags", MapType(StringType(), StringType(), True), True),
            StructField("info",
                        StructType([
                            StructField("version", IntegerType(), True),
                            StructField("timestamp", TimestampType(), True),
                            StructField("changeset", LongType(), True),
                            StructField("userId", IntegerType(), True),
                            StructField("userName", StringType(), True),
                            StructField("visible", BooleanType(), True)
                        ]), True)
        ])

    def test_explode_single_value(self, test_context):
        ways_data = [
            (1, [11, 12]),
            (2, [11, 12, 13]),
            (3, [11])
        ]

        df_ways = test_context.spark.createDataFrame(
            data=ways_data,
            schema=StructType([
                StructField("id", StringType(), True),
                StructField("nodes", ArrayType(LongType(), True), True)
            ]))

        df_res = explode_col(
            df_in=df_ways,
            column="nodes",
            explode_prefix="node_id",
            position_column_name="the_position"
        )

        assert df_res.schema == StructType([
            StructField('id', StringType(), True),
            StructField('the_position', IntegerType(), False),
            StructField('node_id', LongType(), True)
        ])

        assert df_res.collect() == [
            Row(id='1', the_position=0, node_id=11),
            Row(id='1', the_position=1, node_id=12),
            Row(id='2', the_position=0, node_id=11),
            Row(id='2', the_position=1, node_id=12),
            Row(id='2', the_position=2, node_id=13),
            Row(id='3', the_position=0, node_id=11)
        ]

    def test_explode_nonexistent_column(self, test_context):

        with pytest.raises(ValueError) as excinfo:

            explode_col(
                df_in=test_context.osm_data.relations,
                column="notexisting",
                explode_prefix="way_",
                position_column_name="the_position"
            )

        assert "is not in list of columns" in str(excinfo.value)

    def test_explode_nonarray_column(self, test_context):

        with pytest.raises(ValueError) as excinfo:

            explode_col(
                df_in=test_context.osm_data.relations,
                column="id",
                explode_prefix="way_",
                position_column_name="the_position"
            )

        assert "is not of type array" in str(excinfo.value)
