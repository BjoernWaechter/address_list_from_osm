from pyspark.sql.types import (IntegerType, Row, StringType, StructField,
                               StructType, ArrayType)

from osm_address.transform import (add_column_prefix, aggregate_column_to_list,
                                   wrap_columns_into_struct)


class TestColumn:

    def test_add_prefix(self, test_context):

        schema = StructType([
            StructField('firstname', StringType(), True),
            StructField('middlename', StringType(), True),
            StructField('lastname', StringType(), True)
        ])

        data = [
            ("Hans", "Muller", "Test")
        ]

        df_test = test_context.spark.createDataFrame(data=data, schema=schema)

        df_res = add_column_prefix(
            df_in=df_test,
            column_prefix="test_"
        )

        assert df_res.schema == StructType([
            StructField("test_firstname", StringType(), True),
            StructField("test_middlename", StringType(), True),
            StructField("test_lastname", StringType(), True)
            ]
        )

    def test_aggregate_column_to_list(self, test_context):
        nodes_data = [
            (1, 77),
            (1, 1),
            (1, 5),
            (2, 0),
        ]

        df_test = test_context.spark.createDataFrame(
            data=nodes_data,
            schema=StructType([
                StructField("id", IntegerType(), True),
                StructField("node_id", IntegerType(), True)
            ]))

        df_res = aggregate_column_to_list(
            df_in=df_test,
            group_by_columns=["id"],
            collect_column="node_id",
            result_column="nodes",
            sort_list=False
        ).orderBy("id")

        assert df_res.schema == StructType([
            StructField('id', IntegerType(), True),
            StructField('nodes', ArrayType(IntegerType(), False), False)
        ])

        df_res.cache()
        result = df_res.collect()

        assert df_res.count() == 2

        assert result[0]["id"] == 1
        assert sorted(result[0]["nodes"]) == [1, 5, 77]

        assert result[1]["id"] == 2
        assert sorted(result[1]["nodes"]) == [0]

    def test_aggregate_column_to_list_sorted(self, test_context):
        nodes_data = [
            (1, (2, 77)),
            (1, (1, 1)),
            (1, (0, 5)),
            (2, (7, 0)),
        ]

        df_test = test_context.spark.createDataFrame(
            data=nodes_data,
            schema=StructType([
                StructField("id", IntegerType(), True),
                StructField("node", StructType([
                    StructField("node_pos", IntegerType(), True),
                    StructField("node_id", IntegerType(), True)
                    ]))
            ]))

        df_res = aggregate_column_to_list(
            df_in=df_test,
            group_by_columns=["id"],
            collect_column="node",
            result_column="nodes",
            sort_list=True
        ).orderBy("id")

        assert df_res.schema == StructType([
            StructField('id', IntegerType(), True),
            StructField('nodes', ArrayType(StructType([
                StructField('node_pos', IntegerType(), True),
                StructField('node_id', IntegerType(), True)
            ]), False), False)
        ])

        df_res.cache()
        result = df_res.collect()

        assert df_res.count() == 2

        assert result[0]["id"] == 1
        assert sorted(result[0]["nodes"]) == [
            Row(node_pos=0, node_id=5),
            Row(node_pos=1, node_id=1),
            Row(node_pos=2, node_id=77)
        ]

        assert result[1]["id"] == 2
        assert sorted(result[1]["nodes"]) == [Row(node_pos=7, node_id=0)]

    def test_wrap_columns_into_struct(self, test_context):
        nodes_data = [
            (1, 1, "77"),
            (2, 99, "0"),
        ]

        df_test = test_context.spark.createDataFrame(
            data=nodes_data,
            schema=StructType([
                StructField("id", IntegerType(), True),
                StructField("node_pos", IntegerType(), True),
                StructField("node_id", StringType(), True)
            ]))

        df_result = wrap_columns_into_struct(
            df_in=df_test,
            columns=["node_pos", "node_id"],
            wrap_column="node"
        )

        assert df_result.schema == StructType([
            StructField("id", IntegerType(), True),
            StructField("node", StructType([
                StructField("node_pos", IntegerType(), True),
                StructField("node_id", StringType(), True)
            ]), False)])

        result = df_result.collect()

        assert result[0] == Row(id=1, node=Row(node_pos=1, node_id='77'))
        assert result[1] == Row(id=2, node=Row(node_pos=99, node_id='0'))
