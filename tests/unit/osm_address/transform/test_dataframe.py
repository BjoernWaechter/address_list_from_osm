from pyspark.sql.types import (IntegerType, Row, StringType, StructField,
                               StructType)

from osm_address.transform import remove_duplicate_rows


class TestDataframe:

    def test_remove_duplicate_rows_max(self, test_context):
        data = [
            (1, "ag", 2),
            (1, "kas", 1),
            (2, "wg", 5),
            (3, "dsg", 8),
            (3, "je", 3),
            (3, "af", 13),
        ]

        df_data = test_context.spark.createDataFrame(
            data=data,
            schema=StructType([
                StructField("id", IntegerType(), False),
                StructField("str", StringType(), True),
                StructField("order_no", IntegerType(), False),
            ])
        )

        df_result = remove_duplicate_rows(
            df_input=df_data,
            unique_col="id",
            decision_col="order_no",
            decision_max_first=True
        ).orderBy("id")

        assert df_result.collect() == [
            Row(id=1, str='ag', order_no=2),
            Row(id=2, str='wg', order_no=5),
            Row(id=3, str='af', order_no=13)
        ]

        assert df_data.schema == df_result.schema

    def test_remove_duplicate_rows_min(self, test_context):
        data = [
            (1, "ag", 2),
            (1, "kas", 1),
            (2, "wg", 5),
            (3, "dsg", 8),
            (3, "je", 3),
            (3, "af", 13),
        ]

        df_data = test_context.spark.createDataFrame(
            data=data,
            schema=StructType([
                StructField("id", IntegerType(), False),
                StructField("str", StringType(), True),
                StructField("order_no", IntegerType(), False),
            ])
        )

        df_result = remove_duplicate_rows(
            df_input=df_data,
            unique_col="id",
            decision_col="order_no",
            decision_max_first=False
        ).orderBy("id")

        assert df_result.collect() == [
            Row(id=1, str='kas', order_no=1),
            Row(id=2, str='wg', order_no=5),
            Row(id=3, str='je', order_no=3)
        ]

    def test_remove_duplicate_rows_multiple_min(self, test_context):
        data = [
            (1, "ag", 1),
            (1, "kas", 1),
            (2, "wg", 5),
            (3, "dsg", 3),
            (3, "je", 3),
            (3, "af", 13),
        ]

        df_data = test_context.spark.createDataFrame(
            data=data,
            schema=StructType([
                StructField("id", IntegerType(), False),
                StructField("str", StringType(), True),
                StructField("order_no", IntegerType(), False),
            ])
        )

        df_result = remove_duplicate_rows(
            df_input=df_data,
            unique_col="id",
            decision_col="order_no",
            decision_max_first=False
        ).orderBy("id")

        assert df_result.count() == 3
