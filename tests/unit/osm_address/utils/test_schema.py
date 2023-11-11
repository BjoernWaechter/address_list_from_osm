from pyspark.sql.types import StructType, StructField, StringType
import pytest
from osm_address.utils.schema import check_for_duplicate_columns


class TestSchema:

    def test_no_columns_in_common(self, test_context):
        schema1 = StructType([
            StructField('firstname', StringType(), True),
            StructField('middlename', StringType(), True),
            StructField('lastname', StringType(), True)
        ])

        df_test_1 = test_context.spark.createDataFrame(data=[], schema=schema1)

        schema2 = StructType([
            StructField('firstname2', StringType(), True),
            StructField('middlename2', StringType(), True),
            StructField('lastname2', StringType(), True)
        ])

        df_test_2 = test_context.spark.createDataFrame(data=[], schema=schema2)

        check_for_duplicate_columns(
            df_1=df_test_1,
            df_2=df_test_2
        )

    def test_columns_in_common(self, test_context):
        schema1 = StructType([
            StructField('firstname', StringType(), True),
            StructField('middlename', StringType(), True),
            StructField('lastname', StringType(), True)
        ])

        df_test_1 = test_context.spark.createDataFrame(data=[], schema=schema1)

        schema2 = StructType([
            StructField('firstname1', StringType(), True),
            StructField('middlename1', StringType(), True),
            StructField('lastname', StringType(), True)
        ])

        df_test_2 = test_context.spark.createDataFrame(data=[], schema=schema2)

        with pytest.raises(RuntimeError) as exc:

            check_for_duplicate_columns(
                df_1=df_test_1,
                df_2=df_test_2
            )
        assert '"lastname"' in str(exc)

    def test_columns_in_common_types(self, test_context):
        schema = StructType([
            StructField('firstname', StringType(), True),
            StructField('middlename', StringType(), True),
            StructField('lastname', StringType(), True)
        ])

        df_test = test_context.spark.createDataFrame(data=[], schema=schema)

        with pytest.raises(AssertionError) as exc:

            check_for_duplicate_columns(
                df_1=1,
                df_2=df_test
            )

        assert "df_1 is not a pyspark Dataframe" in str(exc)

        with pytest.raises(AssertionError) as exc:
            check_for_duplicate_columns(
                df_1=df_test,
                df_2="test"
            )

        assert "df_2 is not a pyspark Dataframe" in str(exc)

        with pytest.raises(AssertionError) as exc:
            check_for_duplicate_columns(
                df_1=None,
                df_2=df_test
            )

        assert "df_1 is not a pyspark Dataframe" in str(exc)

        with pytest.raises(AssertionError) as exc:
            check_for_duplicate_columns(
                df_1=df_test,
                df_2=None
            )

        assert "df_2 is not a pyspark Dataframe" in str(exc)
