import os
import tempfile

from create_german_addresses import GermanAddresses


class TestE2E:

    def test_bremen(self, test_context):
        bremen_file = f"{os.path.dirname(__file__)}{os.sep}..{os.sep}resources{os.sep}bremen-2023-11.osm.pbf"
        bremen_result_file = f"{os.path.dirname(__file__)}{os.sep}..{os.sep}resources{os.sep}bremen-result.parquet"

        with tempfile.TemporaryDirectory() as tmp_dir:

            result_dir = f"file:///{tmp_dir}"

            GermanAddresses().launch(
                input_path=f"file:///{bremen_file}",
                result_path=result_dir
            )

            df_result = test_context.spark.read.parquet(result_dir)
            df_result.cache()

            df_expected = test_context.spark.read.parquet(bremen_result_file)
            df_expected.cache()

            assert df_result.count() == 143614
            assert df_result.schema == df_expected.schema
            assert df_result.collect() == df_expected.collect()
