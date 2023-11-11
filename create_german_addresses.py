import argparse
import logging
import os
import time

import pyspark.sql.functions as F
from sedona.spark import SedonaContext

from osm_address.osm.osm_pbf_2_parquet import osm_pbf_2_parquet
from osm_address.transform import (create_border_polygons,
                                   join_point_in_multipolygon,
                                   remove_duplicate_rows)
from osm_address.transform.get_points import get_points_from_nodes_and_ways
from osm_address.udf import udf_explode_housenumber

log = logging.getLogger(__name__)

start_time = time.time()


class GermanAddresses:

    def __init__(self):

        config = SedonaContext.builder(). \
            config("spark.kryoserializer.buffer.max", "1024m"). \
            getOrCreate()

        self.spark = SedonaContext.create(config)

    def launch(
            self,
            input_path,
            result_path
    ):

        osm_data = osm_pbf_2_parquet(
            spark=self.spark,
            osm_pbf_path=input_path
        )

        # Postal code - German Postleitzahl
        df_plz = create_border_polygons(
            osm_data=osm_data,
            relation_filter="element_at(tags,'type') = 'boundary' AND element_at(tags,'boundary') = 'postal_code'",
            additional_columns={
                "plz": "element_at(tags, 'postal_code')"
            }
        )

        # Cities - Städte
        df_city = create_border_polygons(
            osm_data=osm_data,
            relation_filter=(
                "element_at(tags, 'boundary') = 'administrative' AND "
                "element_at(tags, 'admin_level') IN (8,6) OR "
                "(  element_at(tags, 'admin_level') = 4 AND "
                "   element_at(tags, 'name') IN ('Berlin','Hamburg', 'Bremen') )"
            ),
            additional_columns={
                "city": "IFNULL(element_at(tags, 'name:de'), element_at(tags, 'name'))",
                "admin_level": "element_at(tags, 'admin_level')"
            }
        )

        df_raw_address = get_points_from_nodes_and_ways(
            osm_data=osm_data,
            osm_filter=(
                "(element_at(tags, 'addr:housenumber') IS NOT NULL OR "
                "element_at(tags, 'addr:nohousenumber') = 'yes') AND "
                "(element_at(tags, 'addr:street') IS NOT NULL OR  "
                " element_at(tags, 'addr:place') IS NOT NULL)  "
            ),
            additional_columns={
                "street": "IFNULL(element_at(tags, 'addr:street'), element_at(tags, 'addr:place') )",
                "housenumber": "element_at(tags, 'addr:housenumber')",
                "plz_raw": "element_at(tags, 'addr:postcode')",
                "city_raw": "element_at(tags, 'addr:city')"
            },
            id_column="id"
        )

        df_temp = join_point_in_multipolygon(
            df_in=df_raw_address,
            point_column="address_point",
            df_polygon=df_plz,
            polygon_column="multipolygon"
        )

        df_address_with_plz = df_temp.selectExpr(
            "id",
            "street",
            "housenumber",
            "IFNULL(plz_raw, plz) plz",
            "latitude",
            "longitude",
            "address_point"
        )

        df_address_with_plz_and_city_with_duplicates = join_point_in_multipolygon(
            df_in=df_address_with_plz,
            point_column="address_point",
            df_polygon=df_city,
            polygon_column="multipolygon"
        ).selectExpr(
            "id",
            "street",
            "housenumber",
            "plz",
            "city",
            "latitude",
            "longitude",
            "admin_level"
        )

        df_address_with_plz_and_city = remove_duplicate_rows(
            df_input=df_address_with_plz_and_city_with_duplicates,
            unique_col="id",
            decision_col="admin_level",
            decision_max_first=True
        ).drop(
            "admin_level",
            "id"
        )

        df_result = df_address_with_plz_and_city.withColumn(
            "hnr_array",
            udf_explode_housenumber(F.lower(df_address_with_plz_and_city.housenumber))
        ).withColumnRenamed(
            "housenumber",
            "org_housenumber"
        ).selectExpr(
            "*",
            f"explode(hnr_array) as housenumber"
        ).drop(
            "hnr_array",
            "org_housenumber"
        )

        df_result = df_result.withColumn("housenumber", F.lower(F.col("housenumber")))

        res_cols = df_result.columns
        res_cols.remove("longitude")
        res_cols.remove("latitude")

        df_result_unified = df_result.groupBy(
            res_cols
        ).agg(
            F.avg("longitude").alias("longitude"),
            F.avg("latitude").alias("latitude")
        ).orderBy(
            "plz",
            "street",
            "housenumber"
        )

        df_result_unified.coalesce(10).write.mode("overwrite").parquet(result_path)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        prog='creates list of addresses from osm',
        description='')

    parser.add_argument('-p', '--path', required=True, help="hdfs path of the osm.pbf file")
    parser.add_argument('-r', '--result', help="hdfs path of the osm.pbf file")

    args = parser.parse_args()

    if args.result is None:
        args.result = f'result/{os.path.basename(args.path).split(".")[0]}'

    log.critical(f"Input path: {args.path}")
    log.critical(f"Output path: {args.result}")

    GermanAddresses().launch(
        input_path=args.path,
        result_path=args.result
    )
