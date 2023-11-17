import os

import findspark as findspark
import pytest
from pyspark.sql import SparkSession
from sedona.spark import SedonaContext

from osm_address.osm import OsmData
from osm_address.transform.get_points import get_points_from_nodes_and_ways


class SparkTestContext:
    def __init__(self, spark: SparkSession, osm_data: OsmData):
        self.spark = spark
        self.osm_data = osm_data


@pytest.fixture(scope="session")
def test_context():
    findspark.init()

    config = (
        SedonaContext.builder()
        .master("local[2]")
        .appName("local-tests")
        .config("spark.executor.cores", "1")
        .config("spark.executor.instances", "1")
        .config("spark.sql.shuffle.partitions", "1")
        .config(
            'spark.jars.packages',
            'com.acervera.osm4scala:osm4scala-spark3-shaded_2.12:1.0.11,'
            'org.datasyslab:geotools-wrapper:1.4.0-28.2,'
            'org.apache.sedona:sedona-spark-shaded-3.0_2.12:1.5.0'
        )
        .getOrCreate()
    )

    spark = SedonaContext.create(config)

    raw_df = spark.read.format("osm.pbf").load(os.path.abspath(
            f"{os.path.dirname(__file__)}{os.sep}..{os.sep}tests{os.sep}resources{os.sep}test.osm.pbf"
    ))

    df_raw_node = raw_df.where("type = 0")
    df_raw_way = raw_df.where("type = 1")
    df_raw_relation = raw_df.where("type = 2")

    df_raw_node.cache()
    df_raw_way.cache()
    df_raw_relation.cache()

    yield SparkTestContext(spark, OsmData(nodes=df_raw_node, ways=df_raw_way, relations=df_raw_relation))
    spark.stop()


@pytest.fixture(scope="session")
def geo_join_context(test_context):

    df_hospital = get_points_from_nodes_and_ways(
        osm_data=test_context.osm_data,
        osm_filter="element_at(tags, 'amenity') = 'hospital'",
        geometry_column="hospital_geom",
        id_column="hospital_id"
    ).drop(
        "longitude",
        "latitude",
        "tags"
    )

    df_pharmacy = get_points_from_nodes_and_ways(
        osm_data=test_context.osm_data,
        osm_filter="element_at(tags, 'amenity') = 'pharmacy'",
        additional_columns={"hospital": "element_at(tags, 'name')"},
        centroid_column="pharmacy_point",
        id_column="pharmacy_id"
    ).drop(
        "longitude",
        "latitude",
        "tags"
    )

    yield {
        "test_context": test_context,
        "df_hospital": df_hospital,
        "df_pharmacy": df_pharmacy
    }
