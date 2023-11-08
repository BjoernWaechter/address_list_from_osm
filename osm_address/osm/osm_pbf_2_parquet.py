import logging

from pyspark.sql import SparkSession

from osm_address.osm import OsmData

log = logging.getLogger(__name__)


def osm_pbf_2_parquet(
        spark: SparkSession,
        osm_pbf_path: str
):
    """

    Args:
        spark: spark session to use for reading the file from osm_pbf_path
        osm_pbf_path: path to read the osm.pbf file from. Can be a file or a folder

    Returns:
        OsmData: Return nodes, ways and relations in a dataclass

    """
    assert spark is not None, "spark cannot be None"
    assert osm_pbf_path is not None, "osm_pbf_hdfs_path cannot be None"

    raw_df = spark.read.format("osm.pbf").load(osm_pbf_path)

    df_raw_node = raw_df.where("type = 0")
    df_raw_way = raw_df.where("type = 1")
    df_raw_relation = raw_df.where("type = 2")

    return OsmData(df_raw_node, df_raw_way, df_raw_relation)
