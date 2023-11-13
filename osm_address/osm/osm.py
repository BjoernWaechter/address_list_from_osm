from pyspark.sql import DataFrame
from dataclasses import dataclass


@dataclass
class OsmData:
    nodes: DataFrame
    ways: DataFrame
    relations: DataFrame
