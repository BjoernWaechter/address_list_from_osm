spark-submit.cmd --master spark://localhost:7077 ^
--packages com.acervera.osm4scala:osm4scala-spark3-shaded_2.12:1.0.11,org.apache.sedona:sedona-spark-shaded-3.0_2.12:1.5.0,org.datasyslab:geotools-wrapper:1.4.0-28.2 ^
--conf spark.executorEnv.PYTHONPATH=%CD% ^
--conf spark.executor.memory=40g ^
--conf spark.executor.cores=4 ^
create_german_addresses.py ^
-p file:///%CD%\data\bremen-latest.osm.pbf ^
-r file:///%CD%\result\bremen-latest