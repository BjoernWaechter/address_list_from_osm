#!/bin/bash

VENV_PATH='/home/ec2-user/.virtualenvs/address_list_from_osm'
HDFS_BASE_PATH="hdfs:///user/$USER"

while [[ $# -gt 0 ]]; do
  case $1 in
    -p|--path)
      INPUT_PATH="$2"
      shift # past argument
      shift # past value
      ;;
    -r|--result)
      RESULT_PATH="$2"
      shift # past argument
      shift # past value
      ;;
    -*|--*)
      echo "Unknown option $1"
      exit 1
      ;;
    *)
      echo "Unknown argument $1"
      exit 1
      ;;
  esac
done

base_osm_name=$(basename ${INPUT_PATH})

rm -f /tmp/${base_osm_name}
wget ${INPUT_PATH} -O /tmp/${base_osm_name}
hadoop fs -put -f /tmp/${base_osm_name} ${HDFS_BASE_PATH}/${base_osm_name}
rm -f /tmp/${base_osm_name}

rm -f /tmp/venv_address_list_from_osm.tar.gz
venv-pack -o /tmp/venv_address_list_from_osm.tar.gz -p $VENV_PATH

archives="/tmp/venv_address_list_from_osm.tar.gz#environment"

spark-submit \
   --conf spark.yarn.appMasterEnv.PYSPARK_PYTHON=./environment/bin/python3 \
   --conf spark.yarn.appMasterEnv.PYTHONPATH=. \
   --master yarn \
   --deploy-mode cluster \
   --archives ${archives} \
   --packages com.acervera.osm4scala:osm4scala-spark3-shaded_2.12:1.0.11,org.apache.sedona:sedona-spark-shaded-3.0_2.12:1.5.0,org.datasyslab:geotools-wrapper:1.4.0-28.2 \
   create_german_addresses.py \
   -p "${HDFS_BASE_PATH}/${base_osm_name}" \
   -r "${HDFS_BASE_PATH}/result/${base_osm_name}"
