# address_list_from_osm
![Coverage Status](coverage/coverage.svg)  
Create a list of residential addresses from OpenStreetMap data

## Development
It's possible to develop and run all tests on a windows machine, but for productive runs a linux based cluster is recommended.

## Prepare environment
### Cluster
Create a cluster with the following setup:
- Spark 3.3
- Python 3.10

An easy approach is to start an AWS EMR cluster with version emr-6.11.1 and use the `bootstrap.sh` script to setup python 3.10.  
Spark versions above 3.3. are not yet supported because of a dependent library.

### Virtual Environment
Since the project uses custom UDFs, an virtual environment is required to ship the code to all workers.  
Script `prepare_pyspark_venv.py` creates an environment with all required packages and also includes the projects code as a package into the virtual environment.
```
python prepare_pyspark_venv.py
```

## Run Tests
In order to run the tests the virtual environment needs to be activated:
```
. ~/.virtualenvs/address_list_from_osm/bin/activate
```
To run unit tests execute:
```
pytest
```
This executes all tests in `tests/unit`.  
To run integration tests run:
```
pytest tests/integration
```

## Compute address list
An example for creating an address list for Germany is included in the project: `create_german_addresses.py`.  
A cluster with hdfs storage is needed for this example.   
It can be executed using the start script `create_german_addresses_linux_yarn.sh`. It requires a link to an `osm.pbf` file as input.  
Data for different regions of the world can be found here: https://download.geofabrik.de/.  
An example execution for Berlin:
```
chmod +x create_german_addresses_linux_yarn.sh
deactivate #the script requires the virtual environment to be inactive
./create_german_addresses_linux_yarn.sh -p https://download.geofabrik.de/europe/germany/berlin-latest.osm.pbf
```
