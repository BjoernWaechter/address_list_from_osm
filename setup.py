from setuptools import find_packages, setup

setup(
   name='osm_address',
   version='1.0',
   description='Module to handle OSM data for address extraction',
   author='Björn Wächter',
   author_email='bjoern@waechterei.de',
   packages=find_packages(include=['osm_address', 'osm_address.*']),
   install_requires=[
      'apache-sedona>=1.5.0',
      'pyarrow==14.0.1',
      'pyspark==3.3.1',
      'shapely==2.0.2'
   ]
)
