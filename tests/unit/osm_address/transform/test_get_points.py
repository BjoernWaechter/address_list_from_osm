from pyspark.sql.types import DoubleType, MapType, Row, StringType, StructField, StructType
from sedona.sql.types import GeometryType

from osm_address.transform.get_points import get_points_from_nodes_and_ways


class TestRawAddress:

    def test_get_raw_address(self, test_context):

        df_result = get_points_from_nodes_and_ways(
            osm_data=test_context.osm_data,
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
            }
        )

        assert df_result.count() == 156
        assert df_result.filter("id LIKE 'N%'").count() == 114
        assert df_result.filter("id LIKE 'W%'").count() == 42

        assert df_result.groupBy().max("longitude").collect()[0][0] < 1.7333881
        assert df_result.groupBy().min("longitude").collect()[0][0] > 1.4841775

        assert df_result.groupBy().max("latitude").collect()[0][0] < 42.618667
        assert df_result.groupBy().min("latitude").collect()[0][0] > 42.439056

        assert df_result.groupBy("city_raw").count().orderBy("city_raw").collect() == [
            Row(city_raw=None, count=77),
            Row(city_raw='Andorra', count=1),
            Row(city_raw='Andorra la Vella', count=45),
            Row(city_raw='Arinsal', count=3),
            Row(city_raw='El Serrat (Andorra)', count=1),
            Row(city_raw='Encamp', count=1),
            Row(city_raw='Erts', count=1),
            Row(city_raw='Escaldes-Engordany', count=8),
            Row(city_raw='Juberri', count=2),
            Row(city_raw='La Massana', count=9),
            Row(city_raw='Pas de La Casa', count=1),
            Row(city_raw='Pas de la Casa', count=3),
            Row(city_raw='Sant Julià de Lòria', count=2),
            Row(city_raw='Santa Coloma', count=2)
        ]
        assert df_result.groupBy("plz_raw").count().orderBy("plz_raw").collect() == [
            Row(plz_raw=None, count=73),
            Row(plz_raw='00500', count=2),
            Row(plz_raw='0AD 200', count=1),
            Row(plz_raw='100', count=1),
            Row(plz_raw='500', count=4),
            Row(plz_raw='600', count=1),
            Row(plz_raw='AD200', count=4),
            Row(plz_raw='AD300', count=1),
            Row(plz_raw='AD40', count=2),
            Row(plz_raw='AD400', count=6),
            Row(plz_raw='AD4000', count=3),
            Row(plz_raw='AD500', count=46),
            Row(plz_raw='AD600', count=4),
            Row(plz_raw='AD700', count=8)
        ]

        assert df_result.schema == StructType([
            StructField("street", StringType(), True),
            StructField("housenumber", StringType(), True),
            StructField("plz_raw", StringType(), True),
            StructField("city_raw", StringType(), True),
            StructField("id", StringType(), True),
            StructField("longitude", DoubleType(), True),
            StructField("latitude", DoubleType(), True),
            StructField("tags", MapType(StringType(), StringType(), True), True),
            StructField("address_point", GeometryType(), True)
            ])

    def test_get_raw_address_column_renamed(self, test_context):

        df_result = get_points_from_nodes_and_ways(
            osm_data=test_context.osm_data,
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
            point_column="test_point"
        )

        assert df_result.schema == StructType([
            StructField("street", StringType(), True),
            StructField("housenumber", StringType(), True),
            StructField("plz_raw", StringType(), True),
            StructField("city_raw", StringType(), True),
            StructField("id", StringType(), True),
            StructField("longitude", DoubleType(), True),
            StructField("latitude", DoubleType(), True),
            StructField("tags", MapType(StringType(), StringType(), True), True),
            StructField("test_point", GeometryType(), True)
            ])
