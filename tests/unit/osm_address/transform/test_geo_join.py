import pytest
from pyspark.sql.functions import expr
from pyspark.sql.types import StructField, StructType, StringType, Row
from sedona.sql.types import GeometryType

from osm_address.transform import (create_border_polygons,
                                   join_point_in_multipolygon)
from osm_address.transform.geo_join import join_nearest_geometry
from osm_address.transform.get_points import get_points_from_nodes_and_ways


class TestGeoJoin:

    def test_join_point_in_poly_schema(self, test_context):

        df_polygon = create_border_polygons(
            osm_data=test_context.osm_data,
            relation_filter="id=9407",  # Andorra
            additional_columns={
                "region": "IFNULL(element_at(tags, 'name:de'), element_at(tags, 'name'))"
            },
            add_debug_columns=False,
            result_polygon_name="multi_poly"
        )

        df_point = test_context.osm_data.nodes.filter(
            "id = 625257"  # zebra crossing in Andorra
        ).selectExpr(
            "ST_Point(CAST(longitude AS Decimal(24,20)), CAST(latitude AS Decimal(24,20))) as point"
        )

        df_res = join_point_in_multipolygon(
            df_in=df_point,
            df_polygon=df_polygon,
            point_column="point",
            polygon_column="multi_poly",
            partition_count=2
        )

        assert df_res.schema == StructType([
            StructField('region', StringType(), True),
            StructField('multi_poly', GeometryType(), True),
            StructField('point', GeometryType(), True)]
        )

    def test_join_point_in_multipoly(self, test_context):

        df_polygon = create_border_polygons(
            osm_data=test_context.osm_data,
            relation_filter="id=9407",  # Andorra
            additional_columns={
                "region": "IFNULL(element_at(tags, 'name:de'), element_at(tags, 'name'))"
            },
            add_debug_columns=True
        )

        df_point = test_context.osm_data.nodes.filter(
            "id = 625257"  # zebra crossing in Andorra
        ).selectExpr(
            "ST_Point(CAST(longitude AS Decimal(24,20)), CAST(latitude AS Decimal(24,20))) as point"
        )

        df_res = join_point_in_multipolygon(
            df_in=df_point,
            df_polygon=df_polygon,
            point_column="point",
            polygon_column="multipolygon",
            partition_count=2
        )

        assert df_res.count() == 1

    def test_join_point_in_inner_poly(self, test_context):

        df_polygon = create_border_polygons(
            osm_data=test_context.osm_data,
            relation_filter="id=2800131",  # Parc Natural de l'Alt Pirineu
            additional_columns={
                "region": "IFNULL(element_at(tags, 'name:de'), element_at(tags, 'name'))"
            },
            add_debug_columns=True
        )

        df_point = test_context.osm_data.nodes.filter(
            "id IN (52613712, 369943997)"  # Two points in inner polygons
        ).selectExpr(
            "id as point_id",
            "ST_Point(CAST(longitude AS Decimal(24,20)), CAST(latitude AS Decimal(24,20))) as point"
        )

        df_res = join_point_in_multipolygon(
            df_in=df_point,
            df_polygon=df_polygon,
            point_column="point",
            polygon_column="multipolygon",
            partition_count=2
        )

        assert df_res.count() == 0

    def test_join_point_in_poly_missing_column(self, test_context):

        df_polygon = create_border_polygons(
            osm_data=test_context.osm_data,
            relation_filter="id=9407",  # Andorra
            additional_columns={
                "region": "IFNULL(element_at(tags, 'name:de'), element_at(tags, 'name'))"
            },
            add_debug_columns=False,
            result_polygon_name="multi_poly"
        )

        df_point = test_context.osm_data.nodes.filter(
            "id = 625257"  # zebra crossing in Andorra
        ).selectExpr(
            "ST_Point(CAST(longitude AS Decimal(24,20)), CAST(latitude AS Decimal(24,20))) as point",
            "tags"
        )

        with pytest.raises(AssertionError) as excinfo:
            join_point_in_multipolygon(
                df_in=df_point,
                df_polygon=df_polygon,
                point_column="test_point",
                polygon_column="multi_poly",
                partition_count=2
            )
        assert "'test_point' not in data frame df_in" in str(excinfo)

        with pytest.raises(AssertionError) as excinfo:
            join_point_in_multipolygon(
                df_in=df_point,
                df_polygon=df_polygon,
                point_column="point",
                polygon_column="test_poly",
                partition_count=2
            )
        assert "'test_poly' not in data frame df_polygon" in str(excinfo)

        with pytest.raises(AssertionError) as excinfo:
            join_point_in_multipolygon(
                df_in=df_point,
                df_polygon=df_polygon,
                point_column="tags",
                polygon_column="multi_poly",
                partition_count=2
            )
        assert "'tags' in data frame df_in is of type" in str(excinfo)

        with pytest.raises(AssertionError) as excinfo:
            join_point_in_multipolygon(
                df_in=df_point,
                df_polygon=df_polygon,
                point_column="point",
                polygon_column="region",
                partition_count=2
            )
        assert "'region' in data frame df_polygon is of type" in str(excinfo)

    def test_nearest_point_to_point(self, test_context):

        df_hospital = get_points_from_nodes_and_ways(
            osm_data=test_context.osm_data,
            osm_filter="element_at(tags, 'amenity') = 'hospital'",
            centroid_column="hospital_point",
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

        df_join = join_nearest_geometry(
            df_in_1=df_hospital,
            df_in_2=df_pharmacy,
            column_name_1="hospital_point",
            column_name_2="pharmacy_point",
            partition_count=4,
            distance_meter_column="distance",
            max_meter=6000,
            join_type="leftouter"
        ).withColumn(
            "distance_rounded",
            expr("ROUND(distance)")
        ).orderBy(
            "hospital_id"
        ).select(
            "hospital_id",
            "pharmacy_id",
            "distance_rounded"
        )

        assert df_join.collect() == [
            Row(hospital_id='N2050364490', pharmacy_id='N2050466659', distance_rounded=812.0),
            Row(hospital_id='N4942543822', pharmacy_id='N690708548', distance_rounded=441.0),
            Row(hospital_id='N522787974', pharmacy_id='N590463655', distance_rounded=1167.0),
            Row(hospital_id='N5262330928', pharmacy_id='N4984739294', distance_rounded=1626.0),
            Row(hospital_id='N666793601', pharmacy_id='N4984739294', distance_rounded=5434.0),
            Row(hospital_id='N666793602', pharmacy_id='N4984739294', distance_rounded=4127.0),
            Row(hospital_id='N666793607', pharmacy_id='N690708520', distance_rounded=5929.0),
            Row(hospital_id='N666793610', pharmacy_id=None, distance_rounded=None),
            Row(hospital_id='W194554955', pharmacy_id='N5723978577', distance_rounded=310.0)
        ]

    def test_nearest_point_to_polygon(self, test_context):

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

        df_join = join_nearest_geometry(
            df_in_1=df_hospital,
            df_in_2=df_pharmacy,
            column_name_1="hospital_geom",
            column_name_2="pharmacy_point",
            partition_count=4,
            distance_meter_column="distance",
            max_meter=6000,
            join_type="leftouter"
        ).withColumn(
            "distance_rounded",
            expr("ROUND(distance)")
        ).orderBy(
            "hospital_id"
        ).select(
            "hospital_id",
            "pharmacy_id",
            "distance_rounded"
        )

        assert df_join.collect() == [
            Row(hospital_id='N2050364490', pharmacy_id='N2050466659', distance_rounded=812.0),
            Row(hospital_id='N4942543822', pharmacy_id='N690708548', distance_rounded=441.0),
            Row(hospital_id='N522787974', pharmacy_id='N590463655', distance_rounded=1167.0),
            Row(hospital_id='N5262330928', pharmacy_id='N4984739294', distance_rounded=1626.0),
            Row(hospital_id='N666793601', pharmacy_id='N4984739294', distance_rounded=5434.0),
            Row(hospital_id='N666793602', pharmacy_id='N4984739294', distance_rounded=4127.0),
            Row(hospital_id='N666793607', pharmacy_id='N690708520', distance_rounded=5929.0),
            Row(hospital_id='N666793610', pharmacy_id=None, distance_rounded=None),
            Row(hospital_id='W194554955', pharmacy_id='N5723978577', distance_rounded=261.0)
        ]

    def test_nearest_point_to_polygon_with_closest1(self, geo_join_context):

        df_join = join_nearest_geometry(
            df_in_1=geo_join_context["df_hospital"],
            df_in_2=geo_join_context["df_pharmacy"],
            column_name_1="hospital_geom",
            column_name_2="pharmacy_point",
            partition_count=4,
            distance_meter_column="distance",
            max_meter=6000,
            join_type="leftouter",
            closest_point_column_1="hospital_close_point"
        ).withColumn(
            "distance_rounded",
            expr("ROUND(distance)")
        ).orderBy(
            "hospital_id"
        ).selectExpr(
            "hospital_id",
            "pharmacy_id",
            "distance_rounded",
            "ST_AsText(ST_ReducePrecision(hospital_close_point,2)) AS hospital_close_point"
        )

        assert df_join.collect() == [
            Row(hospital_id='N2050364490', pharmacy_id='N2050466659', distance_rounded=812.0,
                hospital_close_point='POINT (1.49 42.47)'),
            Row(hospital_id='N4942543822', pharmacy_id='N690708548', distance_rounded=441.0,
                hospital_close_point='POINT (1.73 42.55)'),
            Row(hospital_id='N522787974', pharmacy_id='N590463655', distance_rounded=1167.0,
                hospital_close_point='POINT (1.47 42.57)'),
            Row(hospital_id='N5262330928', pharmacy_id='N4984739294', distance_rounded=1626.0,
                hospital_close_point='POINT (1.57 42.55)'),
            Row(hospital_id='N666793601', pharmacy_id='N4984739294', distance_rounded=5434.0,
                hospital_close_point='POINT (1.65 42.55)'),
            Row(hospital_id='N666793602', pharmacy_id='N4984739294', distance_rounded=4127.0,
                hospital_close_point='POINT (1.63 42.55)'),
            Row(hospital_id='N666793607', pharmacy_id='N690708520', distance_rounded=5929.0,
                hospital_close_point='POINT (1.66 42.54)'),
            Row(hospital_id='N666793610', pharmacy_id=None, distance_rounded=None,
                hospital_close_point=None),
            Row(hospital_id='W194554955', pharmacy_id='N5723978577', distance_rounded=261.0,
                hospital_close_point='POINT (1.53 42.51)')
        ]

    def test_nearest_point_to_polygon_with_closest2(self, geo_join_context):
        df_join = join_nearest_geometry(
            df_in_1=geo_join_context["df_hospital"],
            df_in_2=geo_join_context["df_pharmacy"],
            column_name_1="hospital_geom",
            column_name_2="pharmacy_point",
            partition_count=4,
            distance_meter_column="distance",
            max_meter=6000,
            join_type="leftouter",
            closest_point_column_2="pharmacy_close_point"
        ).withColumn(
            "distance_rounded",
            expr("ROUND(distance)")
        ).orderBy(
            "hospital_id"
        ).selectExpr(
            "hospital_id",
            "pharmacy_id",
            "distance_rounded",
            "ST_AsText(ST_ReducePrecision(pharmacy_close_point,2)) AS pharmacy_close_point"
        )

        assert df_join.collect() == [
            Row(hospital_id='N2050364490', pharmacy_id='N2050466659', distance_rounded=812.0,
                pharmacy_close_point='POINT (1.49 42.46)'),
            Row(hospital_id='N4942543822', pharmacy_id='N690708548', distance_rounded=441.0,
                pharmacy_close_point='POINT (1.73 42.54)'),
            Row(hospital_id='N522787974', pharmacy_id='N590463655', distance_rounded=1167.0,
                pharmacy_close_point='POINT (1.48 42.57)'),
            Row(hospital_id='N5262330928', pharmacy_id='N4984739294', distance_rounded=1626.0,
                pharmacy_close_point='POINT (1.58 42.54)'),
            Row(hospital_id='N666793601', pharmacy_id='N4984739294', distance_rounded=5434.0,
                pharmacy_close_point='POINT (1.58 42.54)'),
            Row(hospital_id='N666793602', pharmacy_id='N4984739294', distance_rounded=4127.0,
                pharmacy_close_point='POINT (1.58 42.54)'),
            Row(hospital_id='N666793607', pharmacy_id='N690708520', distance_rounded=5929.0,
                pharmacy_close_point='POINT (1.73 42.54)'),
            Row(hospital_id='N666793610', pharmacy_id=None, distance_rounded=None, pharmacy_close_point=None),
            Row(hospital_id='W194554955', pharmacy_id='N5723978577', distance_rounded=261.0,
                pharmacy_close_point='POINT (1.53 42.51)')
        ]

    def test_nearest_point_to_polygon_with_closest1and2(self, geo_join_context):
        df_join = join_nearest_geometry(
            df_in_1=geo_join_context["df_hospital"],
            df_in_2=geo_join_context["df_pharmacy"],
            column_name_1="hospital_geom",
            column_name_2="pharmacy_point",
            partition_count=4,
            distance_meter_column="distance",
            max_meter=6000,
            join_type="leftouter",
            closest_point_column_1="hospital_close_point",
            closest_point_column_2="pharmacy_close_point"
        ).withColumn(
            "distance_rounded",
            expr("ROUND(distance)")
        ).orderBy(
            "hospital_id"
        ).selectExpr(
            "hospital_id",
            "pharmacy_id",
            "distance_rounded",
            "ST_AsText(ST_ReducePrecision(hospital_close_point,2)) AS hospital_close_point",
            "ST_AsText(ST_ReducePrecision(pharmacy_close_point,2)) AS pharmacy_close_point"
        )

        assert df_join.collect() == [
            Row(hospital_id='N2050364490', pharmacy_id='N2050466659', distance_rounded=812.0,
                hospital_close_point='POINT (1.49 42.47)', pharmacy_close_point='POINT (1.49 42.46)'),
            Row(hospital_id='N4942543822', pharmacy_id='N690708548', distance_rounded=441.0,
                hospital_close_point='POINT (1.73 42.55)', pharmacy_close_point='POINT (1.73 42.54)'),
            Row(hospital_id='N522787974', pharmacy_id='N590463655', distance_rounded=1167.0,
                hospital_close_point='POINT (1.47 42.57)', pharmacy_close_point='POINT (1.48 42.57)'),
            Row(hospital_id='N5262330928', pharmacy_id='N4984739294', distance_rounded=1626.0,
                hospital_close_point='POINT (1.57 42.55)', pharmacy_close_point='POINT (1.58 42.54)'),
            Row(hospital_id='N666793601', pharmacy_id='N4984739294', distance_rounded=5434.0,
                hospital_close_point='POINT (1.65 42.55)', pharmacy_close_point='POINT (1.58 42.54)'),
            Row(hospital_id='N666793602', pharmacy_id='N4984739294', distance_rounded=4127.0,
                hospital_close_point='POINT (1.63 42.55)', pharmacy_close_point='POINT (1.58 42.54)'),
            Row(hospital_id='N666793607', pharmacy_id='N690708520', distance_rounded=5929.0,
                hospital_close_point='POINT (1.66 42.54)', pharmacy_close_point='POINT (1.73 42.54)'),
            Row(hospital_id='N666793610', pharmacy_id=None, distance_rounded=None, hospital_close_point=None,
                pharmacy_close_point=None),
            Row(hospital_id='W194554955', pharmacy_id='N5723978577', distance_rounded=261.0,
                hospital_close_point='POINT (1.53 42.51)', pharmacy_close_point='POINT (1.53 42.51)')
        ]
