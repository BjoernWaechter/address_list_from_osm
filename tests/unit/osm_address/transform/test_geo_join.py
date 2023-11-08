import pytest
from pyspark.sql.types import StructField, StructType, StringType
from sedona.sql.types import GeometryType

from osm_address.transform import (create_border_polygons,
                                   join_point_in_multipolygon)


class TestGeoJoin:

    def test_join_point_in_poly_schema(self, test_context):

        df_polygon = create_border_polygons(
            osm_data=test_context.osm_data,
            relation_filter=
            "id=9407",  # Andorra
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
            relation_filter=
            "id=9407",  # Andorra
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
            relation_filter=
            "id=2800131",  # Parc Natural de l'Alt Pirineu
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
            relation_filter=
            "id=9407",  # Andorra
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
