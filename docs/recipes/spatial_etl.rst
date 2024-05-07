Spatial ETL
^^^^^^^^^^^

No specific drivers are required for spatial data if they are
transferred as Well Known Text.

.. code:: python

   select_sql_oracle = """
       SELECT
         id,
         SDO_UTIL.TO_WKTGEOMETRY(geom)
       FROM src
       """

   insert_sql_postgis = """
       INSERT INTO dest (id, geom) VALUES (
         %s,
         ST_Transform(ST_GeomFromText(%s, 4326), 27700)
       )
       """

Other spatial operations e.g. coordinate transforms, intersections and
buffering can be carried out in the SQL. Transform functions can
manipulate geometries using the
`Shapely <https://pypi.org/project/Shapely/>`__ library.
