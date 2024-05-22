Database to API / NoSQL copy ETL script template
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

``etlhelper`` can be combined with Python’s
`aiohttp <https://docs.aiohttp.org/en/stable/>`__ library to create an
ETL for posting data from a database into an HTTP API. The API could be
a NoSQL document store (e.g. ElasticSearch, Cassandra) or some other web
service.

This example transfers data from Oracle to ElasticSearch. It uses
``iter_chunks`` to fetch data from the database without loading it all
into memory at once. A custom transform function creates a dictionary
structure from each row of data. This is “dumped” into JSON and posted
to the API via ``aiohttp``.

``aiohttp`` allows the records in each chunk to be posted to the API
asynchronously. The API is often the bottleneck in such pipelines and we
have seen significant speed increases (e.g. 10x) using asynchronous
transfer as opposed to posting records in series.

.. literalinclude:: ../code_demos/recipes/database_to_api.py
   :language: python

In this example, failed rows will fail the whole job. Removing the
``raise_for_status()`` call will let them just be logged instead.
