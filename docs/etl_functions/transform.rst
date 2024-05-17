.. _transform:

Transform
^^^^^^^^^

ETL Helper functions accept a function as the ``transform`` keyword argument,
which enables transformation of data in flight.

This is any Python callable (e.g. function or class) that takes an iterator
and returns another iterator (e.g. list or generator via the ``yield``
statement).
Transform functions are applied to data as they are read
from the database (in the case of data fetching functions and
``copy_rows``), or before they are passed as query parameters (to
``executemany`` or ``load``).
When used with ``copy_rows`` or ``executemany`` the INSERT query must contain
the correct parameter placeholders for the transformed result.

The simplest transform functions modify data returned mutable row
factories e.g., ``dict_row_factory`` in-place. The ``yield`` keyword
makes ``my_transform`` a generator function that returns an ``Iterator``
that can loop over the rows.

.. code:: python

   from typing import Iterator
   import etlhelper as etl
   from etlhelper.row_factories import dict_row_factory


   def my_transform(chunk: Iterator[dict]) -> Iterator[dict]:
       # Add prefix to id, remove newlines, set lower case email addresses

       for row in chunk:  # each row is a dictionary (mutable)
           row['id'] += 1000
           row['description'] = row['description'].replace('\n', ' ')
           row['email'] = row['email'].lower()
           yield row


   etl.fetchall(select_sql, src_conn, row_factory=dict_row_factory,
                transform=my_transform)

It is also possible to assemble the complete transformed chunk and
return it.
This code demonstrates that the returned chunk can have a
different number of rows, and be of different length, to the input.
Because ``namedtuple``\ s are immutable, we have to create a ``new_row``
from each input ``row``.

.. code:: python

   import random
   from typing import Iterator
   import etlhelper as etl
   from etlhelper.row_factories import namedtuple_row_factory


   def my_transform(chunk: Iterator[tuple]) -> list[tuple]:
       # Append random integer (1-10), filter if <5.

       new_chunk = []
       for row in chunk:  # each row is a namedtuple (immutable)
           extra_value = random.randrange(10)
           if extra_value >= 5:  # some rows are dropped
               new_row = (*row, extra_value)  # new rows have extra column
               new_chunk.append(new_row)

       return new_chunk

   etl.fetchall(select_sql, src_conn, row_factory=namedtuple_row_factory,
                transform=my_transform)

Any Python code can be used within the function and extra data can
result from a calculation, a call to a webservice or a query against
another database.
As a standalone function with known inputs and
outputs, the transform functions are also easy to test.

The ``iter_chunks`` and ``iter_rows`` functions return generators.
Each chunk or row of data is only accessed when it is
required. This allows data transformation to be performed via
`memory-efficient
iterator-chains <https://dbader.org/blog/python-iterator-chains>`__.
