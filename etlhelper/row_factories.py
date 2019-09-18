"""
Functions that process row data as it comes from the database.  These are
applied by the iter_rows function because the implementations for different
database engines are different.

A row_factory function must:
  + accept a cursor object as an input
  + only use methods on the cursor that are described by DBAPI
  + return a function that takes a tuple

"""
from collections import namedtuple


def namedtuple_rowfactory(cursor):
    """Return output as a named tuple"""
    column_names = [d[0] for d in cursor.description]

    Row = namedtuple('Row', field_names=column_names)

    def create_row(row):
        return Row(*row)

    return create_row


def dict_rowfactory(cursor):
    """Replace the default tuple output with a dict"""
    column_names = [d[0] for d in cursor.description]

    def create_row(row):
        row_dict = {}
        for i, column_name in enumerate(column_names):
            row_dict[column_name] = row[i]
        return row_dict

    return create_row
