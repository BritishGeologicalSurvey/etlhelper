"""
Row factories are functions that process row data as it comes from the database.
These are applied by the iter_rows function.

A row_factory function must:
  + accept a cursor object as an input
  + only use methods on the cursor that are described by DBAPI
  + return a function that takes a tuple

"""
from collections import namedtuple
from warnings import warn
import re


def namedtuple_row_factory(cursor):
    """
    Return function to convert output row to a named tuple.

    Named tuples allow access to attributes via both position (e.g. row[0]) or
    name (using dot notation, e.g. row.id).  They are immutable, so cannot be
    modified directly in transform functions.  Insert statements based on named
    tuples must use positional placeholders for parameters (e.g. ?, :1, %s).
    """
    column_names = [d[0] for d in cursor.description]

    try:
        Row = namedtuple('Row', field_names=column_names)
    except ValueError:
        Row = namedtuple('Row', field_names=column_names, rename=True)
        renamed_columns = _find_renamed_columns(Row, column_names)
        warn("One or more columns have been renamed. Names that cannot be "
             "converted to namedtuple attributes are replaced by indices. To "
             "prevent column renaming, either provide alias in SQL query, "
             "e.g. 'SELECT count(*) AS c', or use dict_row_factory. ")
        warn(f"{renamed_columns}")

    def create_row(row):
        return Row(*row)

    return create_row


def dict_row_factory(cursor):
    """
    Return function to convert output row to a dictionary.

    Dictionaries allow access to attributes via name (using key notation, e.g.
    row["id"].  They are mutable, so are convenient to modify directly in
    transform functions.  Insert statements based on dictionaries must use
    named placeholders for parameters (e.g. :id, %(id)s).
    """
    column_names = [d[0] for d in cursor.description]

    def create_row(row):
        row_dict = {}
        for i, column_name in enumerate(column_names):
            row_dict[column_name] = row[i]
        return row_dict

    return create_row


def tuple_row_factory(cursor):
    """
    Return function to convert output row to a tuple.

    Tuples allow access to attributes via position (e.g. row[0]).  They are
    immutable, so cannot be modified directly in transform functions.  Insert
    statements based on tuples must use positional placeholders for parameters
    (e.g. ?, :1, %s).

    As the DBAPI default is already to return rows as tuples, using the tuple
    row factory minimises processing overhead.
    """
    def create_row(row):
        return row

    return create_row


def list_row_factory(cursor):
    """
    Return function to convert output row to a list.

    Lists allow access to attributes via position (e.g. row[0]).  They are
    mutable, so are convient to modify directly in transform functions.  Insert
    statements based on lists must use positional placeholders for parameters
    (e.g. ?, :1, %s).
    """
    def create_row(row):
        return list(row)

    return create_row


def _find_renamed_columns(row_class, column_names):
    regex = re.compile(r'^_\d+$')

    renamed_column_ids = [int(f.replace("_", "")) for f in row_class._fields if regex.match(f)]

    result = '\n'.join(f'{column_names[idx]} was renamed to {row_class._fields[idx]}'
                       for idx in renamed_column_ids)

    return result
