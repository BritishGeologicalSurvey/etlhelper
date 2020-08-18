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
from warnings import warn
import re


def namedtuple_row_factory(cursor):
    """Return output as a named tuple"""
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
    """Replace the default tuple output with a dict"""
    column_names = [d[0] for d in cursor.description]

    def create_row(row):
        row_dict = {}
        for i, column_name in enumerate(column_names):
            row_dict[column_name] = row[i]
        return row_dict

    return create_row


def _find_renamed_columns(row_class, column_names):
    regex = re.compile(r'^_\d+$')

    renamed_column_ids = [int(f.replace("_", "")) for f in row_class._fields if regex.match(f)]

    result = '\n'.join(f'{column_names[idx]} was renamed to {row_class._fields[idx]}'
                       for idx in renamed_column_ids)

    return result
