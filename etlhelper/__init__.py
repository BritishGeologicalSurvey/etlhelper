"""
Library to simplify data transfer between databases
"""
import logging
import warnings

# Import helper functions here for more convenient access
# flake8: noqa
from etlhelper.abort import abort_etlhelper_threads
from etlhelper.db_params import DbParams
from etlhelper.etl import (
    copy_rows,
    dump_rows,
    execute,
    executemany,
    fetchone,
    fetchmany,
    fetchall,
    generate_insert_sql,
    get_rows,
    iter_chunks,
    iter_rows,
    load,
    copy_table_rows,
)
from etlhelper.connect import (
    connect,
    get_connection_string,
    get_sqlalchemy_connection_string,
)
from etlhelper.utils import (
    table_info,
)
from etlhelper import (
    row_factories,
)

from . import _version
__version__ = _version.get_versions()['version']

warnings.warn(
    (
        "ETLHelper version 1.0.0 is coming soon and will contain many breaking changes. "
        "Users should pin their versions '(etlhelper<1)' and check GitHub for release details."
    ),
    DeprecationWarning,
    stacklevel=2,
)

# Prepare log handler.  See this StackOverflow answer for details:
# https://stackoverflow.com/a/27835318/3508733
class CleanDebugMessageFormatter(logging.Formatter):
    default_fmt = logging.Formatter('%(asctime)s %(funcName)s: %(message)s')
    debug_fmt = logging.Formatter('%(message)s')

    def format(self, record):
        if record.levelno < logging.INFO:
            return self.debug_fmt.format(record)
        else:
            return self.default_fmt.format(record)

handler = logging.StreamHandler()
handler.setFormatter(CleanDebugMessageFormatter())

# Configure logger
logger = logging.getLogger('etlhelper')
logger.addHandler(handler)
logger.setLevel(level=logging.WARN)
