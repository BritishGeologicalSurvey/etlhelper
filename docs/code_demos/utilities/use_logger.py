import logging

import etlhelper as etl

etl.log_to_console()
etl_logger = logging.getLogger("etlhelper")
etl_logger.info("Hello world!")
