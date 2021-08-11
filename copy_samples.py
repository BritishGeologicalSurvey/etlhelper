# copy_samples.py
import datetime as dt
import json
import logging
import requests
from etlhelper import iter_rows
from etlhelper import logger as etl_logger
from db import ORACLE_DB

logger = logging.getLogger("copy_samples")

SELECT_SAMPLES = """
    SELECT CODE, DESCRIPTION
    FROM BGS.DIC_SEN_SENSOR
    WHERE date_updated BETWEEN :startdate AND :enddate
    ORDER BY date_updated
    """

BASE_URL = "http://localhost:9200/"
HEADERS = {'Content-Type': 'application/json'}


def copy_samples(startdate, enddate):
    """Read samples from Oracle and post to REST API."""
    logger.info("Copying samples with timestamps from %s to %s",
                startdate.isoformat(), enddate.isoformat())
    row_count = 0
    with ORACLE_DB.connect('ORACLE_PASSWORD') as conn:
        # Iterate over rows in memory-safe way.  Transform function converts
        # rows to nested dictionaries suitable for json.dumps().
        for item in iter_rows(SELECT_SAMPLES, conn,
                              parameters={"startdate": startdate,
                                          "enddate": enddate},
                              transform=transform_samples):
            # Post data to API
            logger.debug(item)
            response = requests.post(BASE_URL + 'samples/_doc', headers=HEADERS,
                                     data=json.dumps(item))
            # Check for failed rows
            try:
                response.raise_for_status()
                logger.debug("<%s>: %s\n", response.status_code, response.text)
            except requests.HTTPError:
                logger.error(response.json())
            # Log message for each 5000 rows processed
            row_count += 1
            if row_count % 5000 == 0:
                logger.info("%s items transferred", row_count)
    logger.info("Transfer complete")


def transform_samples(chunk):
    """Transform rows to dictionaries suitable for posting to API"""
    new_chunk = []
    for row in chunk:
        new_row = {
            'sample_code': row.CODE,
            'description': row.DESCRIPTION,
            'metadata': {
                'source': 'ORACLE_DB',  # fixed value
                'transferred_at': dt.datetime.now().isoformat()  # dynamic value
                }
            }
        logger.debug(new_row)
        new_chunk.append(new_row)
    return new_chunk


if __name__ == "__main__":
    # Configure logging
    handler = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s %(levelname)-8s %(message)s')
    handler.setFormatter(formatter)
    logger.setLevel(logging.INFO)
    logger.addHandler(handler)
    logger.propagate = False
    etl_logger.setLevel(logging.INFO)
    # Copy data from 00:00:00 yesterday to 00:00:00 today
    today = dt.datetime.combine(dt.date.today(), dt.time.min)
    yesterday = today - dt.timedelta(1)
    copy_samples(dt.datetime(2000, 1, 1), today)
