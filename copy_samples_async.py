# copy_samples_async.py
import asyncio
import datetime as dt
import json
import logging

import aiohttp
from etlhelper import iter_chunks
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
        data = iter_chunks(SELECT_SAMPLES, conn,
                           parameters={"startdate": startdate, "enddate": enddate},
                           transform=transform_samples)

        for chunk in data:
            result = asyncio.run(post_chunk(chunk))
            row_count += len(result)
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


async def post_chunk(chunk):
    """Post multiple items to API asynchronously."""
    tasks = []

    async with aiohttp.ClientSession() as session:
        for item in chunk:
            tasks.append(post_one(item, session))

        # Process all tasks in parallel.  An exception in any will be raised.
        result = await asyncio.gather(*tasks)

    return result


async def post_one(item, session):
    """Post a single item to API using existing aiohttp Session."""
    response = await session.post(BASE_URL + 'samples/_doc', headers=HEADERS,
                                  data=json.dumps(item))

    # Log responses before throwing errors because they aren't passed into the
    # generated Exceptions otherwise and cannot then be seen for debugging.
    if response.status >= 400:
        response_text = await response.text()
        logger.error('The following item failed: %s\nError message:\n(%s)',
                     item, response_text)
        await response.raise_for_status()

    return response.status


def test_many(item_count=20):
    items = [{'id': n} for n in range(item_count)]
    result = asyncio.run(post_chunk(items))
    print(result)


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
    #test_many()
