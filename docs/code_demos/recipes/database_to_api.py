"""ETL Helper script to demonstrate creating an API to a database using aiohttp."""
import asyncio
import json
import logging
import datetime as dt

import aiohttp
from etlhelper import iter_chunks

from db import ORACLE_DB

logger = logging.getLogger("copy_sensors_async")

SELECT_SENSORS = """
    SELECT CODE, DESCRIPTION
    FROM BGS.DIC_SEN_SENSOR
    WHERE date_updated BETWEEN :startdate AND :enddate
    ORDER BY date_updated
    """
BASE_URL = "http://localhost:9200/"
HEADERS = {"Content-Type": "application/json"}


def copy_sensors(startdate: dt.datetime, enddate: dt.datetime) -> None:
    """Read sensors from Oracle and post to REST API."""
    logger.info("Copying sensors with timestamps from %s to %s",
                startdate.isoformat(), enddate.isoformat())
    row_count = 0

    with ORACLE_DB.connect("ORACLE_PASSWORD") as conn:
        # chunks is a generator that yields lists of dictionaries
        chunks = iter_chunks(
            SELECT_SENSORS,
            conn,
            parameters={"startdate": startdate, "enddate": enddate},
            transform=transform_sensors,
        )

        for chunk in chunks:
            result = asyncio.run(post_chunk(chunk))
            row_count += len(result)
            logger.info("%s items transferred", row_count)

    logger.info("Transfer complete")


def transform_sensors(chunk: list[tuple]) -> list[tuple]:
    """Transform rows to dictionaries suitable for converting to JSON."""
    new_chunk = []

    for row in chunk:
        new_row = {
            "sample_code": row.CODE,
            "description": row.DESCRIPTION,
            "metadata": {
                "source": "ORACLE_DB",  # fixed value
                "transferred_at": dt.datetime.now().isoformat(),  # dynamic value
                }
            }
        logger.debug(new_row)
        new_chunk.append(new_row)

    return new_chunk


async def post_chunk(chunk: list[tuple]) -> list:
    """Post multiple items to API asynchronously."""
    async with aiohttp.ClientSession() as session:
        # Build list of tasks
        tasks = []
        for item in chunk:
            tasks.append(post_one(item, session))

        # Process tasks in parallel.  An exception in any will be raised.
        result = await asyncio.gather(*tasks)

    return result


async def post_one(item: tuple, session: aiohttp.ClientSession) -> int:
    """Post a single item to API using existing aiohttp Session."""
    # Post the item
    response = await session.post(
        BASE_URL + "sensors/_doc",
        headers=HEADERS,
        data=json.dumps(item),
    )

    # Log responses before throwing errors because error info is not included
    # in generated Exceptions and so cannot otherwise be seen for debugging.
    if response.status >= 400:
        response_text = await response.text()
        logger.error(
            "The following item failed: %s\nError message:\n(%s)",
            item,
            response_text,
        )
        await response.raise_for_status()

    return response.status


if __name__ == "__main__":
    # Configure logging
    handler = logging.StreamHandler()
    formatter = logging.Formatter("%(asctime)s %(levelname)-8s %(message)s")
    handler.setFormatter(formatter)
    logger.setLevel(logging.INFO)
    logger.addHandler(handler)

    # Copy data from 1 January 2000 to 00:00:00 today
    today = dt.datetime.combine(dt.date.today(), dt.time.min)
    copy_sensors(dt.datetime(2000, 1, 1), today)
