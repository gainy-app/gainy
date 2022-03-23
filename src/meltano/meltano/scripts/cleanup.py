import datetime
import logging
import os
import re
import psycopg2
from operator import itemgetter
from gainy.utils import db_connect

logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

schema_activity_min_datetime = datetime.datetime.now(
    tz=datetime.timezone.utc) - datetime.timedelta(days=7)


def clean_schemas(db_conn):
    with db_conn.cursor() as cursor:
        cursor.execute(
            "SELECT schema_name FROM deployment.public_schemas WHERE deleted_at is null"
        )
        schemas = map(itemgetter(0), cursor.fetchall())

        # we only select schemas starting with "public" and with some suffix
        schemas = filter(lambda x: re.match(r'^public.+', x), schemas)

        schemas = list(schemas)

        if len(schemas) == 0:
            return

        logger.info('Considering schemas %s', schemas)

        schema_last_activity_at = {}
        for schema in schemas:
            cursor.execute(
                f"SELECT last_activity_at FROM {schema}.deployment_metadata")
            last_activity_at = cursor.fetchone()[0]
            schema_last_activity_at[schema] = last_activity_at

        max_last_activity_at = max(schema_last_activity_at.values())

        for schema in schemas:
            last_activity_at = schema_last_activity_at[schema]
            if last_activity_at == max_last_activity_at:
                logger.info('Skipping schema %s: most recent schema', schema)
                continue
            if last_activity_at > schema_activity_min_datetime:
                logger.info('Skipping schema %s: too recent', schema)
                continue

            query = f"drop schema {schema} cascade"
            logger.warning(query)
            cursor.execute(query)

            cursor.execute(
                "update deployment.public_schemas set deleted_at = now() where schema_name = %(schema)s",
                {"schema": schema})


def clean_realtime_data(db_conn):
    with db_conn.cursor() as cursor:
        query = "delete from raw_data.eod_intraday_prices where time < now() - interval '2 weeks'"
        logger.warning(query)
        cursor.execute(query)
        query = "delete from deployment.realtime_listener_heartbeat where time < now() - interval '1 hour'"
        logger.warning(query)
        cursor.execute(query)


with db_connect() as db_conn:
    clean_schemas(db_conn)
    clean_realtime_data(db_conn)
