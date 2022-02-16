import datetime
import logging
import os
import re
import psycopg2
from operator import itemgetter

PG_ADDRESS = os.getenv("PG_ADDRESS")
PG_PORT = os.getenv("PG_PORT")
PG_USERNAME = os.getenv("PG_USERNAME")
PG_PASSWORD = os.getenv("PG_PASSWORD")
PG_DATABASE = os.getenv("PG_DATABASE")
DBT_TARGET_SCHEMA = os.getenv("DBT_TARGET_SCHEMA")

DB_CONN_STRING = f"postgresql://{PG_USERNAME}:{PG_PASSWORD}@{PG_ADDRESS}:{PG_PORT}/{PG_DATABASE}?options=-csearch_path%3D{DBT_TARGET_SCHEMA}"

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

            query = f"drop schema {schema}"
            logger.warning(query)
            cursor.execute(query)


def clean_realtime_data(db_conn):
    with db_conn.cursor() as cursor:
        query = "delete from raw_data.eod_intraday_prices where time < now() - interval '2 weeks'"
        logger.warning(query)
        cursor.execute(query)


with psycopg2.connect(DB_CONN_STRING) as db_conn:
    clean_schemas(db_conn)
    clean_realtime_data(db_conn)
