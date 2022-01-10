import os
import datetime
import logging
import psycopg2
from psycopg2 import sql
from psycopg2.extras import execute_values
from datadog_api_client.v1 import ApiClient, Configuration
from datadog_api_client.v1.api.metrics_api import MetricsApi
from datadog_api_client.v1.model.metrics_payload import MetricsPayload
from datadog_api_client.v1.model.point import Point
from datadog_api_client.v1.model.series import Series

METRIC_REALTIME_PRICES_COUNT = 'app.realtime_prices_count'

PG_HOST = os.environ['PG_HOST']
PG_PORT = os.environ['PG_PORT']
PG_DBNAME = os.environ['PG_DBNAME']
PG_USERNAME = os.environ['PG_USERNAME']
PG_PASSWORD = os.environ['PG_PASSWORD']
PUBLIC_SCHEMA_NAME = os.environ['PUBLIC_SCHEMA_NAME']
ENV = os.environ['ENV']

DB_CONN_STRING = f"postgresql://{PG_USERNAME}:{PG_PASSWORD}@{PG_HOST}:{PG_PORT}/{PG_DBNAME}"

dd_configuration = Configuration()

def get_logger(name):
    logging.basicConfig()
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG if ENV == "local" else logging.INFO)

    return logger

def get_symbols():
    with psycopg2.connect(DB_CONN_STRING) as db_conn:
        with db_conn.cursor() as cursor:
            cursor.execute(
                sql.SQL("SELECT symbol FROM {public_schema_name}.tickers").
                format(public_schema_name=sql.Identifier(PUBLIC_SCHEMA_NAME)))
            tickers = cursor.fetchall()
            return [ticker[0] for ticker in tickers]

def persist_records(values, source):
    if not len(values):
        return

    id_fields = ['symbol', 'time']
    data_fields = ['open', 'high', 'low', 'close', 'volume', 'granularity']

    field_names_formatted = sql.SQL(',').join([
        sql.Identifier(field_name)
        for field_name in id_fields + data_fields
    ])

    id_fields_formatted = sql.SQL(',').join(
        [sql.Identifier(field_name) for field_name in id_fields])

    sql_clause = sql.SQL(
        "INSERT INTO raw_data.eod_intraday_prices ({field_names}) VALUES %s ON CONFLICT({id_fields}) DO NOTHING"
    ).format(field_names=field_names_formatted,
             id_fields=id_fields_formatted)

    with psycopg2.connect(DB_CONN_STRING) as db_conn:
        with db_conn.cursor() as cursor:
            execute_values(cursor, sql_clause, values)

    submit_dd_metric(METRIC_REALTIME_PRICES_COUNT, float(len(values)), ["source:%s" % (source)])


def submit_dd_metric(metric_name, value, tags=[]):
    if ENV != 'production':
        return

    body = MetricsPayload(
        series=[
            Series(
                metric=metric_name,
                type="gauge",
                points=[Point([datetime.datetime.now().timestamp(), value])],
                tags=tags + ["env:%s" % (ENV)],
            )
        ]
    )

    with ApiClient(dd_configuration) as api_client:
        api_instance = MetricsApi(api_client)
        api_instance.submit_metrics(body=body)
