import psycopg2
from psycopg2 import sql
from psycopg2.extras import execute_values
import os

PG_HOST = os.environ['PG_HOST']
PG_PORT = os.environ['PG_PORT']
PG_DBNAME = os.environ['PG_DBNAME']
PG_USERNAME = os.environ['PG_USERNAME']
PG_PASSWORD = os.environ['PG_PASSWORD']
PUBLIC_SCHEMA_NAME = os.environ['PUBLIC_SCHEMA_NAME']

DB_CONN_STRING = f"postgresql://{PG_USERNAME}:{PG_PASSWORD}@{PG_HOST}:{PG_PORT}/{PG_DBNAME}"

def get_symbols():
    with psycopg2.connect(DB_CONN_STRING) as db_conn:
        with db_conn.cursor() as cursor:
            cursor.execute(
                sql.SQL("SELECT symbol FROM {public_schema_name}.tickers").
                format(public_schema_name=sql.Identifier(PUBLIC_SCHEMA_NAME)))
            tickers = cursor.fetchall()
            return [ticker[0] for ticker in tickers]

def persist_records(values):
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