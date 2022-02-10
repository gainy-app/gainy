import os
import datetime
import logging
import asyncio
from abc import ABC, abstractmethod
import psycopg2
from psycopg2 import sql
from psycopg2.extras import execute_values
from datadog_api_client.v1 import ApiClient, Configuration
from datadog_api_client.v1.api.metrics_api import MetricsApi
from datadog_api_client.v1.model.metrics_payload import MetricsPayload
from datadog_api_client.v1.model.point import Point
from datadog_api_client.v1.model.series import Series

METRIC_REALTIME_PRICES_COUNT = 'app.realtime_prices_count'

NO_MESSAGES_RECONNECT_TIMEOUT = 600  # reconnect if no messages for 10 minutes
MAX_INSERT_RECORDS_COUNT = 1000

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


def persist_records(values, source):
    if not len(values):
        return

    id_fields = ['symbol', 'time']
    data_fields = ['open', 'high', 'low', 'close', 'volume', 'granularity']

    field_names_formatted = sql.SQL(',').join(
        [sql.Identifier(field_name) for field_name in id_fields + data_fields])

    id_fields_formatted = sql.SQL(',').join(
        [sql.Identifier(field_name) for field_name in id_fields])

    sql_clause = sql.SQL(
        "INSERT INTO raw_data.eod_intraday_prices ({field_names}) VALUES %s ON CONFLICT({id_fields}) DO NOTHING"
    ).format(field_names=field_names_formatted, id_fields=id_fields_formatted)

    with psycopg2.connect(DB_CONN_STRING) as db_conn:
        with db_conn.cursor() as cursor:
            execute_values(cursor, sql_clause, values)

    submit_dd_metric(METRIC_REALTIME_PRICES_COUNT, float(len(values)),
                     ["source:%s" % (source)])


def submit_dd_metric(metric_name, value, tags=[]):
    if ENV != 'production':
        return

    body = MetricsPayload(series=[
        Series(
            metric=metric_name,
            type="gauge",
            points=[Point([datetime.datetime.now().timestamp(), value])],
            tags=tags + ["env:%s" % (ENV)],
        )
    ])

    with ApiClient(dd_configuration) as api_client:
        api_instance = MetricsApi(api_client)
        api_instance.submit_metrics(body=body)


class AbstractPriceListener(ABC):

    def __init__(self, source):
        self.symbols = self.filter_symbols(self.get_symbols())
        self.source = source
        self.records_queue = asyncio.Queue()
        self.records_queue_lock = asyncio.Lock()
        self.logger = get_logger(source)
        self.max_insert_records_count = MAX_INSERT_RECORDS_COUNT
        self._no_messages_reconnect_timeout = NO_MESSAGES_RECONNECT_TIMEOUT
        self._latest_symbol_message = {}
        self.start_timestamp = self.get_current_timestamp()
        self.logger.debug("started at %d for symbols %s", self.start_timestamp, self.symbols)

    def get_symbols(self):
        with psycopg2.connect(DB_CONN_STRING) as db_conn:
            query = sql.SQL(
                "SELECT symbol FROM {public_schema_name}.base_tickers where symbol is not null"
            ).format(public_schema_name=sql.Identifier(PUBLIC_SCHEMA_NAME))

            if self.supported_exchanges is not None:
                exchanges = '|'.join(self.supported_exchanges)
                query += sql.SQL(
                    ' and lower(exchange) similar to {pattern}').format(
                        pattern=sql.Literal('(%s)%%' % (exchanges)))

            with db_conn.cursor() as cursor:
                cursor.execute(query)
                tickers = cursor.fetchall()
                return set([ticker[0] for ticker in tickers])

    def filter_symbols(self, symbols):
        return symbols

    @property
    def supported_exchanges(self):
        return None

    def get_current_timestamp(self):
        return datetime.datetime.now().timestamp()

    @abstractmethod
    async def listen(self):
        pass

    def transform_symbol(self, symbol):
        return symbol

    def rev_transform_symbol(self, symbol):
        return symbol

    async def sync(self):
        while True:
            current_timestamp = self.get_current_timestamp()
            try:
                records = []
                for i in range(self.max_insert_records_count):
                    try:
                        async with self.records_queue_lock:
                            records.append(self.records_queue.get_nowait())
                    except asyncio.QueueEmpty:
                        break

                if len(records) == 0:
                    await asyncio.sleep(10)
                    continue

                for record in records:
                    self._latest_symbol_message[
                        record['symbol']] = current_timestamp

                symbols_with_records = list(
                    set([record['symbol'] for record in records]))
                self.logger.info("__sync_records %d %s", current_timestamp,
                                 ",".join(symbols_with_records))

                for record in records:
                    symbol = record['symbol']
                    if symbol not in self.symbols:
                        symbol = self.rev_transform_symbol(symbol)
                        record['symbol'] = symbol

                values = [(
                    record['symbol'],
                    record['date'],
                    record["open"],
                    record["high"],
                    record["low"],
                    record["close"],
                    record["volume"],
                    record['granularity'],
                ) for record in records]

                persist_records(values, self.source)

            except Exception as e:
                self.logger.error("__sync_records: %s", e)

    def should_reconnect(self):
        current_timestamp = self.get_current_timestamp()
        if current_timestamp - self.start_timestamp < self._no_messages_reconnect_timeout:
            return False

        if self.symbols != self.get_symbols():
            self.logger.info("should_reconnect: symbols changed")
            return True

        if max(self._latest_symbol_message.values()) > current_timestamp - self._no_messages_reconnect_timeout:
            self.logger.info("should_reconnect: no messages")
            return True

        return False


async def run(listener_factory):
    task = None
    should_reconnect = True
    listen_task = None
    sync_task = None

    should_run = ENV == "production"

    while True:
        if should_run and should_reconnect:
            if listen_task is not None:
                listen_task.cancel()
            if sync_task is not None:
                sync_task.cancel()

            listener = listener_factory()
            listen_task = asyncio.create_task(listener.listen())
            sync_task = asyncio.create_task(listener.sync())

        await asyncio.sleep(NO_MESSAGES_RECONNECT_TIMEOUT)

        if should_run:
            should_reconnect = listener.should_reconnect()
