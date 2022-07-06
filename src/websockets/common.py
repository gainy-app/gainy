import asyncio
import datetime
import logging
import os
import random
import string
import time
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

NO_MESSAGES_RECONNECT_TIMEOUT = 120  # reconnect if no messages for 2 minutes
MAX_INSERT_RECORDS_COUNT = 1000
LOCK_RESOURCE_ID_POLYGON = 0  # 0 - 10
LOCK_RESOURCE_ID_EOD = 10  # 10 - 20

PG_HOST = os.environ['PG_HOST']
PG_PORT = os.environ['PG_PORT']
PG_DBNAME = os.environ['PG_DBNAME']
PG_USERNAME = os.environ['PG_USERNAME']
PG_PASSWORD = os.environ['PG_PASSWORD']
PUBLIC_SCHEMA_NAME = os.environ['PUBLIC_SCHEMA_NAME']
ENV = os.environ['ENV']

DB_CONN_STRING = f"postgresql://{PG_USERNAME}:{PG_PASSWORD}@{PG_HOST}:{PG_PORT}/{PG_DBNAME}?options=-csearch_path%3D{PUBLIC_SCHEMA_NAME}"

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

    def __init__(self, instance_key, source):
        self.sub_listeners = None
        self.active_tasks = []
        self.instance_key = instance_key
        self.source = source
        self.logger = get_logger(source)
        self.no_messages_reconnect_timeout = NO_MESSAGES_RECONNECT_TIMEOUT
        self.symbols = self.get_symbols()
        self.records_queue = asyncio.Queue()
        self.records_queue_lock = asyncio.Lock()
        self.max_insert_records_count = MAX_INSERT_RECORDS_COUNT
        self._latest_symbol_message = {}
        self.start_timestamp = self.get_current_timestamp()

    def db_connect(self):
        return psycopg2.connect(DB_CONN_STRING)

    def get_current_timestamp(self):
        return datetime.datetime.now().timestamp()

    @abstractmethod
    async def listen(self):
        pass

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
                await self.save_heartbeat(len(self.symbols))

            except Exception as e:
                self.logger.error("__sync_records: %s", e)

    def should_reconnect(self, log_prefix=None):
        if self.sub_listeners is not None:
            for sub_listener in self.sub_listeners:
                if sub_listener.should_reconnect():
                    return True
            return False

        current_timestamp = self.get_current_timestamp()
        if current_timestamp - self.start_timestamp < self.no_messages_reconnect_timeout:
            return False

        if not self.get_symbols().issubset(self.symbols):
            self.logger.info("should_reconnect %s: symbols changed", log_prefix
                             or "")
            return True

        if len(self._latest_symbol_message) > 0:
            latest_message_time = max(self._latest_symbol_message.values())
        else:
            latest_message_time = 0

        no_messages_reconnect_threshold = current_timestamp - self.no_messages_reconnect_timeout
        if latest_message_time < no_messages_reconnect_threshold:
            self.logger.info("should_reconnect %s: no messages", log_prefix
                             or "")
            return True

        return False

    async def save_heartbeat(self, symbols_count):
        try:
            query = """
                insert into deployment.realtime_listener_heartbeat(source, key, symbols_count, time)
                values (%(source)s, %(key)s, %(symbols_count)s, now())
            """
            with self.db_connect() as db_conn:
                with db_conn.cursor() as cursor:
                    cursor.execute(
                        query, {
                            "source": self.source,
                            "key": self.instance_key,
                            "symbols_count": symbols_count,
                        })
        except Exception as e:
            self.logger.exception(e)

    def get_active_listeners_symbols_count(self, db_conn):
        query = """
            SELECT sum(symbols_count)
            FROM (
                SELECT distinct on (key) symbols_count
                FROM deployment.realtime_listener_heartbeat
                where time > %(from_time)s and source = %(source)s and key != %(key)s
                order by key, time desc
            ) t
        """

        from_time = datetime.datetime.now(
            tz=datetime.timezone.utc) - datetime.timedelta(
                seconds=self.no_messages_reconnect_timeout)

        with db_conn.cursor() as cursor:
            cursor.execute(
                query, {
                    "from_time": from_time,
                    "source": self.source,
                    "key": self.instance_key,
                })
            result = cursor.fetchone()[0] or 0

            self.logger.debug("[%s, %s]  active_listeners_symbols_count %d",
                              self.instance_key, self.source, result)
            return result

    def connect(self):
        if self.sub_listeners is not None:
            tasks = []
            for sub_listener in self.sub_listeners:
                tasks += sub_listener.connect()
            return tasks

        self.active_tasks = [
            asyncio.create_task(self.listen()),
            asyncio.create_task(self.sync())
        ]
        return self.active_tasks

    def check_reconnect(self):
        if self.sub_listeners is not None:
            tasks = []
            for sub_listener in self.sub_listeners:
                tasks += sub_listener.check_reconnect()
            return tasks

        if not self.should_reconnect():
            return self.active_tasks

        for task in self.active_tasks:
            task.cancel()
        time.sleep(1)

        return self.connect()


async def run(listener_factory):
    logger = get_logger(__name__)
    instance_key = ''.join(
        random.choice(string.ascii_lowercase) for i in range(10))

    listener = None
    for i in range(60):
        try:
            listener = listener_factory(instance_key)
            listener.connect()
            break
        except psycopg2.errors.UndefinedTable:
            await asyncio.sleep(60)

    if listener is None:
        raise Exception('Failed to initialize listener')

    while True:
        try:
            listener.check_reconnect()

            await asyncio.sleep(listener.no_messages_reconnect_timeout)
        except psycopg2.errors.UndefinedTable:
            pass
        except Exception as e:
            logger.exception(e)

        await asyncio.sleep(30)
