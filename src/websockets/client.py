import asyncio
import websockets
import logging
import sys
import json
import os
import time
import datetime
import hashlib
from decimal import Decimal
import psycopg2
from psycopg2.extras import execute_values
from psycopg2 import sql

ENV = os.environ["ENV"]
API_TOKEN = os.environ["API_TOKEN"]
PG_HOST = os.environ['PG_HOST']
PG_PORT = os.environ['PG_PORT']
PG_DBNAME = os.environ['PG_DBNAME']
PG_USERNAME = os.environ['PG_USERNAME']
PG_PASSWORD = os.environ['PG_PASSWORD']
PUBLIC_SCHEMA_NAME = os.environ['PUBLIC_SCHEMA_NAME']

DB_CONN_STRING = f"postgresql://{PG_USERNAME}:{PG_PASSWORD}@{PG_HOST}:{PG_PORT}/{PG_DBNAME}"

logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG if ENV == "local" else logging.INFO)


class PricesListener:
    def __init__(self, symbols):
        self.__buckets = {}
        self.__last_written_key = {}
        self.symbols = symbols
        self.log_prefix = hashlib.sha1(
            (",".join(symbols)).encode('utf-8')).hexdigest()
        self.granularity = 10000  # 10 seconds
        self.api_token = API_TOKEN
        self.db_conn_string = DB_CONN_STRING
        self.volume_zero_threshold = 60  # reconnect if receive consequtive 60 buckets with volume 0 (10 minutes of no data)
        self.__init_volume_zero_count()

    def handle_price_message(self, message):
        # Message format: {"s":"AAPL","p":161.14,"c":[12,37],"v":1,"dp":false,"t":1637573639704}
        symbol = message["s"]
        timestamp = message["t"]
        price = message["p"]
        volume = message["v"]

        if symbol not in self.__buckets:
            self.__buckets[symbol] = {}
        key = timestamp // self.granularity
        """
        Fill the last available OHLC candle for the symbol.
        """
        decimal_price = Decimal(price)
        decimal_volume = Decimal(volume)

        logger.debug("[%s] handle_price_message key %s", self.log_prefix, key)
        if key in self.__buckets[symbol]:
            self.__buckets[symbol][key]["high"] = max(
                self.__buckets[symbol][key]["high"], decimal_price)
            self.__buckets[symbol][key]["low"] = min(
                self.__buckets[symbol][key]["low"], decimal_price)
            self.__buckets[symbol][key]["close"] = decimal_price
            self.__buckets[symbol][key]["volume"] += decimal_volume
        else:
            self.__buckets[symbol][key] = {
                "open": decimal_price,
                "high": decimal_price,
                "low": decimal_price,
                "close": decimal_price,
                "volume": decimal_volume,
            }

    def handle_message(self, message):
        try:
            logger.debug("[%s] %s", self.log_prefix, message)
            if not message:
                return

            message = json.loads(message)

            # {"status_code":200,"message":"Authorized"}
            if "status_code" in message:
                if message["status_code"] != 200:
                    logger.error(message)
                return

            self.handle_price_message(message)
        except e:
            logger.error('handle_message %s', e)
            raise e

    async def start(self):
        self.__sync_records_task = asyncio.ensure_future(self.__sync_records())
        url = "wss://ws.eodhistoricaldata.com/ws/us?api_token=%s" % (
            self.api_token)

        for i in range(10):
            try:
                async for websocket in websockets.connect(url):
                    self.websocket = websocket
                    logger.info("[%s] connected to websocket for symbols: %s",
                                self.log_prefix, ",".join(self.symbols))
                    try:
                        await websocket.send(
                            json.dumps({
                                "action": "subscribe",
                                "symbols": ",".join(self.symbols)
                            }))
                        async for message in websocket:
                            self.handle_message(message)

                    except websockets.ConnectionClosed as e:
                        logger.error("ConnectionClosed Error caught: %s", e)

                        continue

                logger.error("[%s] reached the end of websockets.connect loop",
                             self.log_prefix)

            except e:
                logger.error("[%s] %s Error caught in start func: %s",
                             self.log_prefix,
                             type(e).__name__, str(e))

                continue

        logger.error("[%s] reached the end of start func", self.log_prefix)

    async def __sync_records(self):
        while True:
            try:
                # sleep the amount of time needed to wake up in 1 second after the previous interval is closed
                await asyncio.sleep(
                    (self.granularity -
                     round(time.time() * 1000) % self.granularity) / 1000 + 1)

                # latest fully closed time period
                current_key = round(time.time() * 1000) // self.granularity - 1
                date = datetime.datetime.fromtimestamp(current_key *
                                                       self.granularity / 1000)

                symbols_with_records = [
                    symbol for symbol in self.symbols
                    if symbol in self.__buckets
                    and current_key in self.__buckets[symbol]
                ]
                logger.info("[%s] __sync_records %s", self.log_prefix,
                            ",".join(symbols_with_records))
                logger.debug("[%s] current_key %s", self.log_prefix,
                             current_key)

                values = []
                for symbol in self.symbols:
                    if symbol not in self.__buckets or current_key not in self.__buckets[
                            symbol]:
                        self.volume_zero_count[symbol] += 1

                        if self.volume_zero_count[
                                symbol] >= self.volume_zero_threshold:
                            logger.info(
                                "[%s] volume_zero_threshold reached for symbol %s, reconnecting",
                                self.log_prefix, symbol)

                            self.__init_volume_zero_count()

                            if self.websocket is not None:
                                await self.websocket.close()
                            else:
                                logger.error("[%s] websocket is null",
                                             self.log_prefix)

                        continue

                    self.volume_zero_count[symbol] = 0
                    bucket = self.__buckets[symbol]

                    values.append((
                        symbol,
                        date,
                        bucket[current_key]["open"],
                        bucket[current_key]["high"],
                        bucket[current_key]["low"],
                        bucket[current_key]["close"],
                        bucket[current_key]["volume"],
                        self.granularity,
                    ))

                    del bucket[current_key]

                self.__persist_records(values)

            except e:
                logger.error("[%s] __sync_records: %s", self.log_prefix, e)

    def __persist_records(self, values):
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

        with psycopg2.connect(self.db_conn_string) as db_conn:
            with db_conn.cursor() as cursor:
                execute_values(cursor, sql_clause, values)

    def __init_volume_zero_count(self):
        self.volume_zero_count = {symbol: 0 for symbol in self.symbols}


def get_symbols():
    with psycopg2.connect(DB_CONN_STRING) as db_conn:
        with db_conn.cursor() as cursor:
            cursor.execute(
                sql.SQL("SELECT symbol FROM {public_schema_name}.tickers").
                format(public_schema_name=sql.Identifier(PUBLIC_SCHEMA_NAME)))
            tickers = cursor.fetchall()
            return [ticker[0] for ticker in tickers]


async def main():
    max_size = 100
    symbols_tasks = {}

    while True:
        # get all symbols
        all_symbols = set(get_symbols())
        tracked_symbols = set(symbols_tasks.keys())

        obsolete_symbols = tracked_symbols - all_symbols
        for symbol in obsolete_symbols:
            symbols_tasks[symbol].cancel()
            del symbols_tasks[symbol]

        new_symbols = list(all_symbols - tracked_symbols)
        chunks = [
            new_symbols[i:i + max_size]
            for i in range(0, len(new_symbols), max_size)
        ]

        for symbols in chunks:
            task = asyncio.create_task(PricesListener(symbols).start())
            for symbol in symbols:
                symbols_tasks[symbol] = task

        await asyncio.sleep(3600)


if __name__ == "__main__":
    asyncio.run(main())
