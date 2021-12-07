import asyncio
import websockets
import logging
import sys
import json
import os
import time
import datetime
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
        self.granularity = 10000  # 10 seconds
        self.api_token = API_TOKEN
        self.db_conn_string = DB_CONN_STRING

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
        logger.debug(message)
        if not message:
            return

        message = json.loads(message)

        # {"status_code":200,"message":"Authorized"}
        if "status_code" in message:
            if message["status_code"] != 200:
                logger.error(message)
            return

        self.handle_price_message(message)

    async def start(self):
        logger.info("connecting to websocket for symbols: %s",
                    ",".join(self.symbols))

        self.__sync_records_task = asyncio.ensure_future(self.__sync_records())

        async for websocket in websockets.connect(
                "wss://ws.eodhistoricaldata.com/ws/us?api_token=%s" %
            (self.api_token)):
            try:
                await websocket.send(
                    json.dumps({
                        "action": "subscribe",
                        "symbols": ",".join(self.symbols)
                    }))
                async for message in websocket:
                    self.handle_message(message)

            except websockets.ConnectionClosed:
                continue

    async def __sync_records(self):
        while True:
            try:
                # sleep the amount of time needed to wake up in 1 second after the previous interval is closed
                await asyncio.sleep(
                    (self.granularity -
                     round(time.time() * 1000) % self.granularity) / 1000 + 1)
                logger.info("__sync_records %d" % (time.time() * 1000))

                # latest fully closed time period
                current_key = round(time.time() * 1000) // self.granularity - 1
                date = datetime.datetime.fromtimestamp(current_key *
                                                       self.granularity / 1000)
                values = []
                for symbol, bucket in self.__buckets.items():
                    if current_key not in bucket:
                        continue

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

            except Exception as e:
                logger.error("__sync_records: " + str(e))

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


def get_symbols():
    with psycopg2.connect(DB_CONN_STRING) as db_conn:
        with db_conn.cursor() as cursor:
            cursor.execute(
                sql.SQL("SELECT symbol FROM {public_schema_name}.tickers").
                format(public_schema_name=sql.Identifier(PUBLIC_SCHEMA_NAME)))
            tickers = cursor.fetchall()
            return [ticker[0] for ticker in tickers]


async def main():
    max_size = 50
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
