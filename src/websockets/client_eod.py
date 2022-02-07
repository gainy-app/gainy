import asyncio
import websockets
import sys
import json
import os
import time
import datetime
import hashlib
from decimal import Decimal
from common import get_logger, get_symbols, persist_records

ENV = os.environ["ENV"]
EOD_API_TOKEN = os.environ["EOD_API_TOKEN"]

logger = get_logger(__name__)


class PricesListener:

    def __init__(self, symbols):
        self.__buckets = {}
        self.__last_written_key = {}
        self.symbols = symbols
        self.log_prefix = hashlib.sha1(
            (",".join(symbols)).encode('utf-8')).hexdigest()
        self.granularity = 60000  # 60 seconds
        self.api_token = EOD_API_TOKEN
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

    def handle_message(self, message_raw):
        try:
            logger.debug("[%s] %s", self.log_prefix, message_raw)
            if not message_raw:
                return

            message = json.loads(message_raw)

            # {"status_code":200,"message":"Authorized"}
            # {"status":500,"message":"Server error"}
            if "status_code" in message:
                status = message["status_code"]
            elif "status" in message:
                status = message["status"]
            else:
                status = None

            if status is not None:
                if status != 200:
                    logger.error("[%s] %s", self.log_prefix, message)
                return

            self.handle_price_message(message)
        except Exception as e:
            logger.error('[%s] handle_message %s: %s', self.log_prefix,
                         message_raw, e)

    async def start(self):
        self.__sync_records_task = asyncio.ensure_future(self.__sync_records())
        url = "wss://ws.eodhistoricaldata.com/ws/us?api_token=%s" % (
            self.api_token)
        first_attempt = True

        while True:
            if first_attempt:
                first_attempt = False
            else:
                logger.info("[%s] sleeping before reconnecting to websocket",
                            self.log_prefix)
                time.sleep(60)

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
                        logger.error("[%s] ConnectionClosed Error caught: %s",
                                     self.log_prefix, e)

                        continue

                logger.error("[%s] reached the end of websockets.connect loop",
                             self.log_prefix)

            except Exception as e:
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

                persist_records(values, "eod")

            except Exception as e:
                logger.error("[%s] __sync_records: %s", self.log_prefix, e)

    def __init_volume_zero_count(self):
        self.volume_zero_count = {symbol: 0 for symbol in self.symbols}


async def main():
    max_size = 500
    symbols_tasks = {}

    should_run = ENV == "production"

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

        if len(chunks) > 1:
            chunks = chunks[0:1]

        if should_run and len(symbols_tasks) == 0:
            for symbols in chunks:
                task = asyncio.create_task(PricesListener(symbols).start())
                for symbol in symbols:
                    symbols_tasks[symbol] = task

        await asyncio.sleep(3600)


if __name__ == "__main__":
    asyncio.run(main())
