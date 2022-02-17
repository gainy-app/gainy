import asyncio
import websockets
import json
import os
import datetime
from abc import abstractmethod
from decimal import Decimal
from common import run, AbstractPriceListener, NO_MESSAGES_RECONNECT_TIMEOUT

EOD_API_TOKEN = os.environ["EOD_API_TOKEN"]
SYMBOLS_LIMIT = 11000


class PricesListener(AbstractPriceListener):

    def __init__(self):
        super().__init__("eod")

        self._buckets = {}
        self.granularity = 60000  # 60 seconds
        self.api_token = EOD_API_TOKEN
        self._first_filled_key = None

    def get_symbols(self):
        return self.filter_symbols(super().get_symbols())

    def filter_symbols(self, symbols):
        symbols = [i for i in symbols if i.find('-') == -1]
        symbols.sort()
        return set(symbols[:SYMBOLS_LIMIT])

    @property
    def supported_exchanges(self):
        return ['nyse', 'nasdaq']

    async def handle_price_message(self, message):
        # Message format: {"s":"AAPL","p":161.14,"c":[12,37],"v":1,"dp":false,"t":1637573639704}
        symbol = message["s"]
        timestamp = message["t"]
        price = message["p"]
        volume = message["v"]

        if symbol not in self._buckets:
            self._buckets[symbol] = {}
        key = timestamp // self.granularity
        """
        Fill the last available OHLC candle for the symbol.
        """
        decimal_price = Decimal(price)
        decimal_volume = Decimal(volume)

        self.logger.debug("handle_price_message key %s", key)
        async with self.records_queue_lock:
            if key in self._buckets[symbol]:
                self._buckets[symbol][key]["high"] = max(
                    self._buckets[symbol][key]["high"], decimal_price)
                self._buckets[symbol][key]["low"] = min(
                    self._buckets[symbol][key]["low"], decimal_price)
                self._buckets[symbol][key]["close"] = decimal_price
                self._buckets[symbol][key]["volume"] += decimal_volume
            else:
                self._buckets[symbol][key] = {
                    "open": decimal_price,
                    "high": decimal_price,
                    "low": decimal_price,
                    "close": decimal_price,
                    "volume": decimal_volume,
                }

                prev_key = key - 1
                prev_key_filled = prev_key in self._buckets[symbol]
                if prev_key_filled and self._first_filled_key is not None and self._first_filled_key < prev_key:
                    self.logger.debug("persisting key %d %s", prev_key, symbol)
                    date = datetime.datetime.fromtimestamp(
                        prev_key * self.granularity / 1000)
                    self.records_queue.put_nowait({
                        **{
                            "symbol": symbol,
                            "date": date,
                            "granularity": self.granularity,
                        },
                        **self._buckets[symbol][prev_key]
                    })

                if self._first_filled_key is None:
                    self._first_filled_key = prev_key

                if prev_key_filled:
                    del self._buckets[symbol][prev_key]

    async def handle_message(self, message_raw):
        try:
            self.logger.debug("%s", message_raw)
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
                    self.logger.error("%s", message)
                return

            await self.handle_price_message(message)
        except Exception as e:
            self.logger.error('handle_message %s: %s', message_raw, e)

    async def listen(self):
        url = "wss://ws.eodhistoricaldata.com/ws/us?api_token=%s" % (
            self.api_token)
        first_attempt = True

        while True:
            if first_attempt:
                first_attempt = False
            else:
                self.logger.info("sleeping before reconnecting to websocket")
                await asyncio.sleep(60)

            try:
                async for websocket in websockets.connect(url):
                    self.websocket = websocket
                    self.logger.info("connected to websocket for symbols: %s",
                                     ",".join(self.symbols))
                    try:
                        await websocket.send(
                            json.dumps({
                                "action": "subscribe",
                                "symbols": ",".join(self.symbols)
                            }))
                        async for message in websocket:
                            await self.handle_message(message)

                    except websockets.ConnectionClosed as e:
                        self.logger.error("ConnectionClosed Error caught: %s",
                                          e)

                        continue

                self.logger.error("reached the end of websockets.connect loop")

            except Exception as e:
                self.logger.error("%s Error caught in start func: %s",
                                  type(e).__name__, str(e))

                continue

        self.logger.error("reached the end of start func")

        return True


if __name__ == "__main__":
    asyncio.run(run(lambda: PricesListener()))
