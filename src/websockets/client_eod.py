import asyncio
import websockets
import json
import os
import datetime
import re
from abc import abstractmethod
from decimal import Decimal
from common import run, AbstractPriceListener, NO_MESSAGES_RECONNECT_TIMEOUT

EOD_API_TOKEN = os.environ["EOD_API_TOKEN"]
SYMBOLS_LIMIT = 11000
MANDATORY_SYMBOLS = ['DJI.INDX', 'GSPC.INDX', 'IXIC.INDX', 'BTC.CC']


class PricesListener(AbstractPriceListener):

    def __init__(self):
        super().__init__("eod")

        self._buckets = {}
        self.granularity = 60000  # 60 seconds
        self.api_token = EOD_API_TOKEN
        self._first_filled_key = None
        self._rev_transform_mapping = {}

    def get_symbols(self):
        with self.db_connect() as db_conn:
            query = "SELECT symbol FROM base_tickers where symbol is not null and lower(exchange) similar to '(nyse|nasdaq)%'"

            with db_conn.cursor() as cursor:
                cursor.execute(query)
                tickers = cursor.fetchall()

        symbols = [ticker[0] for ticker in tickers]
        symbols = list(filter(lambda symbol: symbol.find('-') == -1, symbols))
        symbols.sort()
        return set(MANDATORY_SYMBOLS + symbols[:SYMBOLS_LIMIT - len(MANDATORY_SYMBOLS)])

    async def handle_price_message(self, message):
        # Message format: {"s":"AAPL","p":161.14,"c":[12,37],"v":1,"dp":false,"t":1637573639704}
        symbol = message["s"]
        timestamp = message["t"]
        price = message["p"]
        decimal_price = Decimal(price)

        volume = message.get("v") or (Decimal(message.get("q")) * decimal_price)
        decimal_volume = Decimal(volume)

        if symbol not in self._buckets:
            self._buckets[symbol] = {}
        key = timestamp // self.granularity
        """
        Fill the last available OHLC candle for the symbol.
        """

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
            self.logger.error('handle_message %s: %s', e, message_raw)

    async def listen(self):
        listen_us_task = asyncio.create_task(self.listen_us())
        listen_crypto_task = asyncio.create_task(self.listen_crypto())

        await listen_us_task
        await listen_crypto_task

    async def listen_us(self):
        symbols = filter(lambda symbol: re.search(r'.CC$', symbol) is None, self.symbols)
        symbols = list(symbols)
        await self._base_listen('us', symbols)

    async def listen_crypto(self):
        symbols = list(filter(lambda symbol: re.search(r'.CC$', symbol) is not None, self.symbols))

        self._rev_transform_mapping = {}
        for symbol in symbols:
            self._rev_transform_mapping[self.transform_symbol(symbol)] = symbol

        symbols = list(map(lambda symbol: self.transform_symbol(symbol), symbols))
        await self._base_listen('crypto', symbols)

    async def _base_listen(self, endpoint, symbols):
        url = f"wss://ws.eodhistoricaldata.com/ws/{endpoint}?api_token={self.api_token}"
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
                    self.logger.info(f"connected to websocket '{endpoint}' for symbols: {','.join(symbols)}")
                    try:
                        await websocket.send(
                            json.dumps({
                                "action": "subscribe",
                                "symbols": ",".join(symbols)
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

    def transform_symbol(self, symbol):
        return re.sub(r'.CC$', '-USD', symbol)

    def rev_transform_symbol(self, symbol):
        if symbol in self._rev_transform_mapping:
            return self._rev_transform_mapping[symbol]

        return symbol


if __name__ == "__main__":
    asyncio.run(run(lambda: PricesListener()))
