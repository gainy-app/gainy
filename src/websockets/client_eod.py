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
SYMBOLS_LIMIT = 13000
MANDATORY_SYMBOLS = ['DJI.INDX', 'GSPC.INDX', 'IXIC.INDX', 'BTC.CC']


class PricesListener(AbstractPriceListener):

    def __init__(self):
        super().__init__("eod")

        self._buckets = {}
        self.granularity = 60000  # 60 seconds
        self.api_token = EOD_API_TOKEN
        self._latest_filled_key = None

    def get_symbols(self):
        with self.db_connect() as db_conn:
            query = """
                SELECT base_tickers.symbol, case when type = 'crypto' then 1 else 0 end as priority
                FROM base_tickers
                         left join ticker_metrics on ticker_metrics.symbol = base_tickers.symbol
                where base_tickers.symbol is not null
                  and (lower(exchange) similar to '(nyse|nasdaq)%' or type = 'crypto')
                order by priority desc, market_capitalization desc nulls last
            """

            with db_conn.cursor() as cursor:
                cursor.execute(query)
                tickers = cursor.fetchall()

        symbols = [ticker[0] for ticker in tickers]
        symbols = list(filter(lambda symbol: symbol.find('-') == -1, symbols))
        symbols.sort()
        return set(MANDATORY_SYMBOLS +
                   symbols[:SYMBOLS_LIMIT - len(MANDATORY_SYMBOLS)])

    async def handle_price_message(self, endpoint, message):
        # Message format: {"s":"AAPL","p":161.14,"c":[12,37],"v":1,"dp":false,"t":1637573639704}
        symbol = message["s"]
        timestamp = message["t"]
        price = message["p"]
        decimal_price = Decimal(price)

        volume = decimal_volume = None
        if "v" in message:
            volume = message.get("v")
            decimal_volume = Decimal(volume)
        elif "q" in message:
            volume = Decimal(message["q"]) * decimal_price
            decimal_volume = volume

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
                if decimal_volume is not None:
                    self._buckets[symbol][key]["volume"] += decimal_volume
            else:
                self._buckets[symbol][key] = {
                    "open": decimal_price,
                    "high": decimal_price,
                    "low": decimal_price,
                    "close": decimal_price,
                    "volume": decimal_volume,
                }

        self.persist_key(endpoint, key - 1)

    def persist_key(self, endpoint, key):
        if self._latest_filled_key is not None and self._latest_filled_key >= key:
            return

        # return in case the key is incomplete (first one to persist)
        # except for indices - because we don't care about volume
        if self._latest_filled_key is None and endpoint != 'index':
            self._latest_filled_key = key
            return

        self._latest_filled_key = key

        date = datetime.datetime.fromtimestamp(key * self.granularity / 1000)

        for symbol, bucket in self._buckets.items():
            if key not in bucket:
                continue

            self.logger.debug("persisting key %d %s", key, symbol)
            self.records_queue.put_nowait({
                **{
                    "symbol": self.rev_transform_symbol(endpoint, symbol),
                    "date": date,
                    "granularity": self.granularity,
                },
                **bucket[key]
            })
            del bucket[key]

    async def handle_message(self, endpoint, message_raw):
        try:
            self.logger.debug("%s %s", endpoint, message_raw)
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

            await self.handle_price_message(endpoint, message)
        except Exception as e:
            self.logger.error('handle_message %s: %s', e, message_raw)

    async def listen(self):
        coroutines = [
            self._base_listen(endpoint)
            for endpoint in ['us', 'crypto', 'index']
        ]
        await asyncio.gather(*coroutines)

    async def _base_listen(self, endpoint):
        symbols = [
            self.transform_symbol(endpoint, symbol) for symbol in self.symbols
            if self._get_eod_endpoint(symbol) == endpoint
        ]
        if not symbols:
            return

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
                    self.logger.info(
                        f"connected to websocket '{endpoint}' for symbols: {','.join(symbols)}"
                    )
                    try:
                        await websocket.send(
                            json.dumps({
                                "action": "subscribe",
                                "symbols": ",".join(symbols)
                            }))
                        async for message in websocket:
                            await self.handle_message(endpoint, message)

                    except websockets.ConnectionClosed as e:
                        self.logger.error(
                            f"ConnectionClosed Error caught: {e}")

                    finally:
                        self.logger.info(f"Unsubscribing from {endpoint}")
                        try:
                            await websocket.send(
                                json.dumps({
                                    "action": "unsubscribe",
                                    "symbols": ",".join(symbols)
                                }))
                        except Exception as e:
                            self.logger.error(
                                "%s Error caught while unsubscribing: %s",
                                type(e).__name__, str(e))

            except asyncio.CancelledError:
                self.logger.debug(f"listen done for {endpoint}")
                return

            except Exception as e:
                self.logger.error("%s Error caught in start func: %s",
                                  type(e).__name__, str(e))

                continue

    def transform_symbol(self, endpoint, symbol):
        if endpoint == 'crypto':
            return re.sub(r'\.CC$', '-USD', symbol)
        if endpoint == 'index':
            return re.sub(r'\.INDX$', '', symbol)

        return symbol

    def rev_transform_symbol(self, endpoint, symbol):
        if endpoint == 'crypto':
            return re.sub(r'\-USD$', '.CC', symbol)
        if endpoint == 'index':
            return symbol + '.INDX'

        return symbol

    def _get_eod_endpoint(self, symbol):
        if re.search(r'\.CC$', symbol) is not None:
            return 'crypto'
        if re.search(r'\.INDX$', symbol) is not None:
            return 'index'
        return 'us'


if __name__ == "__main__":
    asyncio.run(run(lambda: PricesListener()))
