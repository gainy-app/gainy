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
MANDATORY_SYMBOLS = [
    'DJI.INDX', 'GSPC.INDX', 'IXIC.INDX', 'BTC.CC', 'ETH.CC', 'USDT.CC',
    'DOGE.CC', 'BNB.CC', 'XRP.CC', 'DOT.CC', 'SOL.CC', 'ADA.CC', 'SOS', 'ITP'
]
SYMBOLS_LIMIT = int(os.getenv('SYMBOLS_LIMIT', len(MANDATORY_SYMBOLS)))


class PricesListener(AbstractPriceListener):

    def __init__(self, instance_key, endpoint=None):
        self.no_messages_reconnect_timeout = 60
        self.endpoint = endpoint

        super().__init__(instance_key, "eod")

        self._buckets = {}
        self.granularity = 60000  # 60 seconds
        self.api_token = EOD_API_TOKEN
        self._latest_filled_key = None

        if self.endpoint is None:
            self.sub_listeners = [
                PricesListener(instance_key, endpoint)
                for endpoint in ['us', 'crypto', 'index']
            ]
        else:
            self.logger.debug("[%s] started at %d for symbols %s", endpoint,
                              self.start_timestamp, self.symbols)

    def get_symbols(self):
        with self.db_connect() as db_conn:
            max_symbols_count = SYMBOLS_LIMIT - len(MANDATORY_SYMBOLS)
            max_symbols_count -= self.get_active_listeners_symbols_count(
                db_conn)

            count_to_fetch = int(0.99 * max_symbols_count)

            if count_to_fetch > 0:
                query = """
                    SELECT base_tickers.symbol
                    FROM base_tickers
                             left join ticker_metrics using (symbol)
                             left join crypto_realtime_metrics using (symbol)
                    where base_tickers.symbol not like '%%-%%'
                      and (lower(exchange) similar to '(nyse|nasdaq)%%')
                    order by coalesce(ticker_metrics.market_capitalization, crypto_realtime_metrics.market_capitalization) desc nulls last
                    limit %(count)s
                """

                with db_conn.cursor() as cursor:
                    cursor.execute(query, {"count": count_to_fetch})
                    tickers = cursor.fetchall()

                symbols = [ticker[0] for ticker in tickers]
                symbols.sort()
            else:
                symbols = []

        symbols += MANDATORY_SYMBOLS

        if self.endpoint is not None:
            symbols = filter(
                lambda symbol: self._get_eod_endpoint(symbol) == self.endpoint,
                symbols)

        return set(symbols)

    async def handle_price_message(self, message):
        # Message format: {"s":"AAPL","p":161.14,"c":[12,37],"v":1,"dp":false,"t":1637573639704}
        symbol = message["s"]
        timestamp = message["t"]
        price = message["p"]
        decimal_price = Decimal(price)

        # timestamp should be in milliseconds, so check whether we need to multiply by 1000
        current_timestamp = self.get_current_timestamp() * 1000
        if abs(current_timestamp - timestamp * 1000) < abs(current_timestamp -
                                                           timestamp):
            timestamp *= 1000

        volume = 0
        decimal_volume = Decimal(0)
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

        self.persist_key(key - 1)

    def persist_key(self, key):
        if self._latest_filled_key is not None and self._latest_filled_key >= key:
            return

        # return in case the key is incomplete (first one to persist)
        # except for indices - because we don't care about volume
        if self._latest_filled_key is None and self.endpoint != 'index':
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
                    "symbol": self.rev_transform_symbol(symbol),
                    "date": date,
                    "granularity": self.granularity,
                },
                **bucket[key]
            })
            del bucket[key]

    async def handle_message(self, message_raw):
        try:
            self.logger.debug("%s %s", self.endpoint, message_raw)
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
                    self.logger.error("Bad status: %s %s", status, message_raw)
                return

            await self.handle_price_message(message)
        except Exception as e:
            self.logger.error('handle_message %s: %s', e, message_raw)

    def should_reconnect(self):
        return super().should_reconnect(self.endpoint)

    async def sync(self):
        try:
            if self.sub_listeners is not None:
                coroutines = [
                    sub_listener.sync() for sub_listener in self.sub_listeners
                ]
                await asyncio.gather(*coroutines)
            else:
                await super().sync()
        except Exception as e:
            self.logger.exception(e)

    async def listen(self):
        if self.endpoint is None:
            coroutines = [
                sub_listener.listen() for sub_listener in self.sub_listeners
            ]
            await asyncio.gather(*coroutines)
            return

        symbols = [self.transform_symbol(symbol) for symbol in self.symbols]
        if not symbols:
            return

        url = f"wss://ws.eodhistoricaldata.com/ws/{self.endpoint}?api_token={self.api_token}"
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
                        f"connected to websocket '{self.endpoint}' for symbols: {','.join(symbols)}"
                    )
                    try:
                        await websocket.send(
                            json.dumps({
                                "action": "subscribe",
                                "symbols": ",".join(symbols)
                            }))
                        async for message in websocket:
                            await self.handle_message(message)

                    except websockets.ConnectionClosed as e:
                        self.logger.warning(
                            f"ConnectionClosed Error caught: {e}")

                    finally:
                        self.logger.info(f"Unsubscribing from {self.endpoint}")
                        try:
                            await websocket.send(
                                json.dumps({
                                    "action": "unsubscribe",
                                    "symbols": ",".join(symbols)
                                }))
                        except Exception as e:
                            self.logger.warning(
                                "%s Error caught while unsubscribing: %s",
                                type(e).__name__, str(e))

            except asyncio.CancelledError:
                self.logger.debug(f"listen done for {self.endpoint}")
                return

            except Exception as e:
                self.logger.error("%s Error caught in start func: %s",
                                  type(e).__name__, str(e))

                continue

    def transform_symbol(self, symbol):
        if self.endpoint == 'crypto':
            return re.sub(r'\.CC$', '-USD', symbol)
        if self.endpoint == 'index':
            return re.sub(r'\.INDX$', '', symbol)

        return symbol

    def rev_transform_symbol(self, symbol):
        if self.endpoint == 'crypto':
            return re.sub(r'\-USD$', '.CC', symbol)
        if self.endpoint == 'index':
            return symbol + '.INDX'

        return symbol

    def _get_eod_endpoint(self, symbol):
        if re.search(r'\.CC$', symbol) is not None:
            return 'crypto'
        if re.search(r'\.INDX$', symbol) is not None:
            return 'index'
        return 'us'


if __name__ == "__main__":
    asyncio.run(run(lambda key: PricesListener(key)))
