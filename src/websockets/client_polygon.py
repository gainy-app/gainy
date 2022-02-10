import asyncio
import polygon
from polygon.enums import StreamCluster
import os
import datetime
from decimal import Decimal
from common import run, AbstractPriceListener, NO_MESSAGES_RECONNECT_TIMEOUT

POLYGON_API_TOKEN = os.environ["POLYGON_API_TOKEN"]
POLYGON_REALTIME_STREAMING_HOST = os.environ["POLYGON_REALTIME_STREAMING_HOST"]


class PricesListener(AbstractPriceListener):

    def __init__(self):
        super().__init__("polygon")

        self.api_token = POLYGON_API_TOKEN
        self.host = POLYGON_REALTIME_STREAMING_HOST

    async def handle_price_message(self, message):
        # { "ev": "AM", "sym": "GTE", "v": 4110, "av": 9470157, "op": 0.4372, "vw": 0.4488, "o": 0.4488, "c": 0.4486, "h": 0.4489, "l": 0.4486, "a": 0.4352, "z": 685, "s": 1610144640000, "e": 1610144700000 }
        symbol = message["sym"]
        timestamp_start = message["s"]
        timestamp_end = message["e"]
        volume = Decimal(message["v"])
        open = Decimal(message["o"])
        high = Decimal(message["h"])
        low = Decimal(message["l"])
        close = Decimal(message["c"])
        date = datetime.datetime.fromtimestamp(timestamp_start / 1000)
        granularity = timestamp_end - timestamp_start

        async with self.records_queue_lock:
            self.records_queue.put_nowait({
                "symbol": symbol,
                "date": date,
                "granularity": granularity,
                "open": open,
                "high": high,
                "low": low,
                "close": close,
                "volume": volume,
            })

    async def handle_message(self, message):
        try:
            self.logger.debug(str(message))
            if not message:
                return

            if "ev" not in message or message['ev'] != 'AM':
                self.logger.error('Unexpected message type: %s', str(message))
                return

            await self.handle_price_message(message)
        except Exception as e:
            self.logger.error('handle_message %s: %s', message, e)

    async def listen(self):
        stream_client = polygon.AsyncStreamClient(self.api_token,
                                                  StreamCluster.STOCKS,
                                                  host=self.host)

        try:
            await stream_client.subscribe_stock_minute_aggregates(
                [self.transform_symbol(i) for i in self.symbols],
                self.handle_message)

            while 1:
                try:
                    await stream_client.handle_messages(
                        reconnect=True, max_reconnection_attempts=100)
                except Exception as e:
                    self.logger.error("%s Error caught in start func: %s",
                                      type(e).__name__, str(e))
                    await asyncio.sleep(90)
        finally:
            await stream_client.close_stream()

    def transform_symbol(self, symbol):
        return i.replace('-', '.')

    def rev_transform_symbol(self, symbol):
        return i.replace('.', '-')


if __name__ == "__main__":
    asyncio.run(run(lambda: PricesListener()))
