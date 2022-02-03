import asyncio
import polygon
from polygon.enums import StreamCluster
import os
import datetime
from decimal import Decimal
from common import get_logger, get_symbols, persist_records

ENV = os.environ["ENV"]
POLYGON_API_TOKEN = os.environ["POLYGON_API_TOKEN"]
POLYGON_REALTIME_STREAMING_HOST = os.environ["POLYGON_REALTIME_STREAMING_HOST"]

NO_MESSAGES_RECONNECT_TIMEOUT = 600  # reconnect if no messages for 10 minutes
MAX_INSERT_RECORDS_COUNT = 1000

logger = get_logger(__name__)


class PricesListener:

    def __init__(self, symbols):
        self.symbols = symbols
        self.api_token = POLYGON_API_TOKEN
        self.host = POLYGON_REALTIME_STREAMING_HOST

        self.__no_messages_reconnect_timeout = NO_MESSAGES_RECONNECT_TIMEOUT
        self.__latest_symbol_message = {}
        self.start_timestamp = self.get_current_timestamp()
        logger.debug("started at %d", self.start_timestamp)

        self.max_insert_records_count = MAX_INSERT_RECORDS_COUNT
        self.records_queue = asyncio.Queue()
        self.records_queue_lock = asyncio.Lock()

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

        async with self.records_queue_lock:
            self.records_queue.put_nowait({
                "symbol":
                symbol,
                "date":
                datetime.datetime.fromtimestamp(timestamp_start / 1000),
                "granularity":
                timestamp_end - timestamp_start,
                "open":
                open,
                "high":
                high,
                "low":
                low,
                "close":
                close,
                "volume":
                volume,
            })

    async def handle_message(self, message):
        try:
            logger.debug(str(message))
            if not message:
                return

            if "ev" not in message or message['ev'] != 'AM':
                logger.error('Unexpected message type: %s', str(message))
                return

            await self.handle_price_message(message)
        except Exception as e:
            logger.error('handle_message %s: %s', message, e)

    async def listen(self):
        stream_client = polygon.AsyncStreamClient(self.api_token,
                                                  StreamCluster.STOCKS,
                                                  host=self.host)

        try:
            await stream_client.subscribe_stock_minute_aggregates(
                list(self.symbols), self.handle_message)

            while 1:
                try:
                    await stream_client.handle_messages(
                        reconnect=True, max_reconnection_attempts=100)
                except Exception as e:
                    logger.error("%s Error caught in start func: %s",
                                 type(e).__name__, str(e))
                    await asyncio.sleep(90)
        finally:
            await stream_client.close_stream()

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
                    await asyncio.sleep(60)
                    continue

                for record in records:
                    self.__latest_symbol_message[
                        record['symbol']] = current_timestamp

                symbols_with_records = list(
                    set([record['symbol'] for record in records]))
                logger.info("__sync_records %d %s", current_timestamp,
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

                persist_records(values, "polygon")

            except Exception as e:
                logger.error("__sync_records: %s", e)

    def get_current_timestamp(self):
        return datetime.datetime.now().timestamp()

    def should_reconnect(self):
        current_timestamp = self.get_current_timestamp()
        if current_timestamp - self.start_timestamp < self.__no_messages_reconnect_timeout:
            return False

        for symbol in self.symbols:
            if symbol not in self.__latest_symbol_message:
                continue

            timeout_threshold = current_timestamp - self.__no_messages_reconnect_timeout
            logger.debug('should_reconnect %s %d %d', symbol,
                         self.__latest_symbol_message[symbol],
                         timeout_threshold)
            if self.__latest_symbol_message[symbol] > timeout_threshold:
                return False

        return True


async def main():
    tracked_symbols = None
    task = None
    should_reconnect = False
    listen_task = None
    sync_task = None

    should_run = ENV == "production"

    while True:
        symbols = set([i.replace('-', '.') for i in get_symbols()])

        if should_run and tracked_symbols != symbols or should_reconnect:
            tracked_symbols = symbols

            if listen_task is not None:
                listen_task.cancel()
            if sync_task is not None:
                sync_task.cancel()

            listener = PricesListener(symbols)
            listen_task = asyncio.create_task(listener.listen())
            sync_task = asyncio.create_task(listener.sync())

        await asyncio.sleep(NO_MESSAGES_RECONNECT_TIMEOUT)

        if should_run:
            should_reconnect = listener.should_reconnect()
            logger.info("should_reconnect: %d", should_reconnect)


if __name__ == "__main__":
    asyncio.run(main())
