import asyncio
import websockets
import json
import os
import datetime
import re
from decimal import Decimal
from common import run, AbstractPriceListener

EOD_API_TOKEN = os.environ["EOD_API_TOKEN"]
MANDATORY_SYMBOLS = [
    'DJI.INDX', 'GSPC.INDX', 'IXIC.INDX', 'BTC.CC', 'ETH.CC', 'USDT.CC',
    'DOGE.CC', 'BNB.CC', 'XRP.CC', 'DOT.CC', 'SOL.CC', 'ADA.CC', 'LINK.CC',
    'ATOM.CC', 'CRV.CC', 'MATIC.CC', 'DAI.CC', 'SHIB.CC', 'STEPN.CC', 'TRX.CC',
    'UNI.CC', 'USDC.CC', 'AFRM', 'BILL', 'COIN', 'FISV', 'HOOD', 'INTU',
    'NVEI', 'PYPL', 'SOFI', 'SQ', 'UPST', 'WDAY'
]
SYMBOLS_LIMIT = int(os.getenv('SYMBOLS_LIMIT', len(MANDATORY_SYMBOLS)))


class PricesListener(AbstractPriceListener):

    def __init__(self, instance_key, endpoint=None):
        self._ping_interval = 5
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
                    SELECT symbol
                    FROM base_tickers
                             left join ticker_metrics using (symbol)
                    where (lower(exchange) similar to '(nyse|nasdaq)%%' and symbol not like '%%-%%')
                       or (type = 'crypto' and symbol in ('1INCHB.CC', '1INCH.CC', 'AAVEB.CC', 'AAVEDOWN.CC', 'AAVEUP.CC', 'AAVE.CC', 'ACMB.CC', 'ACM.CC', 'ADAB.CC', 'ADADOWN.CC', 'ADAT.CC', 'ADAUP.CC', 'ADA.CC', 'AERGOB.CC', 'AIONB.CC', 'AION.CC', 'AKRO.CC', 'ALGOB.CC', 'ALGOT.CC', 'ALGO.CC', 'ALICEB.CC', 'ALICE.CC', 'ALPHAB.CC', 'ALPHA.CC', 'ANKRT.CC', 'ANKR.CC', 'ANTB.CC', 'ANT.CC', 'ARDR.CC', 'ARPA.CC', 'ASR.CC', 'ATM.CC', 'ATOMB.CC', 'ATOMT.CC', 'ATOM.CC', 'AUCTIONB.CC', 'AUDB.CC', 'AUDIOB.CC', 'AUDIO.CC', 'AUD.CC', 'AUTOB.CC', 'AUTO.CC', 'AVAB.CC', 'AVA.CC', 'AVAXB.CC', 'AVAX.CC', 'AXSB.CC', 'AXS.CC', 'BADGERB.CC', 'BADGER.CC', 'BAKEB.CC', 'BALB.CC', 'BAL.CC', 'BANDB.CC', 'BAND.CC', 'BATB.CC', 'BATT.CC', 'BAT.CC', 'BCC.CC', 'BCHABCB.CC', 'BCHABCT.CC', 'BCHABC.CC', 'BCHAB.CC', 'BCHB.CC', 'BCHDOWN.CC', 'BCHSVT.CC', 'BCHSV.CC', 'BCHT.CC', 'BCHUP.CC', 'BCH.CC', 'BCPTT.CC', 'BEAM.CC', 'BEARB.CC', 'BEAR.CC', 'BELB.CC', 'BEL.CC', 'BIFIB.CC', 'BKRWB.CC', 'BKRW.CC', 'BLZB.CC', 'BLZ.CC', 'BNBBEARB.CC', 'BNBBEAR.CC', 'BNBBULLB.CC', 'BNBBULL.CC', 'BNBB.CC', 'BNBDOWN.CC', 'BNBT.CC', 'BNBUP.CC', 'BNB.CC', 'BNTB.CC', 'BNT.CC', 'BOTB.CC', 'BTCB.CC', 'BTCDOWN.CC', 'BTCSTB.CC', 'BTCT.CC', 'BTCUP.CC', 'BTC.CC', 'BTSB.CC', 'BTS.CC', 'BTTB.CC', 'BTTT.CC', 'BTT.CC', 'BULLB.CC', 'BULL.CC', 'BUSD.CC', 'BZRXB.CC', 'BZRX.CC', 'CAKEB.CC', 'CAKE.CC', 'CELO.CC', 'CELR.CC', 'CFXB.CC', 'CFX.CC', 'CHR.CC', 'CHZB.CC', 'CHZ.CC', 'CKBB.CC', 'CKB.CC', 'COCOS.CC', 'COMPB.CC', 'COMP.CC', 'COS.CC', 'COTI.CC', 'COVERB.CC', 'CREAMB.CC', 'CRVB.CC', 'CRV.CC', 'CTKB.CC', 'CTK.CC', 'CTSIB.CC', 'CTSI.CC', 'CTXC.CC', 'CVC.CC', 'CVPB.CC', 'DAIB.CC', 'DAI.CC', 'DASHB.CC', 'DASH.CC', 'DATAB.CC', 'DATA.CC', 'DCRB.CC', 'DCR.CC', 'DEGOB.CC', 'DEGO.CC', 'DEXEB.CC', 'DGBB.CC', 'DGB.CC', 'DIAB.CC', 'DIA.CC', 'DNTB.CC', 'DNT.CC', 'DOCK.CC', 'DODOB.CC', 'DODO.CC', 'DOGEB.CC', 'DOGE.CC', 'DOTB.CC', 'DOTDOWN.CC', 'DOTUP.CC', 'DOT.CC', 'DREP.CC', 'DUSK.CC', 'EGLDB.CC', 'EGLD.CC', 'ENJB.CC', 'ENJ.CC', 'EOSBEARB.CC', 'EOSBEAR.CC', 'EOSBULLB.CC', 'EOSBULL.CC', 'EOSB.CC', 'EOSDOWN.CC', 'EOST.CC', 'EOSUP.CC', 'EOS.CC', 'EPSB.CC', 'EPS.CC', 'ERDB.CC', 'ERD.CC', 'ETCB.CC', 'ETCT.CC', 'ETC.CC', 'ETHBEARB.CC', 'ETHBEAR.CC', 'ETHBULLB.CC', 'ETHBULL.CC', 'ETHB.CC', 'ETHDOWN.CC', 'ETHT.CC', 'ETHUP.CC', 'ETH.CC', 'EURB.CC', 'EUR.CC', 'FET.CC', 'FILB.CC', 'FILDOWN.CC', 'FILUP.CC', 'FIL.CC', 'FIOB.CC', 'FIO.CC', 'FIRO.CC', 'FISB.CC', 'FIS.CC', 'FLMB.CC', 'FLM.CC', 'FORB.CC', 'FRONTB.CC', 'FTMT.CC', 'FTM.CC', 'FTT.CC', 'FUN.CC', 'FXSB.CC', 'GBPB.CC', 'GBP.CC', 'GHSTB.CC', 'GRTB.CC', 'GRT.CC', 'GTOT.CC', 'GTO.CC', 'GXS.CC', 'HARDB.CC', 'HARD.CC', 'HBARB.CC', 'HBAR.CC', 'HC.CC', 'HEGICB.CC', 'HIVE.CC', 'HNT.CC', 'HOT.CC', 'ICXB.CC', 'ICX.CC', 'IDEXB.CC', 'INJB.CC', 'INJ.CC', 'IOSTB.CC', 'IOTAB.CC', 'IOTA.CC', 'IOTX.CC', 'IRISB.CC', 'IRIS.CC', 'JSTB.CC', 'JST.CC', 'JUVB.CC', 'JUV.CC', 'KAVA.CC', 'KEY.CC', 'KMDB.CC', 'KMD.CC', 'KNCB.CC', 'KNC.CC', 'KP3RB.CC', 'KSMB.CC', 'KSM.CC', 'LENDB.CC', 'LEND.CC', 'LINAB.CC', 'LINA.CC', 'LINKB.CC', 'LINKDOWN.CC', 'LINKT.CC', 'LINKUP.CC', 'LINK.CC', 'LITB.CC', 'LIT.CC', 'LRCB.CC', 'LRC.CC', 'LSK.CC', 'LTCB.CC', 'LTCDOWN.CC', 'LTCT.CC', 'LTCUP.CC', 'LTC.CC', 'LTO.CC', 'LUNAB.CC', 'LUNA.CC', 'MANAB.CC', 'MANA.CC', 'MATICB.CC', 'MATIC.CC', 'MBL.CC', 'MCO.CC', 'MDT.CC', 'MFT.CC', 'MITH.CC', 'MKRB.CC', 'MKR.CC', 'MTL.CC', 'NANOB.CC', 'NANO.CC', 'NBS.CC', 'NEARB.CC', 'NEAR.CC', 'NEOB.CC', 'NEOT.CC', 'NEO.CC', 'NKN.CC', 'NMRB.CC', 'NMR.CC', 'NPXS.CC', 'NULS.CC', 'OCEANB.CC', 'OCEAN.CC', 'OGN.CC', 'OG.CC', 'OMGB.CC', 'OMG.CC', 'OM.CC', 'ONEB.CC', 'ONET.CC', 'ONE.CC', 'ONG.CC', 'ONTB.CC', 'ONT.CC', 'ORN.CC', 'OXT.CC', 'PAXB.CC', 'PAXGB.CC', 'PAXG.CC', 'PAXT.CC', 'PAX.CC', 'PERL.CC', 'PERPB.CC', 'PERP.CC', 'PHAB.CC', 'PHBT.CC', 'PNT.CC', 'PONDB.CC', 'POND.CC', 'PROMB.CC', 'PSGB.CC', 'PSG.CC', 'QTUMB.CC', 'QTUM.CC', 'RAMPB.CC', 'RAMP.CC', 'REEFB.CC', 'REEF.CC', 'REN.CC', 'REPB.CC', 'REP.CC', 'RIF.CC', 'RLC.CC', 'ROSEB.CC', 'ROSE.CC', 'RSRB.CC', 'RSR.CC', 'RUNEB.CC', 'RUNE.CC', 'RVNB.CC', 'RVN.CC', 'SANDB.CC', 'SAND.CC', 'SC.CC', 'SFPB.CC', 'SFP.CC', 'SKLB.CC', 'SKL.CC', 'SNXB.CC', 'SNX.CC', 'SOLB.CC', 'SOL.CC', 'SRMB.CC', 'SRM.CC', 'STMX.CC', 'STORJB.CC', 'STORJ.CC', 'STORM.CC', 'STRATB.CC', 'STRAXB.CC', 'STRAX.CC', 'STX.CC', 'SUN.CC', 'SUPERB.CC', 'SUPER.CC', 'SUSD.CC', 'SUSHIB.CC', 'SUSHIDOWN.CC', 'SUSHIUP.CC', 'SUSHI.CC', 'SWRVB.CC', 'SXPB.CC', 'SXPDOWN.CC', 'SXPUP.CC', 'SXP.CC', 'SYSB.CC', 'TCT.CC', 'TFUELT.CC', 'TFUEL.CC', 'THETA.CC', 'TKOB.CC', 'TKO.CC', 'TOMOB.CC', 'TOMO.CC', 'TRBB.CC', 'TRB.CC', 'TROY.CC', 'TRUB.CC', 'TRU.CC', 'TRXB.CC', 'TRXDOWN.CC', 'TRXT.CC', 'TRXUP.CC', 'TRX.CC', 'TUSDBT.CC', 'TUSDB.CC', 'TUSD.CC', 'TVKB.CC', 'TWTB.CC', 'TWT.CC', 'UFTB.CC', 'UMA.CC', 'UNFIB.CC', 'UNFI.CC', 'UNIB.CC', 'UNIDOWN.CC', 'UNIUP.CC', 'UNI.CC', 'UTK.CC', 'VEN.CC', 'VETB.CC', 'VET.CC', 'VIDTB.CC', 'VITE.CC', 'VTHOB.CC', 'VTHO.CC', 'WAN.CC', 'WAVESB.CC', 'WAVEST.CC', 'WAVES.CC', 'WINGB.CC', 'WING.CC', 'WIN.CC', 'WNXMB.CC', 'WNXM.CC', 'WRXB.CC', 'WRX.CC', 'WTC.CC', 'XEM.CC', 'XLMB.CC', 'XLMDOWN.CC', 'XLMT.CC', 'XLMUP.CC', 'XLM.CC', 'XMRB.CC', 'XMR.CC', 'XRPBEARB.CC', 'XRPBEAR.CC', 'XRPBULLB.CC', 'XRPBULL.CC', 'XRPB.CC', 'XRPDOWN.CC', 'XRPT.CC', 'XRPUP.CC', 'XRP.CC', 'XTZB.CC', 'XTZDOWN.CC', 'XTZUP.CC', 'XTZ.CC', 'XVGB.CC', 'XVSB.CC', 'XVS.CC', 'XZC.CC', 'YFIB.CC', 'YFIDOWN.CC', 'YFIIB.CC', 'YFII.CC', 'YFIUP.CC', 'YFI.CC', 'ZECB.CC', 'ZECT.CC', 'ZEC.CC', 'ZEN.CC', 'ZILB.CC', 'ZIL.CC', 'ZRXB.CC', 'ZRX.CC'))
                    order by type = 'crypto' desc, market_capitalization desc nulls last
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

    def should_reconnect(self, log_prefix=None):
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
                self.logger.info(
                    f"sleeping before reconnecting to {self.endpoint}")
                await asyncio.sleep(60)

            try:
                await self._connect_and_listen(url, symbols)

            except asyncio.CancelledError:
                self.logger.debug(f"listen done for {self.endpoint}")
                return

            except Exception as e:
                self.logger.exception(e)

                continue

    def transform_symbol(self, symbol):
        if self.endpoint == 'crypto':
            return re.sub(r'\.CC$', '-USD', symbol)
        if self.endpoint == 'index':
            return re.sub(r'\.INDX$', '', symbol)

        return symbol

    def rev_transform_symbol(self, symbol):
        if self.endpoint == 'crypto':
            return re.sub(r'-USD$', '.CC', symbol)
        if self.endpoint == 'index':
            return symbol + '.INDX'

        return symbol

    def _get_eod_endpoint(self, symbol):
        if re.search(r'\.CC$', symbol) is not None:
            return 'crypto'
        if re.search(r'\.INDX$', symbol) is not None:
            return 'index'
        return 'us'

    async def _connect_and_listen(self, url, symbols):
        async with websockets.connect(
                url, ping_interval=self._ping_interval) as websocket:
            try:
                self.logger.info(
                    f"connected to websocket '{self.endpoint}' for symbols: {','.join(symbols)}"
                )
                await websocket.send(
                    json.dumps({
                        "action": "subscribe",
                        "symbols": ",".join(symbols)
                    }))
                async for message in websocket:
                    await self.handle_message(message)

            except (websockets.ConnectionClosed, asyncio.TimeoutError) as e:
                self.logger.warning(e)

            finally:
                self.logger.info(f"Unsubscribing from {self.endpoint}")
                try:
                    await websocket.send(
                        json.dumps({
                            "action": "unsubscribe",
                            "symbols": ",".join(symbols)
                        }))

                except websockets.ConnectionClosed:
                    pass

                except Exception as e:
                    self.logger.warning(
                        "%s Error caught while unsubscribing: %s",
                        type(e).__name__, str(e))


if __name__ == "__main__":
    asyncio.run(run(lambda key: PricesListener(key)))
