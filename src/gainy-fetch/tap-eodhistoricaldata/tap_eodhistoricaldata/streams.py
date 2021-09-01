"""Stream type classes for tap-eodhistoricaldata."""

import datetime
from pathlib import Path
from typing import Any, Dict, Optional, Iterator, Iterable


import pendulum
import singer
from singer import RecordMessage
from singer_sdk.helpers._typing import conform_record_data_types
from singer_sdk.helpers._util import utc_now

from tap_eodhistoricaldata.client import eodhistoricaldataStream

SCHEMAS_DIR = Path(__file__).parent / Path("./schemas")


class AbstractEODStream(eodhistoricaldataStream):
    @property
    def is_sorted(self) -> bool:
        return True

    @property
    def partitions(self) -> Iterator[Dict[str, Any]]:
        parts = super().partitions
        start = self.config.get('start_symbol', None)
        sorted_symbols = sorted(self.config['symbols'])

        if start and start in sorted_symbols:
            return list(map(lambda x: {'Code': x}, sorted_symbols[sorted_symbols.index(start):]))

        if not parts:
            return list(map(lambda x: {'Code': x}, sorted_symbols))

        last_processed_item = parts[-1]["Code"]

        if last_processed_item not in sorted_symbols:
            return list(map(lambda x: {'Code': x}, sorted_symbols))

        return list(map(lambda x: {'Code': x}, sorted_symbols[sorted_symbols.index(last_processed_item) + 1:]))

    def post_process(self, row: dict, context: Optional[dict] = None) -> dict:
        row['Code'] = context['Code']

        def replace_na(row):
            for k, v in row.items():
                if v == 'NA' or v == '"NA"':
                    row[k] = {}
            return row

        return replace_na(row)

    def get_records(self, context: Optional[dict]) -> Iterable[Dict[str, Any]]:
        try:
            # Cannot return super().get_records(context) because for some reason error catching does not work.
            for row in self.request_records(context):
                row = self.post_process(row, context)
                yield row
        except Exception as e:
            self.logger.error('Error while requesting %s for symbol %s: %s' % (self.name, context['Code'], str(e)))
            pass

class Fundamentals(AbstractEODStream):
    name = "fundamentals"
    path = "/fundamentals/{Code}"
    primary_keys = ["Code"]
    selected_by_default = True

    STATE_MSG_FREQUENCY = 10

    replication_key = 'UpdatedAt'
    schema_filepath = SCHEMAS_DIR / "fundamentals.json"

    def get_url_params(
            self, context: Optional[dict], next_page_token: Optional[Any]
    ) -> Dict[str, Any]:
        """Return a dictionary of values to be used in URL parameterization."""
        params = super().get_url_params(context, next_page_token)
        params["filter"] = "General,Earnings,Highlights,AnalystRatings,Technicals,Valuation,Financials"
        return params

    def post_process(self, row: dict, context: Optional[dict] = None) -> dict:
        if 'UpdatedAt' in row['General']:
            row['UpdatedAt'] = row['General']['UpdatedAt']
        else:
            row['UpdatedAt'] = {}

        return super().post_process(row, context)

class HistoricalDividends(AbstractEODStream):
    name = "dividends"
    path = "/div/{Code}?fmt=json"
    primary_keys = ["Code", "date"]
    selected_by_default = True

    STATE_MSG_FREQUENCY = 10

    replication_key = 'date'
    schema_filepath = SCHEMAS_DIR / "dividends.json"

class HistoricalPrices(AbstractEODStream):
    name = "historical_prices"
    path = "/eod/{Code}?fmt=json&period=d&from=%s" % ((datetime.datetime.now() - datetime.timedelta(days=14*30)).strftime('%Y-%m-%d'))
    primary_keys = ["Code", "date"]
    selected_by_default = True

    STATE_MSG_FREQUENCY = 10

    replication_key = 'date'
    schema_filepath = SCHEMAS_DIR / "eod.json"
