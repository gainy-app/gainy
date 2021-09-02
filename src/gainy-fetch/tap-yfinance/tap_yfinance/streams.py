"""Stream type classes for tap-yfinance."""

import datetime
from pathlib import Path
from typing import Any, Dict, Optional, Iterator, Iterable, Union, Tuple

import yfinance

import singer
from singer import RecordMessage
from singer_sdk.helpers._typing import conform_record_data_types
from singer_sdk.helpers._util import utc_now
from singer_sdk.streams import Stream

SCHEMAS_DIR = Path(__file__).parent / Path("./schemas")

class Info(Stream):
    name = "yfinance_info"
    primary_keys = ["Code"]
    selected_by_default = True

    STATE_MSG_FREQUENCY = 100

    replication_key = 'UpdatedAt'
    schema_filepath = SCHEMAS_DIR / "info.json"

    def get_records(
        self, context: Optional[dict]
    ) -> Iterable[Union[dict, Tuple[dict, dict]]]:
        try:
            yield self.post_process(yfinance.Ticker(context['Code']).info, context)
        except Exception as e:
            self.logger.error('Error while requesting %s for symbol %s: %s' % (self.name, context['Code'], str(e)))
            pass

    def post_process(self, row: dict, context: Optional[dict] = None) -> dict:
        row['Code'] = context['Code']
        row['UpdatedAt'] = datetime.datetime.now().strftime('%Y-%m-%d')

        def replace_na(row):
            for k, v in row.items():
                if v == 'NA' or v == '"NA"':
                    row[k] = {}
            return row

        return replace_na(row)

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

    def _write_record_message(self, record: dict) -> None:
        """Write out a RECORD message."""
        record = conform_record_data_types(
            stream_name=self.name,
            row=record,
            schema=self.schema,
            logger=self.logger,
        )
        for stream_map in self.stream_maps:
            mapped_record = stream_map.transform(record)
            # Emit record if not filtered
            if mapped_record is not None:
                record_message = RecordMessage(
                    stream=stream_map.stream_alias,
                    record=mapped_record,
                    version=None,
                    time_extracted=utc_now(),
                )
                singer.write_message(record_message)
