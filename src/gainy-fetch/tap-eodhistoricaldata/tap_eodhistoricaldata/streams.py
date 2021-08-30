"""Stream type classes for tap-eodhistoricaldata."""

from pathlib import Path
from typing import Any, Dict, Optional, Iterator

import pendulum
import singer
from singer import RecordMessage
from singer_sdk.helpers._typing import conform_record_data_types
from singer_sdk.helpers._util import utc_now

from tap_eodhistoricaldata.client import eodhistoricaldataStream

SCHEMAS_DIR = Path(__file__).parent / Path("./schemas")


class Fundamentals(eodhistoricaldataStream):
    """Define custom stream."""
    name = "fundamentals"
    path = "/fundamentals/{Code}"
    primary_keys = ["Code"]
    selected_by_default = True

    STATE_MSG_FREQUENCY = 10

    replication_key = 'UpdatedAt'
    schema_filepath = SCHEMAS_DIR / "fundamentals.json"

    @property
    def is_sorted(self) -> bool:
        return True

    def get_url_params(
            self, context: Optional[dict], next_page_token: Optional[Any]
    ) -> Dict[str, Any]:
        """Return a dictionary of values to be used in URL parameterization."""
        params = super().get_url_params(context, next_page_token)
        params["filter"] = "General,Earnings,Highlights,AnalystRatings,Technicals,Valuation,Financials"
        return params

    @property
    def partitions(self) -> Iterator[Dict[str, Any]]:
        parts = super().partitions
        start = self.config.get('start_symbol', None)
        srted = sorted(self.config['symbols'])

        if start and start in srted:
            return list(map(lambda x: {'Code': x}, srted[srted.index(start):]))

        if not parts:
            return list(map(lambda x: {'Code': x}, srted))

        last_processed_item = parts[-1]["Code"]

        if last_processed_item not in srted:
            return list(map(lambda x: {'Code': x}, srted))

        return list(map(lambda x: {'Code': x}, srted[srted.index(last_processed_item) + 1:]))

    def post_process(self, row: dict, context: Optional[dict] = None) -> dict:
        row['Code'] = context['Code']
        if 'UpdatedAt' in row['General']:
            row['UpdatedAt'] = row['General']['UpdatedAt']
        else:
            row['UpdatedAt'] = {}

        def replace_na(row):
            for k, v in row.items():
                if v == 'NA' or v == '"NA"':
                    row[k] = {}
            return row

        return replace_na(row)

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
