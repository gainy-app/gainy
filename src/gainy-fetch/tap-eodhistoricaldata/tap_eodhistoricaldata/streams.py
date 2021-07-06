"""Stream type classes for tap-eodhistoricaldata."""

from pathlib import Path
from typing import Any, Dict, Optional, Iterator

import pendulum
import singer
from singer import RecordMessage
from singer_sdk.helpers._typing import conform_record_data_types

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

    @property
    def partitions(self) -> Iterator[Dict[str, Any]]:
        return map(lambda x: {'Code': x}, self.config['symbols'])

    def post_process(self, row: dict, context: Optional[dict] = None) -> dict:
        row['Code'] = context['Code']
        row['UpdatedAt'] = row['General']['UpdatedAt']
        return row

    def _write_record_message(self, record: dict) -> None:
        """Write out a RECORD message."""
        record = conform_record_data_types(
            stream_name=self.name,
            row=record,
            schema=self.schema,
            logger=self.logger,
        )
        record_message = RecordMessage(
            stream=self.name,
            record=record,
            version=None,
            time_extracted=pendulum.now(),
        )
        singer.write_message(record_message)
