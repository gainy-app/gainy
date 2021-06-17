"""Stream type classes for tap-eodhistoricaldata."""

from pathlib import Path
from typing import Any, Dict, Optional, Iterator

from tap_eodhistoricaldata.client import eodhistoricaldataStream

SCHEMAS_DIR = Path(__file__).parent / Path("./schemas")


class Fundamentals(eodhistoricaldataStream):
    """Define custom stream."""
    name = "fundamentals"
    path = "/fundamentals/{Code}"
    primary_keys = ["Code"]
    selected_by_default  = True

    replication_key = None
    schema_filepath = SCHEMAS_DIR / "fundamentals.json"

    @property
    def partitions(self) -> Iterator[Dict[str, Any]]:
        return map(lambda x: {'Code': x}, self.config['symbols'])

    def post_process(self, row: dict, context: Optional[dict] = None) -> dict:
        row['Code'] = context['Code']
        return row
