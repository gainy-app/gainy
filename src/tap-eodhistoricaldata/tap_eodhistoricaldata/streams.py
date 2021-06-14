"""Stream type classes for tap-eodhistoricaldata."""

from pathlib import Path
from typing import Any, Dict, Optional, Union, List, Iterable

from singer_sdk import typing as th  # JSON Schema typing helpers

from tap_eodhistoricaldata.client import eodhistoricaldataStream

# TODO: Delete this is if not using json files for schema definition
SCHEMAS_DIR = Path(__file__).parent / Path("./schemas")
# TODO: - Override `UsersStream` and `GroupsStream` with your own stream definition.
#       - Copy-paste as many times as needed to create multiple stream types.


class Fundamentals(eodhistoricaldataStream):
    """Define custom stream."""
    name = "fundamentals"
    path = "/fundamentals/{Code}"
    primary_keys = ["Code"]

    replication_key = None
    # Optionally, you may also use `schema_filepath` in place of `schema`:
    # schema_filepath = SCHEMAS_DIR / "users.json"
    schema_filepath = SCHEMAS_DIR / "fundamentals.json"

    @property
    def partitions(self) -> Optional[List[dict]]:
        return map(lambda x: { 'Code': x }, self.config['symbols'])

    def post_process(self, row: dict, context: Optional[dict] = None) -> dict:
        row['Code'] = context['Code']
        return row


