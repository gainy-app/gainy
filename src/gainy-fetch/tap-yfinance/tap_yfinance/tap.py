"""yfinance tap class."""

from typing import List

from singer_sdk import Tap, Stream
from singer_sdk import typing as th  # JSON schema typing helpers

from tap_yfinance.streams import (
    Info
)

STREAM_TYPES = [
    Info
]

class TapYFinance(Tap):
    """yfinance tap class."""
    name = "tap-yfinance"

    config_jsonschema = th.PropertiesList(
        th.Property("symbols", th.ArrayType(th.StringType), required=True),
        th.Property("start_symbol", th.StringType),
    ).to_dict()

    def discover_streams(self) -> List[Stream]:
        """Return a list of discovered streams."""
        return [stream_class(tap=self) for stream_class in STREAM_TYPES]
