"""Tests standard tap features using the built-in SDK tests library."""

import pytest

from singer_sdk.testing import get_standard_tap_tests

from tap_eodhistoricaldata.tap import Tapeodhistoricaldata
from requests import Session

SAMPLE_CONFIG = {
    "api_token": "fake_token",
    "symbols": ["AAPL"]
}


# Run standard built-in tap tests from the SDK

@pytest.mark.vcr
def test_standard_tap_tests():
    """Run standard tap tests from the SDK."""

    tests = get_standard_tap_tests(
        Tapeodhistoricaldata,
        config=SAMPLE_CONFIG
    )

    for test in tests:
        test()

