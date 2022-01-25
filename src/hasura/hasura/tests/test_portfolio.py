import os
import requests
import json
import datetime
import logging
from common import make_graphql_request, PROFILE_ID, MIN_PORTFOLIO_HOLDING_GROUPS_COUNT

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

def test_portfolio():
    with open(
            os.path.join(os.path.dirname(__file__),
                         'queries/GetPlaidHoldings.graphql'), 'r') as f:

        query = f.read()

    data = make_graphql_request(query, {"profileId": PROFILE_ID})['data']

    assert data['portfolio_gains'] is not None
    assert data['profile_holding_groups'] is not None
    assert len(data['profile_holding_groups']
               ) >= MIN_PORTFOLIO_HOLDING_GROUPS_COUNT

    for holding_group in data['profile_holding_groups']:
        assert holding_group['details'] is not None
        assert holding_group['gains'] is not None
        assert holding_group['holdings'] is not None
        assert len(holding_group['holdings']) > 0
        for holding in holding_group['holdings']:
            assert holding['holding_details'] is not None
            assert holding['gains'] is not None

