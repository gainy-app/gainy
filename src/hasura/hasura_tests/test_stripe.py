import logging

import pytest

from hasura_tests.common import make_graphql_request
from hasura_tests.trading.common import load_query

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def test_stripe_get_checkout_url():
    query = load_query('queries', 'StripeGetCheckoutUrl')
    data = make_graphql_request(
        query, {
            "price_id": "price_1LQ8ugD1LH0kYxaoP9gtuEa0",
            "success_url": "http://example.com/success.html",
            "cancel_url": "http://example.com/cancel.html"
        })['data']['stripe_get_checkout_url']

    assert "url" in data
    assert data["url"]


def test_stripe_get_payment_sheet_data():
    query = load_query('queries', 'StripeGetPaymentSheetData')
    data = make_graphql_request(query, {
        "profile_id": 2,
    })['data']['stripe_get_payment_sheet_data']

    assert "customer_id" in data
    assert "setup_intent_client_secret" in data
    assert "ephemeral_key" in data
    assert "publishable_key" in data
    assert data["customer_id"]
    assert data["setup_intent_client_secret"]
    assert data["ephemeral_key"]
    assert data["publishable_key"]


# def get_stripe_webhook_event_ids():
#     return [
#         'evt_3LhsRvD1LH0kYxao0MPxrbhd',  # payment_intent.succeeded
#         'evt_1LhEWvD1LH0kYxaoaTOJ2mU1',  # payment_method.attached
#     ]
#
#
# @pytest.mark.parametrize("event_id", get_stripe_webhook_event_ids())
# def test_webhook(event_id):
#     query = load_query('queries', 'StripeWebhook')
#     data = make_graphql_request(query, {
#         "event_id": event_id,
#     }, None)['data']['stripe_webhook']
#
#     assert "ok" in data
#     assert data["ok"]
