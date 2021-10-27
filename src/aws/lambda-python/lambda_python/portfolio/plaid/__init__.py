import os
import plaid
from plaid.api import plaid_api
from plaid.model.investments_holdings_get_request import InvestmentsHoldingsGetRequest

from portfolio.plaid.common import get_plaid_client, handle_error


class PlaidClient:
    def __init__(self):
        self.client = get_plaid_client()

    def get_investment_holdings(self, access_token, async_req=False):
        request = InvestmentsHoldingsGetRequest(access_token=access_token)
        response = self.client.investments_holdings_get(request,
                                                        async_req=async_req)

        return response
