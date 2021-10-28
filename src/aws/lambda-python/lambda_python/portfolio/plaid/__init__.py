import os
import plaid
from plaid.api import plaid_api
from plaid.model.investments_holdings_get_request import InvestmentsHoldingsGetRequest
from plaid.model.investments_transactions_get_request import InvestmentsTransactionsGetRequest

from portfolio.plaid.common import get_plaid_client, handle_error


class PlaidClient:
    def __init__(self):
        self.client = get_plaid_client()

    def get_investment_holdings(self, access_token, async_req=False):
        request = InvestmentsHoldingsGetRequest(access_token=access_token)
        response = self.client.investments_holdings_get(request,
                                                        async_req=async_req)

        return response

    def get_investment_transactions(self,
                                    access_token,
                                    start_date,
                                    end_date,
                                    async_req=False):
        request = InvestmentsTransactionsGetRequest(access_token=access_token,
                                                    start_date=start_date,
                                                    end_date=end_date)
        response = self.client.investments_transactions_get(
            request, async_req=async_req)

        return response
