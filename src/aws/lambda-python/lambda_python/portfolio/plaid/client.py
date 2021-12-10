from plaid.model.investments_holdings_get_request import InvestmentsHoldingsGetRequest
from plaid.model.investments_transactions_get_request import InvestmentsTransactionsGetRequest
from plaid.model.investments_transactions_get_request_options import InvestmentsTransactionsGetRequestOptions

from portfolio.plaid.common import get_plaid_client, handle_error


class PlaidClient:
    def __init__(self):
        self.client = get_plaid_client()

    def get_investment_holdings(self, access_token):
        request = InvestmentsHoldingsGetRequest(access_token=access_token)
        response = self.client.investments_holdings_get(request)

        return response

    def get_investment_transactions(self,
                                    access_token,
                                    start_date,
                                    end_date,
                                    count=100,
                                    offset=0):
        request = InvestmentsTransactionsGetRequest(
            access_token=access_token,
            start_date=start_date,
            end_date=end_date,
            options=InvestmentsTransactionsGetRequestOptions(
                count=count,
                offset=offset,
            ))
        response = self.client.investments_transactions_get(request)

        return response

    def webhook_verification_key_get(self, current_key_id):
        request = WebhookVerificationKeyGetRequest(current_key_id)
        response = self.client.webhook_verification_key_get(request)

        return response
