import os

from gainy.plaid.client import PlaidClient as GainyPlaidClient
from gainy.utils import get_logger

from plaid.model.investments_transactions_get_response import InvestmentsTransactionsGetResponse
from plaid.model.country_code import CountryCode
from plaid.model.products import Products
from plaid.model.link_token_create_request import LinkTokenCreateRequest
from plaid.model.link_token_create_request_user import LinkTokenCreateRequestUser
from plaid.model.item_public_token_exchange_request import ItemPublicTokenExchangeRequest
from plaid.model.investments_holdings_get_request import InvestmentsHoldingsGetRequest
from plaid.model.investments_transactions_get_request import InvestmentsTransactionsGetRequest
from plaid.model.investments_transactions_get_request_options import InvestmentsTransactionsGetRequestOptions
from plaid.model.webhook_verification_key_get_request import WebhookVerificationKeyGetRequest
from plaid.model.item_get_request import ItemGetRequest
from plaid.model.institutions_get_by_id_request import InstitutionsGetByIdRequest
from plaid.model.processor_token_create_request import ProcessorTokenCreateRequest

logger = get_logger(__name__)

PLAID_WEBHOOK_URL = os.getenv("PLAID_WEBHOOK_URL")
COUNTRY_CODES = [CountryCode('US')]


class PlaidClient(GainyPlaidClient):

    def create_link_token(self,
                          profile_id,
                          redirect_uri,
                          products,
                          env=None,
                          access_token=None,
                          account_filters=None):
        #TODO when we have verified phone number, we can implement https://plaid.com/docs/link/returning-user/#enabling-the-returning-user-experience
        params = {
            "client_name": "Gainy",
            "country_codes": COUNTRY_CODES,
            "language": 'en',
            "redirect_uri": redirect_uri,
            "webhook": PLAID_WEBHOOK_URL,
            "user":
            LinkTokenCreateRequestUser(client_user_id=str(profile_id), )
        }
        if access_token is None:
            params['products'] = [Products(i) for i in products]
        else:
            params['access_token'] = access_token

        if account_filters:
            params['account_filters'] = account_filters

        request = LinkTokenCreateRequest(**params)
        response = self.get_client(env).link_token_create(request)

        return response

    def exchange_public_token(self, public_token, env=None):
        request = ItemPublicTokenExchangeRequest(public_token=public_token)
        return self.get_client(env).item_public_token_exchange(request)

    def create_processor_token(self, access_token, account_id, processor):
        request = ProcessorTokenCreateRequest(access_token=access_token,
                                              account_id=account_id,
                                              processor=processor)
        return self.get_client(access_token).processor_token_create(request)

    def get_investment_holdings(self, access_token):
        request = InvestmentsHoldingsGetRequest(access_token=access_token)
        return self.get_client(access_token).investments_holdings_get(request)

    def get_investment_transactions(self,
                                    access_token,
                                    start_date,
                                    end_date,
                                    count=100,
                                    offset=0
                                    ) -> InvestmentsTransactionsGetResponse:
        request = InvestmentsTransactionsGetRequest(
            access_token=access_token,
            start_date=start_date,
            end_date=end_date,
            options=InvestmentsTransactionsGetRequestOptions(
                count=count,
                offset=offset,
            ))
        response = self.get_client(access_token).investments_transactions_get(
            request)

        return response

    def get_item(self, access_token):
        request = ItemGetRequest(access_token=access_token)
        response = self.get_client(access_token).item_get(request)

        return response

    def get_institution(self, access_token, institution_id):
        request = InstitutionsGetByIdRequest(institution_id=institution_id,
                                             country_codes=COUNTRY_CODES)
        response = self.get_client(access_token).institutions_get_by_id(
            request)

        return response

    def webhook_verification_key_get(self, current_key_id, access_token):
        request = WebhookVerificationKeyGetRequest(current_key_id)
        response = self.get_client(access_token).webhook_verification_key_get(
            request)

        return response
