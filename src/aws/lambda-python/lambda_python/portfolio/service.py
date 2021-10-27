import os
import plaid
import itertools
import datetime

from portfolio.plaid import PlaidClient
from portfolio.models import HoldingData, Security, Account


class PortfolioService:
    def __init__(self):
        self.plaid_client = PlaidClient()

    def get_holdings(self, db_conn, profile_id):
        with db_conn.cursor() as cursor:
            cursor.execute(
                f"SELECT access_token FROM app.profile_plaid_access_tokens WHERE profile_id = %s",
                (profile_id, ))

            profile_plaid_access_tokens = cursor.fetchall()

        print(profile_plaid_access_tokens)
        result_threads = [
            self.plaid_client.get_investment_holdings(access_token,
                                                      async_req=True)
            for [access_token] in profile_plaid_access_tokens
        ]

        # InvestmentsHoldingsGetResponse[]
        responses = [thread.get() for thread in result_threads]

        holdings = [
            self.__hydrate_holding_data(holding_data) for response in responses
            for holding_data in response.holdings
        ]
        securities = [
            self.__hydrate_security(security) for response in responses
            for security in response.securities
        ]
        accounts = [
            self.__hydrate_account(account) for response in responses
            for account in response.accounts
        ]

        return {
            'holdings': holdings,
            'securities': securities,
            'accounts': accounts,
        }

    def __hydrate_holding_data(self, data):
        model = HoldingData()
        model.iso_currency_code = data.iso_currency_code
        model.quantity = data.quantity
        model.security_ref_id = data.security_id
        model.account_ref_id = data.account_id
        model.ref_id = "_".join([data.account_id, data.security_id])

        return model

    def __hydrate_security(self, data):
        model = Security()
        model.close_price = data.close_price
        model.close_price_as_of = (data.close_price_as_of or
                                   datetime.date.today()).strftime('%Y-%m-%d')
        model.iso_currency_code = data.iso_currency_code
        model.name = data.name
        model.ref_id = data.security_id
        model.ticker_symbol = data.ticker_symbol
        model.type = data.type

        return model

    def __hydrate_account(self, data):
        model = Account()
        model.ref_id = data['account_id']
        model.balance_available = data['balances']['available']
        model.balance_current = data['balances']['current']
        model.balance_iso_currency_code = data['balances']['iso_currency_code']
        model.balance_limit = data['balances']['limit']
        model.mask = data['mask']
        model.name = data['name']
        model.official_name = data['official_name']
        model.subtype = str(data['subtype'])
        model.type = str(data['type'])

        return model
