import os
import plaid
import itertools
import datetime
import multiprocessing.dummy as mt

from portfolio.plaid import PlaidClient
from portfolio.models import HoldingData, Security, Account, TransactionData


class PortfolioService:
    def __init__(self):
        self.plaid_client = PlaidClient()

    def get_holdings(self, db_conn, profile_id):
        profile_plaid_access_tokens = self.__get_access_tokens(
            db_conn, profile_id)

        # InvestmentsHoldingsGetResponse[]
        holdings = []
        securities = []
        accounts = []
        for plaid_access_token in profile_plaid_access_tokens:
            response = self.plaid_client.get_investment_holdings(
                plaid_access_token["access_token"])

            token_holdings = [
                self.__hydrate_holding_data(holding_data)
                for holding_data in response.holdings
            ]
            token_securities = [
                self.__hydrate_security(security)
                for security in response.securities
            ]
            token_accounts = [
                self.__hydrate_account(account)
                for account in response.accounts
            ]
            for i in token_holdings:
                i.plaid_access_token_id = plaid_access_token["id"]
            for i in token_accounts:
                i.plaid_access_token_id = plaid_access_token["id"]

            holdings += token_holdings
            securities += token_securities
            accounts += token_accounts

        return {
            'holdings': self.__unique(holdings),
            'securities': self.__unique(securities),
            'accounts': self.__unique(accounts),
        }

    def get_transactions(self, db_conn, profile_id, count=100, offset=0):
        profile_plaid_access_tokens = self.__get_access_tokens(
            db_conn, profile_id)

        # InvestmentsTransactionsGetResponse[]
        transactions = []
        securities = []
        accounts = []
        for plaid_access_token in profile_plaid_access_tokens:
            response = self.plaid_client.get_investment_transactions(
                plaid_access_token["access_token"],
                count=count,
                offset=offset,
                start_date=datetime.date.today() -
                datetime.timedelta(days=20 * 365),
                end_date=datetime.date.today())

            token_transactions = [
                self.__hydrate_transaction_data(transaction_data)
                for transaction_data in response.investment_transactions
            ]
            token_securities = [
                self.__hydrate_security(security)
                for security in response.securities
            ]
            token_accounts = [
                self.__hydrate_account(account)
                for account in response.accounts
            ]
            for i in token_transactions:
                i.plaid_access_token_id = plaid_access_token["id"]
            for i in token_accounts:
                i.plaid_access_token_id = plaid_access_token["id"]

            transactions += token_transactions
            securities += token_securities
            accounts += token_accounts

        return {
            'transactions': self.__unique(transactions),
            'securities': self.__unique(securities),
            'accounts': self.__unique(accounts),
        }

    def __unique(self, entities):
        d = {entity.unique_id(): entity for entity in entities}
        return d.values()

    def __get_access_tokens(self, db_conn, profile_id):
        with db_conn.cursor() as cursor:
            cursor.execute(
                f"SELECT id, access_token FROM app.profile_plaid_access_tokens WHERE profile_id = %s",
                (profile_id, ))

            profile_plaid_access_tokens = cursor.fetchall()

            return [
                dict(zip(['id', 'access_token'], row))
                for row in profile_plaid_access_tokens
            ]

    def __hydrate_holding_data(self, data):
        model = HoldingData()
        model.iso_currency_code = data.iso_currency_code
        model.quantity = data.quantity
        model.security_ref_id = data.security_id
        model.account_ref_id = data.account_id
        model.ref_id = "_".join([data.account_id, data.security_id])

        return model

    def __hydrate_transaction_data(self, data):
        model = TransactionData()

        model.amount = data.amount
        model.date = data.date.strftime('%Y-%m-%d')
        model.fees = data.fees
        model.iso_currency_code = data.iso_currency_code
        model.name = data.name
        model.price = data.price
        model.quantity = data.quantity
        model.subtype = data.subtype
        model.type = data.type

        model.ref_id = data.investment_transaction_id
        model.account_ref_id = data.account_id
        model.security_ref_id = data.security_id

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
