import datetime
from portfolio.plaid import PlaidClient
from portfolio.models import HoldingData, Security, Account, TransactionData, Institution


class PlaidService:

    def __init__(self):
        self.plaid_client = PlaidClient()

    def max_transactions_limit(self):
        return 500

    def get_holdings(self, db_conn, plaid_access_token):
        # InvestmentsHoldingsGetResponse[]
        response = self.plaid_client.get_investment_holdings(
            plaid_access_token["access_token"])

        holdings = [
            self.__hydrate_holding_data(holding_data)
            for holding_data in response.holdings
        ]
        securities = [
            self.__hydrate_security(security)
            for security in response.securities
        ]
        accounts = [
            self.__hydrate_account(account) for account in response.accounts
        ]
        for i in holdings:
            i.plaid_access_token_id = plaid_access_token["id"]
        for i in accounts:
            i.plaid_access_token_id = plaid_access_token["id"]

        return {
            'holdings': holdings,
            'securities': securities,
            'accounts': accounts,
        }

    def get_transactions(self,
                         db_conn,
                         plaid_access_token,
                         count=100,
                         offset=0):
        # InvestmentsTransactionsGetResponse[]
        response = self.plaid_client.get_investment_transactions(
            plaid_access_token["access_token"],
            count=count,
            offset=offset,
            start_date=datetime.date.today() -
            datetime.timedelta(days=20 * 365),
            end_date=datetime.date.today())

        transactions = [
            self.__hydrate_transaction_data(transaction_data)
            for transaction_data in response.investment_transactions
        ]
        securities = [
            self.__hydrate_security(security)
            for security in response.securities
        ]
        accounts = [
            self.__hydrate_account(account) for account in response.accounts
        ]
        for i in transactions:
            i.plaid_access_token_id = plaid_access_token["id"]
        for i in accounts:
            i.plaid_access_token_id = plaid_access_token["id"]

        return {
            'transactions': transactions,
            'securities': securities,
            'accounts': accounts,
        }

    def get_institution(self, db_conn, plaid_access_token):
        item_response = self.plaid_client.get_item(
            plaid_access_token['access_token'])
        institution_id = item_response['item']['institution_id']

        institution_response = self.plaid_client.get_institution(
            plaid_access_token['access_token'], institution_id)

        return self.__hydrate_institution(institution_response['institution'])

    def set_token_institution(self, db_conn, plaid_access_token, institution):
        with db_conn.cursor() as cursor:
            cursor.execute(
                "UPDATE app.profile_plaid_access_tokens SET institution_id = %(institution_id)s WHERE id = %(plaid_access_token_id)s",
                {
                    "institution_id": institution.id,
                    "plaid_access_token_id": plaid_access_token['id'],
                })

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

    def __hydrate_institution(self, data):
        model = Institution()
        model.ref_id = data['institution_id']
        model.name = data['name']

        return model
