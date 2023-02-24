import datetime
import time
from typing import Optional, Iterable

from plaid.model.account_base import AccountBase as PlaidAccount
from plaid.model.holding import Holding as PlaidHolding
from plaid.model.investment_transaction import InvestmentTransaction as PlaidTransaction
from plaid.model.investments_transactions_get_response import InvestmentsTransactionsGetResponse
from plaid.model.security import Security as PlaidSecurity
from psycopg2.extras import RealDictCursor

from gainy.plaid.service import PlaidService as GainyPlaidService
from portfolio.plaid import PlaidClient
from gainy.plaid.common import handle_error
from portfolio.models import HoldingData, Security, Account, TransactionData, Institution
from gainy.utils import get_logger

import plaid

SERVICE_PLAID = 'plaid'
logger = get_logger(__name__)


class PlaidService(GainyPlaidService):

    def __init__(self, db_conn):
        super().__init__(db_conn)
        self.plaid_client = PlaidClient()

    def exchange_public_token(self, public_token, env):
        try:
            return self.plaid_client.exchange_public_token(public_token, env)
        except plaid.ApiException as e:
            handle_error(e)

    def create_processor_token(self, access_token, account_id, processor):
        try:
            response = self.plaid_client.create_processor_token(
                access_token, account_id, processor)
        except plaid.ApiException as e:
            handle_error(e)

        logger.info('create_processor_token', extra={"response": response})
        return response['processor_token']

    def max_transactions_limit(self):
        return 500

    def get_holdings(self, access_token):
        if access_token.get("is_artificial"):
            return {
                'holdings': [],
                'securities': [],
                'accounts': [],
            }

        logging_extra = {
            'profile_id': access_token['profile_id'],
            'access_token_id': access_token['id'],
        }

        try:
            # InvestmentsHoldingsGetResponse[]
            request_start = time.time()
            response = self.plaid_client.get_investment_holdings(
                access_token["access_token"])
            request_end = time.time()
            logging_extra['response'] = response
            logging_extra['request_duration'] = request_end - request_start

            holdings = [
                self.__hydrate_holding_data(holding_data)
                for holding_data in response.holdings
            ]
            securities = [
                self.__hydrate_security(security)
                for security in response.securities
            ]
            accounts = [
                self.hydrate_account(account) for account in response.accounts
            ]

            for i in holdings:
                i.plaid_access_token_id = access_token["id"]
            for i in accounts:
                i.plaid_access_token_id = access_token["id"]

            return {
                'holdings': holdings,
                'securities': securities,
                'accounts': accounts,
            }
        except plaid.ApiException as e:
            self._handle_api_exception(e, access_token)
        finally:
            logger.info('get_holdings', extra=logging_extra)

    def get_transactions(self, plaid_access_token, count=100, offset=0):
        if plaid_access_token.get("is_artificial"):
            return {
                'transactions': [],
                'securities': [],
                'accounts': [],
            }

        try:
            response: InvestmentsTransactionsGetResponse = self.plaid_client.get_investment_transactions(
                plaid_access_token["access_token"],
                count=count,
                offset=offset,
                start_date=datetime.date.today() -
                datetime.timedelta(days=20 * 365),
                end_date=datetime.date.today())
            logger.info('plaid transactions',
                        extra={
                            "profile_id": plaid_access_token["profile_id"],
                            "response": response
                        })

            transactions = [
                self.__hydrate_transaction_data(transaction_data)
                for transaction_data in response.investment_transactions
            ]
            securities = [
                self.__hydrate_security(security)
                for security in response.securities
            ]
            accounts = [
                self.hydrate_account(account) for account in response.accounts
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
        except plaid.ApiException as e:
            self._handle_api_exception(e, plaid_access_token)

    def get_institution(self, access_token) -> Optional[Institution]:
        logger.info('get_institution', extra={"access_token": access_token})
        if access_token.get("is_artificial"):
            return None

        item_response = self.plaid_client.get_item(
            access_token['access_token'])
        institution_id = item_response['item']['institution_id']

        institution_response = self.plaid_client.get_institution(
            access_token['access_token'], institution_id)

        return self.__hydrate_institution(institution_response['institution'])

    def set_token_institution(self, plaid_access_token, institution):
        with self.db_conn.cursor() as cursor:
            cursor.execute(
                "UPDATE app.profile_plaid_access_tokens SET institution_id = %(institution_id)s WHERE id = %(plaid_access_token_id)s",
                {
                    "institution_id": institution.id,
                    "plaid_access_token_id": plaid_access_token['id'],
                })

    def __hydrate_holding_data(self, data: PlaidHolding) -> HoldingData:
        model = HoldingData()
        model.iso_currency_code = data.iso_currency_code
        model.quantity = data.quantity
        model.security_ref_id = data.security_id
        model.account_ref_id = data.account_id
        model.ref_id = "_".join([data.account_id, data.security_id])

        return model

    def __hydrate_transaction_data(self,
                                   data: PlaidTransaction) -> TransactionData:
        model = TransactionData()

        model.amount = data.amount
        model.date = data.date.strftime('%Y-%m-%d')
        model.fees = data.fees
        model.iso_currency_code = data.iso_currency_code
        model.name = data.name
        model.price = data.price
        model.quantity = data.quantity
        model.subtype = data.subtype.value
        model.type = data.type.value

        model.ref_id = data.investment_transaction_id
        model.account_ref_id = data.account_id
        model.security_ref_id = data.security_id

        return model

    def __hydrate_security(self, data: PlaidSecurity) -> Security:
        model = Security()
        model.close_price = data.close_price
        model.close_price_as_of = (data.close_price_as_of or
                                   datetime.date.today()).strftime('%Y-%m-%d')
        model.iso_currency_code = data.iso_currency_code
        model.name = data.name or ""
        model.ref_id = data.security_id
        model.ticker_symbol = data.ticker_symbol
        model.type = data.type

        return model

    def hydrate_account(self, data: PlaidAccount) -> Account:
        model = Account()
        model.ref_id = data['account_id']
        model.balance_available = data['balances']['available']
        model.balance_current = data['balances']['current']
        model.balance_iso_currency_code = data['balances']['iso_currency_code']
        model.balance_limit = data['balances']['limit']
        model.mask = data['mask'] or ""
        model.name = data['name']
        model.official_name = data['official_name']
        model.subtype = str(data['subtype'])
        model.type = str(data['type'])

        return model

    def __hydrate_institution(self, data) -> Institution:
        model = Institution()
        model.ref_id = data['institution_id']
        model.name = data['name']

        return model

    def get_access_token(self, id=None, item_id=None):
        where = []
        params = {}
        if item_id:
            where.append("item_id = %(item_id)s")
            params["item_id"] = item_id
        if id:
            where.append("id = %(id)s")
            params["id"] = id

        query = "SELECT id, access_token, is_artificial, profile_id FROM app.profile_plaid_access_tokens"
        if where:
            query += " where " + " and ".join(where)

        with self.db_conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(query, params)
            row = cursor.fetchone()
        if not row:
            return None

        row['service'] = SERVICE_PLAID
        return dict(row)

    def get_access_tokens(self,
                          profile_id=None,
                          purpose=None) -> Iterable[dict]:
        where = []
        params = {}
        if profile_id:
            where.append("profile_id = %(profile_id)s")
            params["profile_id"] = profile_id
        if purpose:
            where.append("purpose = %(purpose)s")
            params["purpose"] = purpose

        query = "SELECT id, access_token, is_artificial, profile_id FROM app.profile_plaid_access_tokens"
        if where:
            query += " where " + " and ".join(where)

        with self.db_conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(query, params)

            for row in cursor:
                row['service'] = SERVICE_PLAID
                yield dict(row)
