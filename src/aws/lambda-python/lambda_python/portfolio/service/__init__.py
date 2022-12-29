import time

from portfolio.exceptions import AccessTokenApiException, AccessTokenLoginRequiredException
from portfolio.plaid import PlaidService
from portfolio.repository import PortfolioRepository
from gainy.utils import get_logger

logger = get_logger(__name__)

SERVICE_PLAID = 'plaid'


class PortfolioService:

    def __init__(self, db_conn, portfolio_repository: PortfolioRepository,
                 plaid_service: PlaidService):
        self.db_conn = db_conn
        self.portfolio_repository = portfolio_repository
        self.services = {SERVICE_PLAID: plaid_service}

    def get_holdings(self, profile_id):
        holdings = []
        securities = []
        accounts = []
        for access_token in self.__get_access_tokens(profile_id):
            try:
                token_data = self.__get_service(
                    access_token['service']).get_holdings(access_token)

                holdings += token_data['holdings']
                securities += token_data['securities']
                accounts += token_data['accounts']
            except AccessTokenLoginRequiredException as e:
                self._set_access_token_reauth(e.access_token)

        self.persist_holding_data(profile_id, securities, accounts, holdings)

        return holdings

    def sync_token_holdings(self, access_token):
        try:
            data = self.__get_service(
                access_token['service']).get_holdings(access_token)
            holdings = data['holdings']
            self.persist_holding_data(access_token['profile_id'],
                                      data['securities'], data['accounts'],
                                      holdings)

            return len(holdings)
        except AccessTokenLoginRequiredException as e:
            self._set_access_token_reauth(e.access_token)
            return 0

    def get_transactions(self, profile_id, count=500, offset=0):
        transactions = []
        securities = []
        accounts = []

        for access_token in self.__get_access_tokens(profile_id):
            try:
                self.sync_institution(access_token)
                token_service = self.__get_service(access_token['service'])
                token_data = token_service.get_transactions(access_token,
                                                            count=count,
                                                            offset=offset)

                transactions += token_data['transactions']
                securities += token_data['securities']
                accounts += token_data['accounts']
            except AccessTokenLoginRequiredException as e:
                self._set_access_token_reauth(e.access_token)

        self.persist_transaction_data(profile_id, securities, accounts,
                                      transactions)

        return transactions

    def sync_token_transactions(self, access_token):
        all_transactions = []
        transactions_count = 0
        count = self.__get_service(
            access_token['service']).max_transactions_limit()
        try:
            for offset in range(0, 1000000, count):
                request_start = time.time()
                data = self.__get_service(
                    access_token['service']).get_transactions(access_token,
                                                              count=count,
                                                              offset=offset)
                request_end = time.time()

                cur_transactions = data['transactions']
                cur_tx_cnt = len(cur_transactions)

                persist_start = time.time()
                all_transactions += cur_transactions
                self.persist_transaction_data(access_token['profile_id'],
                                              data['securities'],
                                              data['accounts'],
                                              cur_transactions)
                persist_end = time.time()

                first_tx = cur_transactions[0].to_dict(
                ) if cur_tx_cnt else None
                last_tx = cur_transactions[-1].to_dict(
                ) if cur_tx_cnt else None
                logging_extra = {
                    'profile_id': access_token['profile_id'],
                    'access_token_id': access_token['id'],
                    'offset': offset,
                    'tx_cnt': cur_tx_cnt,
                    'request_duration': request_end - request_start,
                    'persist_duration': persist_end - persist_start,
                    'first_tx': first_tx,
                    'last_tx': last_tx,
                }
                logger.info('sync_token_transactions', extra=logging_extra)

                transactions_count += cur_tx_cnt
                if cur_tx_cnt < count:
                    break
        except AccessTokenLoginRequiredException as e:
            self._set_access_token_reauth(e.access_token)
        except AccessTokenApiException as e:
            pass

        # cleanup
        self.portfolio_repository.remove_other_by_access_token(
            all_transactions)

        return transactions_count

    def sync_institution(self, access_token):
        institution = self.__get_service(
            access_token['service']).get_institution(access_token)

        if not institution:
            return

        self.portfolio_repository.persist(institution)

        self.__get_service(access_token['service']).set_token_institution(
            access_token, institution)

    def persist_holding_data(self, profile_id, securities, accounts, holdings):
        securities_dict = self.__persist_securities(securities)
        accounts_dict = self.__persist_accounts(accounts, profile_id)
        holdings = [
            i for i in holdings if i.security_ref_id is not None
            and i.security_ref_id in securities_dict and i.account_ref_id
            is not None and i.account_ref_id in accounts_dict
        ]
        holdings = self.__unique(holdings)

        # persist holdings
        for entity in holdings:
            entity.profile_id = profile_id
            entity.security_id = securities_dict[entity.security_ref_id]
            entity.account_id = accounts_dict[entity.account_ref_id]
        self.portfolio_repository.persist(holdings)

        # cleanup
        self.portfolio_repository.remove_other_by_access_token(holdings)
        self.portfolio_repository.remove_other_by_access_token(accounts)

    def persist_transaction_data(self, profile_id, securities, accounts,
                                 transactions):
        securities_dict = self.__persist_securities(securities)
        accounts_dict = self.__persist_accounts(accounts, profile_id)
        transactions = [
            i for i in transactions if i.security_ref_id is not None
            and i.security_ref_id in securities_dict and i.account_ref_id
            is not None and i.account_ref_id in accounts_dict
        ]
        transactions = self.__unique(transactions)

        # persist transactions
        for entity in transactions:
            entity.profile_id = profile_id
            entity.security_id = securities_dict[entity.security_ref_id]
            entity.account_id = accounts_dict[entity.account_ref_id]
        self.portfolio_repository.persist(transactions)

    def __get_service(self, name):
        if name not in self.services:
            raise Exception('Service %s not supported' % (name))

        return self.services[name]

    def __persist_securities(self, securities):
        self.portfolio_repository.persist(self.__unique(securities))
        return {security.ref_id: security.id for security in securities}

    def __persist_accounts(self, accounts, profile_id):
        for entity in accounts:
            entity.profile_id = profile_id
        self.portfolio_repository.persist(self.__unique(accounts))
        return {account.ref_id: account.id for account in accounts}

    def __unique(self, entities):
        d = {entity.unique_id(): entity for entity in entities}
        return d.values()

    def __get_access_tokens(self, profile_id):
        with self.db_conn.cursor() as cursor:
            cursor.execute(
                f"SELECT id, access_token, is_artificial, profile_id FROM app.profile_plaid_access_tokens WHERE profile_id = %s and purpose = 'portfolio'",
                (profile_id, ))

            access_tokens = cursor.fetchall()

            return [
                dict(
                    zip([
                        'id', 'access_token', 'is_artificial', 'profile_id',
                        'service'
                    ], row + (SERVICE_PLAID, ))) for row in access_tokens
            ]

    def _set_access_token_reauth(self, access_token):
        with self.db_conn.cursor() as cursor:
            cursor.execute(
                "update app.profile_plaid_access_tokens set needs_reauth_since = now() where id = %(access_token_id)s",
                {"access_token_id": access_token['id']})
