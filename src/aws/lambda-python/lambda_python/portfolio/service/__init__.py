from portfolio.exceptions import AccessTokenLoginRequiredException
from portfolio.plaid import PlaidService
from portfolio.repository import PortfolioRepository

SERVICE_PLAID = 'plaid'


class PortfolioService:

    def __init__(self):
        self.portfolio_repository = PortfolioRepository()
        self.services = {SERVICE_PLAID: PlaidService()}

    def get_holdings(self, db_conn, profile_id):
        holdings = []
        securities = []
        accounts = []
        for access_token in self.__get_access_tokens(db_conn, profile_id):
            try:
                token_data = self.__get_service(
                    access_token['service']).get_holdings(
                        db_conn, access_token)

                holdings += token_data['holdings']
                securities += token_data['securities']
                accounts += token_data['accounts']
            except AccessTokenLoginRequiredException as e:
                self._set_access_token_reauth(db_conn, e.access_token)

        self.persist_holding_data(db_conn, profile_id, securities, accounts,
                                  holdings)

        return holdings

    def sync_token_holdings(self, db_conn, access_token):
        try:
            data = self.__get_service(access_token['service']).get_holdings(
                db_conn, access_token)
            holdings = data['holdings']
            self.persist_holding_data(db_conn, access_token['profile_id'],
                                      data['securities'], data['accounts'],
                                      holdings)

            return len(holdings)
        except AccessTokenLoginRequiredException as e:
            self._set_access_token_reauth(db_conn, e.access_token)
            return 0

    def get_transactions(self, db_conn, profile_id, count=500, offset=0):
        transactions = []
        securities = []
        accounts = []

        for access_token in self.__get_access_tokens(db_conn, profile_id):
            try:
                self.sync_institution(db_conn, access_token)
                token_service = self.__get_service(access_token['service'])
                token_data = token_service.get_transactions(db_conn,
                                                            access_token,
                                                            count=count,
                                                            offset=offset)

                transactions += token_data['transactions']
                securities += token_data['securities']
                accounts += token_data['accounts']
            except AccessTokenLoginRequiredException as e:
                self._set_access_token_reauth(db_conn, e.access_token)

        self.persist_transaction_data(db_conn, profile_id, securities,
                                      accounts, transactions)

        return transactions

    def sync_token_transactions(self, db_conn, access_token):
        all_transactions = []
        transactions_count = 0
        count = self.__get_service(
            access_token['service']).max_transactions_limit()
        try:
            for offset in range(0, 1000000, count):
                data = self.__get_service(
                    access_token['service']).get_transactions(db_conn,
                                                              access_token,
                                                              count=count,
                                                              offset=offset)
                cur_transactions = data['transactions']
                all_transactions += cur_transactions
                self.persist_transaction_data(db_conn,
                                              access_token['profile_id'],
                                              data['securities'],
                                              data['accounts'],
                                              cur_transactions)

                transactions_count += len(cur_transactions)
                if len(cur_transactions) < count:
                    break
        except AccessTokenLoginRequiredException as e:
            self._set_access_token_reauth(db_conn, e.access_token)

        # cleanup
        self.portfolio_repository.remove_other_by_access_token(
            db_conn, all_transactions)

        return transactions_count

    def sync_institution(self, db_conn, access_token):
        institution = self.__get_service(
            access_token['service']).get_institution(db_conn, access_token)
        self.portfolio_repository.persist(db_conn, institution)
        self.__get_service(access_token['service']).set_token_institution(
            db_conn, access_token, institution)

    def persist_holding_data(self, db_conn, profile_id, securities, accounts,
                             holdings):
        securities_dict = self.__persist_securities(db_conn, securities)
        accounts_dict = self.__persist_accounts(db_conn, accounts, profile_id)
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
        self.portfolio_repository.persist(db_conn, holdings)

        # cleanup
        self.portfolio_repository.remove_other_by_access_token(
            db_conn, holdings)
        self.portfolio_repository.remove_other_by_access_token(
            db_conn, accounts)

    def persist_transaction_data(self, db_conn, profile_id, securities,
                                 accounts, transactions):
        securities_dict = self.__persist_securities(db_conn, securities)
        accounts_dict = self.__persist_accounts(db_conn, accounts, profile_id)
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
        self.portfolio_repository.persist(db_conn, transactions)

    def __get_service(self, name):
        if name not in self.services:
            raise Error('Service %s not supported' % (name))

        return self.services[name]

    def __persist_securities(self, db_conn, securities):
        self.portfolio_repository.persist(db_conn, self.__unique(securities))
        return {security.ref_id: security.id for security in securities}

    def __persist_accounts(self, db_conn, accounts, profile_id):
        for entity in accounts:
            entity.profile_id = profile_id
        self.portfolio_repository.persist(db_conn, self.__unique(accounts))
        return {account.ref_id: account.id for account in accounts}

    def __unique(self, entities):
        d = {entity.unique_id(): entity for entity in entities}
        return d.values()

    def __get_access_tokens(self, db_conn, profile_id):
        with db_conn.cursor() as cursor:
            cursor.execute(
                f"SELECT id, access_token FROM app.profile_plaid_access_tokens WHERE profile_id = %s",
                (profile_id, ))

            access_tokens = cursor.fetchall()

            return [
                dict(
                    zip(['id', 'access_token', 'service'],
                        row + (SERVICE_PLAID, ))) for row in access_tokens
            ]

    def _set_access_token_reauth(self, db_conn, access_token):
        with db_conn.cursor() as cursor:
            cursor.execute(
                "update app.profile_plaid_access_tokens set needs_reauth_since = now() where id = %(access_token_id)s",
                {"access_token_id": access_token['id']})
