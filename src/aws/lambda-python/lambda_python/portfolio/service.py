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
            token_data = self.__get_service(
                access_token['service']).get_holdings(db_conn, access_token)

            holdings += token_data['holdings']
            securities += token_data['securities']
            accounts += token_data['accounts']

        self.persist_holding_data(db_conn, profile_id, securities, accounts,
                                  holdings)

        return holdings

    def sync_token_holdings(self, db_conn, access_token):
        data = self.__get_service(access_token['service']).get_holdings(
            db_conn, access_token)
        holdings = data['holdings']
        self.persist_holding_data(db_conn, access_token['profile_id'],
                                  data['securities'], data['accounts'],
                                  holdings)

        return len(holdings)

    def get_transactions(self, db_conn, profile_id, count=100, offset=0):
        transactions = []
        securities = []
        accounts = []

        for access_token in self.__get_access_tokens(db_conn, profile_id):
            token_data = self.__get_service(
                access_token['service']).get_transactions(db_conn,
                                                          plaid_access_token,
                                                          count=count,
                                                          offset=offset)

            transactions += token_data['transactions']
            securities += token_data['securities']
            accounts += token_data['accounts']

        self.persist_transaction_data(db_conn, profile_id, securities,
                                      accounts, transactions)

        return transactions

    def sync_token_transactions(self, db_conn, access_token):
        transactions_count = 0
        count = self.__get_service(
            access_token['service']).max_transactions_limit()
        for offset in range(0, 1000000, count):
            data = self.__get_service(
                access_token['service']).get_transactions(db_conn,
                                                          access_token,
                                                          count=count,
                                                          offset=offset)
            transactions = data['transactions']
            self.persist_transaction_data(db_conn, access_token['profile_id'],
                                          data['securities'], data['accounts'],
                                          transactions)

            transactions_count += len(transactions)
            if len(transactions) < count:
                break

        return transactions_count

    def persist_holding_data(self, db_conn, profile_id, securities, accounts,
                             holdings):
        securities_dict = self.__persist_securities(db_conn, securities)
        accounts_dict = self.__persist_accounts(db_conn, accounts, profile_id)

        # persist holdings
        for entity in holdings:
            entity.profile_id = profile_id
            entity.security_id = securities_dict[entity.security_ref_id]
            entity.account_id = accounts_dict[entity.account_ref_id]
        self.portfolio_repository.persist(db_conn, holdings)

        # cleanup
        self.portfolio_repository.remove_other_by_profile_id(db_conn, holdings)
        self.portfolio_repository.remove_other_by_profile_id(db_conn, accounts)

    def persist_transaction_data(self, db_conn, profile_id, securities,
                                 accounts, transactions):
        securities_dict = self.__persist_securities(db_conn, securities)
        accounts_dict = self.__persist_accounts(db_conn, accounts, profile_id)

        # persist transactions
        for entity in transactions:
            entity.profile_id = profile_id
            entity.security_id = securities_dict[entity.security_ref_id]
            entity.account_id = accounts_dict[entity.account_ref_id]
        self.portfolio_repository.persist(db_conn, transactions)

        # cleanup
        self.portfolio_repository.remove_other_by_profile_id(
            db_conn, transactions)
        self.portfolio_repository.remove_other_by_profile_id(db_conn, accounts)

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
