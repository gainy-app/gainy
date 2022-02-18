import os
from portfolio.plaid import PlaidService
from portfolio.repository import PortfolioRepository
from psycopg2 import sql
from psycopg2.extras import RealDictCursor
from service.logging import get_logger

logger = get_logger(__name__)

SERVICE_PLAID = 'plaid'
SCRIPT_DIR = os.path.dirname(__file__)


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

    def get_transactions(self, db_conn, profile_id, count=500, offset=0):
        transactions = []
        securities = []
        accounts = []

        for access_token in self.__get_access_tokens(db_conn, profile_id):
            self.sync_institution(db_conn, access_token)
            token_service = self.__get_service(access_token['service'])
            token_data = token_service.get_transactions(db_conn,
                                                        access_token,
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
        transactions = self.__unique(transactions)

        # persist transactions
        for entity in transactions:
            entity.profile_id = profile_id
            entity.security_id = securities_dict[entity.security_ref_id]
            entity.account_id = accounts_dict[entity.account_ref_id]
        self.portfolio_repository.persist(db_conn, transactions)

    def get_portfolio_chart(self, db_conn, profile_id, filter):
        with open(os.path.join(SCRIPT_DIR, "sql/portfolio_chart.sql")) as f:
            portfolio_chart_query = f.read()

        params = {
            "profile_id": profile_id,
        }
        where_clause = [
            sql.SQL(
                "portfolio_expanded_transactions.profile_id = %(profile_id)s")
        ]
        join_clause = []

        if filter.periods is not None and len(filter.periods):
            where_clause.append(
                sql.SQL("portfolio_transaction_chart.period in %(periods)s"))
            params['periods'] = tuple(filter.periods)

        if filter.account_ids is not None or filter.institution_ids is not None:
            join_clause.append(
                sql.SQL(
                    "join app.profile_portfolio_accounts on profile_portfolio_accounts.id = portfolio_expanded_transactions.account_id"
                ))

            if filter.account_ids is not None and len(filter.account_ids):
                where_clause.append(
                    sql.SQL(
                        "portfolio_expanded_transactions.account_id in %(account_ids)s"
                    ))
                params['account_ids'] = tuple(filter.account_ids)

            if filter.institution_ids is not None and len(
                    filter.institution_ids):
                join_clause.append(
                    sql.SQL(
                        "join app.profile_plaid_access_tokens on profile_plaid_access_tokens.id = profile_portfolio_accounts.plaid_access_token_id"
                    ))
                where_clause.append(
                    sql.SQL(
                        "profile_plaid_access_tokens.institution_id in %(institution_ids)s"
                    ))
                params['institution_ids'] = tuple(filter.institution_ids)

        if filter.interest_ids is not None or filter.category_ids is not None or filter.security_types is not None:
            join_clause.append(
                sql.SQL(
                    "join portfolio_securities_normalized on portfolio_securities_normalized.id = portfolio_expanded_transactions.security_id"
                ))

            if filter.interest_ids is not None and len(filter.interest_ids):
                join_clause.append(
                    sql.SQL(
                        "join ticker_interests on ticker_interests.symbol = portfolio_securities_normalized.ticker_symbol"
                    ))
                where_clause.append(sql.SQL("interest_id in %(interest_ids)s"))
                params['interest_ids'] = tuple(filter.interest_ids)

            if filter.category_ids is not None and len(filter.category_ids):
                join_clause.append(
                    sql.SQL(
                        "join ticker_categories on ticker_categories.symbol = portfolio_securities_normalized.ticker_symbol"
                    ))
                where_clause.append(sql.SQL("category_id in %(category_ids)s"))
                params['category_ids'] = tuple(filter.category_ids)

            if filter.security_types is not None and len(
                    filter.security_types):
                where_clause.append(
                    sql.SQL(
                        "portfolio_securities_normalized.type in %(security_types)s"
                    ))
                params['security_types'] = tuple(filter.security_types)

        if filter.ltt_only is not None and filter.ltt_only:
            join_clause.append(
                sql.SQL(
                    "join app.profile_holdings on profile_holdings.profile_id = portfolio_expanded_transactions.profile_id and profile_holdings.security_id = portfolio_expanded_transactions.security_id"
                ))
            join_clause.append(
                sql.SQL(
                    "join portfolio_holding_details on portfolio_holding_details.holding_id = profile_holdings.id"
                ))
            where_clause.append(
                sql.SQL("portfolio_holding_details.ltt_quantity_total > 0"))

        join_clause = sql.SQL("\n").join(join_clause)
        where_clause = sql.SQL('and ') + sql.SQL(' and ').join(where_clause)
        query = sql.SQL(portfolio_chart_query).format(
            where_clause=where_clause, join_clause=join_clause)
        logger.debug(query.as_string(db_conn))

        with db_conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(query, params)
            rows = cursor.fetchall()

        rows = list(self._filter_chart_by_transaction_count(rows))

        return self._add_static_values_to_chart(db_conn, profile_id, filter,
                                                rows)

    def _filter_chart_by_transaction_count(self, rows):
        transaction_counts_1d = {}
        for row in rows:
            if row['period'] != '1d':
                continue

            if row['transaction_count'] not in transaction_counts_1d:
                transaction_counts_1d[row['transaction_count']] = 0

            transaction_counts_1d[row['transaction_count']] += 1

        max_transaction_count_1d = max(
            transaction_counts_1d, key=transaction_counts_1d.get
        ) if len(transaction_counts_1d) > 0 else 0

        prev_row = None
        for row in sorted(rows,
                          key=lambda row: (row['period'], row['datetime'])):

            transaction_count = row['transaction_count']
            period = row['period']

            # during the day there is constant transaction_count, so we just pick all rows with max transaction_count
            should_skip_1d = transaction_count != max_transaction_count_1d
            if period == '1d' and should_skip_1d:
                continue

            # for other periods transactions count should not decrease, so we pick all rows that follow a non-decreasing transaction count pattern
            if prev_row is not None:
                prev_transaction_count = prev_row['transaction_count']
                prev_period = prev_row['period']
                should_skip_other_periods = period == prev_period and transaction_count < prev_transaction_count
                if period != '1d' and should_skip_other_periods:
                    continue

            yield row

    def _add_static_values_to_chart(self, db_conn, profile_id, filter, rows):
        with open(os.path.join(SCRIPT_DIR,
                               "sql/portfolio_chart_static.sql")) as f:
            portfolio_chart_query = f.read()

        params = {
            "profile_id": profile_id,
        }
        where_clause = [
            sql.SQL("profile_holdings_normalized.profile_id = %(profile_id)s")
        ]
        join_clause = []

        if filter.account_ids is not None or filter.institution_ids is not None:
            join_clause.append(
                sql.SQL(
                    "join app.profile_portfolio_accounts on profile_portfolio_accounts.id = profile_holdings_normalized.account_id"
                ))

            if filter.account_ids is not None and len(filter.account_ids):
                where_clause.append(
                    sql.SQL(
                        "profile_holdings_normalized.account_id in %(account_ids)s"
                    ))
                params['account_ids'] = tuple(filter.account_ids)

            if filter.institution_ids is not None and len(
                    filter.institution_ids):
                join_clause.append(
                    sql.SQL(
                        "join app.profile_plaid_access_tokens on profile_plaid_access_tokens.id = profile_portfolio_accounts.plaid_access_token_id"
                    ))
                where_clause.append(
                    sql.SQL(
                        "profile_plaid_access_tokens.institution_id in %(institution_ids)s"
                    ))
                params['institution_ids'] = tuple(filter.institution_ids)

        if filter.interest_ids is not None and len(filter.interest_ids):
            join_clause.append(
                sql.SQL(
                    "join ticker_interests on ticker_interests.symbol = portfolio_securities_normalized.ticker_symbol"
                ))
            where_clause.append(sql.SQL("(interest_id in %(interest_ids)s) or portfolio_securities_normalized.type = 'cash'"))
            params['interest_ids'] = tuple(filter.interest_ids)

        if filter.category_ids is not None and len(filter.category_ids):
            join_clause.append(
                sql.SQL(
                    "join ticker_categories on ticker_categories.symbol = portfolio_securities_normalized.ticker_symbol"
                ))
            where_clause.append(sql.SQL("(category_id in %(category_ids)s) or portfolio_securities_normalized.type = 'cash'"))
            params['category_ids'] = tuple(filter.category_ids)

        if filter.security_types is not None and len(filter.security_types):
            where_clause.append(
                sql.SQL(
                    "portfolio_securities_normalized.type in %(security_types)s"
                ))
            params['security_types'] = tuple(filter.security_types)

        if filter.ltt_only is not None and filter.ltt_only:
            join_clause.append(
                sql.SQL(
                    "join portfolio_holding_details on portfolio_holding_details.holding_id = profile_holdings_normalized.holding_id"
                ))
            where_clause.append(
                sql.SQL("portfolio_holding_details.ltt_quantity_total > 0"))

        join_clause = sql.SQL("\n").join(join_clause)
        where_clause = sql.SQL('and ') + sql.SQL(' and ').join(where_clause)
        query = sql.SQL(portfolio_chart_query).format(
            where_clause=where_clause, join_clause=join_clause)
        logger.debug(query.as_string(db_conn))

        with db_conn.cursor() as cursor:
            cursor.execute(query, params)
            static_data = cursor.fetchall()

        if not len(static_data) or static_data[0][0] is None:
            return rows

        value = float(static_data[0][0])
        for i in rows:
            i['open'] += value
            i['high'] += value
            i['low'] += value
            i['close'] += value
            i['adjusted_close'] += value

        return rows

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
