import json
import os
from psycopg2 import sql
from psycopg2.extras import RealDictCursor
from service.logging import get_logger

logger = get_logger(__name__)

SCRIPT_DIR = os.path.dirname(__file__)


class PortfolioChartService:

    def get_portfolio_chart(self, db_conn, profile_id, filter):
        with open(os.path.join(SCRIPT_DIR, "../sql/portfolio_chart.sql")) as f:
            query = f.read()

        params = {}
        where_clause = []
        join_clause = []

        if self._should_return_empty_result(filter):
            return []

        self._filter_query_by_profile_id(params, where_clause, join_clause,
                                         profile_id)
        self._filter_query_by_periods(params, where_clause, join_clause,
                                      filter)
        self._filter_query_by_account_ids(params, where_clause, join_clause,
                                          filter)
        self._filter_query_by_institution_ids(params, where_clause,
                                              join_clause, filter)
        self._filter_query_by_access_token_ids(params, where_clause,
                                               join_clause, filter)
        self._filter_query_by_interest_ids(params, where_clause, join_clause,
                                           filter)
        self._filter_query_by_category_ids(params, where_clause, join_clause,
                                           filter)
        self._filter_query_by_security_types(params, where_clause, join_clause,
                                             filter)
        self._filter_query_by_ltt_only(params, where_clause, join_clause,
                                       filter)

        rows = self._execute_query(params, where_clause, join_clause, query,
                                   db_conn)

        rows = list(self._filter_chart_by_transaction_count(rows))
        if not rows:
            return []

        static_value = self._get_chart_static_value(db_conn, profile_id,
                                                    filter)
        if static_value:
            for i in rows:
                i['open'] += static_value
                i['high'] += static_value
                i['low'] += static_value
                i['close'] += static_value
                i['adjusted_close'] += static_value

        if max(row['adjusted_close'] for row in rows) < 1e-3:
            return []

        return rows

    def get_portfolio_chart_previous_period_close(self, db_conn, profile_id,
                                                  filter):
        with open(
                os.path.join(SCRIPT_DIR,
                             "../sql/portfolio_chart_prev_close.sql")) as f:
            query = f.read()

        params = {}
        where_clause = []
        join_clause = []

        if self._should_return_empty_result(filter):
            return []

        self._filter_query_by_profile_id(params, where_clause, join_clause,
                                         profile_id)
        self._filter_query_by_periods(params, where_clause, join_clause,
                                      filter)
        self._filter_query_by_account_ids(params, where_clause, join_clause,
                                          filter)
        self._filter_query_by_institution_ids(params, where_clause,
                                              join_clause, filter)
        self._filter_query_by_access_token_ids(params, where_clause,
                                               join_clause, filter)
        self._filter_query_by_interest_ids(params, where_clause, join_clause,
                                           filter)
        self._filter_query_by_category_ids(params, where_clause, join_clause,
                                           filter)
        self._filter_query_by_security_types(params, where_clause, join_clause,
                                             filter)
        self._filter_query_by_ltt_only(params, where_clause, join_clause,
                                       filter)

        data = self._execute_query(params, where_clause, join_clause, query,
                                   db_conn)

        if not data:
            return {}
        data = data[0]

        static_value = self._get_chart_static_value(db_conn, profile_id,
                                                    filter)
        if static_value:
            for k in data:
                if data[k] is None:
                    continue
                data[k] += static_value

        return data

    def get_portfolio_piechart(self, db_conn, profile_id, filter):
        with open(os.path.join(SCRIPT_DIR,
                               "../sql/portfolio_piechart.sql")) as f:
            query = f.read()

        params = {}
        where_clause = []
        join_clause = []

        if self._should_return_empty_result(filter):
            return []

        self._filter_query_by_profile_id(params, where_clause, join_clause,
                                         profile_id)
        self._filter_query_by_access_token_ids(params, where_clause,
                                               join_clause, filter)

        rows = self._execute_query(params, where_clause, join_clause, query,
                                   db_conn)

        return rows

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
                prev_row = row
                continue

            # for other periods transactions count should not decrease, so we pick all rows that follow a non-decreasing transaction count pattern
            if prev_row is not None:
                prev_transaction_count = prev_row['transaction_count']
                prev_period = prev_row['period']
                should_skip_other_periods = period == prev_period and transaction_count < prev_transaction_count
                if period != '1d' and should_skip_other_periods:
                    continue

            prev_row = row
            yield row

    def _get_chart_static_value(self, db_conn, profile_id, filter):
        with open(os.path.join(SCRIPT_DIR,
                               "../sql/portfolio_chart_static.sql")) as f:
            query = f.read()

        params = {}
        where_clause = []
        join_clause = []

        self._filter_query_by_profile_id(params, where_clause, join_clause,
                                         profile_id)
        self._filter_query_by_account_ids(params, where_clause, join_clause,
                                          filter)
        self._filter_query_by_institution_ids(params, where_clause,
                                              join_clause, filter)
        self._filter_query_by_access_token_ids(params, where_clause,
                                               join_clause, filter)
        self._filter_query_by_interest_ids(params, where_clause, join_clause,
                                           filter)
        self._filter_query_by_category_ids(params, where_clause, join_clause,
                                           filter)
        self._filter_query_by_security_types(params, where_clause, join_clause,
                                             filter)
        self._filter_query_by_ltt_only(params, where_clause, join_clause,
                                       filter)

        static_data = self._execute_query(params, where_clause, join_clause,
                                          query, db_conn)

        if not len(static_data) or static_data[0]['value'] is None:
            return 0

        return float(static_data[0]['value'])

    def _should_return_empty_result(self, filter):
        if filter.periods is not None and not len(filter.periods):
            return True
        if filter.account_ids is not None and not len(filter.account_ids):
            return True
        if filter.institution_ids is not None and not len(
                filter.institution_ids):
            return True
        if filter.access_token_ids is not None and not len(
                filter.access_token_ids):
            return True
        if filter.interest_ids is not None and not len(filter.interest_ids):
            return True
        if filter.category_ids is not None and not len(filter.category_ids):
            return True
        if filter.security_types is not None and not len(
                filter.security_types):
            return True

        return False

    def _filter_query_by_profile_id(self, params, where_clause, join_clause,
                                    profile_id):
        params["profile_id"] = profile_id,
        where_clause.append(
            sql.SQL("profile_plaid_access_tokens.profile_id = %(profile_id)s"))

    def _filter_query_by_periods(self, params, where_clause, join_clause,
                                 filter):
        if not filter.periods:
            return

        where_clause.append(
            sql.SQL("portfolio_transaction_chart.period in %(periods)s"))
        params['periods'] = tuple(filter.periods)

    def _filter_query_by_account_ids(self, params, where_clause, join_clause,
                                     filter):
        if not filter.account_ids:
            return

        where_clause.append(
            sql.SQL("profile_portfolio_accounts.id in %(account_ids)s"))
        params['account_ids'] = tuple(filter.account_ids)

    def _filter_query_by_institution_ids(self, params, where_clause,
                                         join_clause, filter):
        if not filter.institution_ids:
            return

        where_clause.append(
            sql.SQL(
                "profile_plaid_access_tokens.institution_id in %(institution_ids)s"
            ))
        params['institution_ids'] = tuple(filter.institution_ids)

    def _filter_query_by_access_token_ids(self, params, where_clause,
                                          join_clause, filter):
        if not filter.access_token_ids:
            return
        where_clause.append(
            sql.SQL("profile_plaid_access_tokens.id in %(access_token_ids)s"))
        params['access_token_ids'] = tuple(filter.access_token_ids)

    def _filter_query_by_interest_ids(self, params, where_clause, join_clause,
                                      filter):
        if not filter.interest_ids:
            return

        join_clause.append(
            sql.SQL(
                "join ticker_interests on ticker_interests.symbol = portfolio_securities_normalized.ticker_symbol"
            ))
        where_clause.append(sql.SQL("interest_id in %(interest_ids)s"))
        params['interest_ids'] = tuple(filter.interest_ids)

    def _filter_query_by_category_ids(self, params, where_clause, join_clause,
                                      filter):
        if not filter.category_ids:
            return

        join_clause.append(
            sql.SQL(
                "join ticker_categories on ticker_categories.symbol = portfolio_securities_normalized.ticker_symbol"
            ))
        where_clause.append(sql.SQL("category_id in %(category_ids)s"))
        params['category_ids'] = tuple(filter.category_ids)

    def _filter_query_by_security_types(self, params, where_clause,
                                        join_clause, filter):
        if not filter.security_types:
            return

        where_clause.append(
            sql.SQL(
                "portfolio_securities_normalized.type in %(security_types)s"))
        params['security_types'] = tuple(filter.security_types)

    def _filter_query_by_ltt_only(self, params, where_clause, join_clause,
                                  filter):
        if not filter.ltt_only:
            return
        join_clause.append(
            sql.SQL(
                "join app.profile_holdings on profile_holdings.profile_id = profile_plaid_access_tokens.profile_id and profile_holdings.security_id = portfolio_securities_normalized.id and profile_holdings.account_id = profile_portfolio_accounts.id"
            ))
        join_clause.append(
            sql.SQL(
                "join portfolio_holding_details on portfolio_holding_details.holding_id = profile_holdings.id"
            ))
        where_clause.append(
            sql.SQL("portfolio_holding_details.ltt_quantity_total > 0"))

    def _execute_query(self, params, where_clause, join_clause, query,
                       db_conn):
        join_clause = sql.SQL("\n").join(join_clause)
        where_clause = sql.SQL(' and ').join(where_clause)
        query = sql.SQL(query).format(where_clause=where_clause,
                                      join_clause=join_clause)
        logger.debug(json.dumps(query.as_string(db_conn)))

        with db_conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(query, params)
            return cursor.fetchall()
