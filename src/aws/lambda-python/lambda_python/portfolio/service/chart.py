import os
from psycopg2 import sql
from psycopg2.extras import RealDictCursor
from gainy.utils import get_logger
from portfolio.models import PortfolioChartFilter

logger = get_logger(__name__)

SCRIPT_DIR = os.path.dirname(__file__)


class PortfolioChartService:

    def __init__(self, db_conn):
        self.db_conn = db_conn

    def get_portfolio_chart(self, profile_id, filter: PortfolioChartFilter):
        with open(os.path.join(SCRIPT_DIR, "../sql/portfolio_chart.sql")) as f:
            query = f.read()

        params = {"profile_id": profile_id}
        transaction_where_clause = []
        chart_where_clause = []
        join_clause = []

        if self._should_return_empty_result(filter):
            return []

        self._filter_query_by_periods(params, chart_where_clause, None, filter)
        self._filter_query_by_institution_ids(params, transaction_where_clause,
                                              join_clause, filter)
        self._filter_query_by_access_token_ids(params,
                                               transaction_where_clause,
                                               join_clause, filter)
        self._filter_query_by_broker_ids(params, transaction_where_clause,
                                         join_clause, filter)
        self._filter_query_by_interest_ids(params, transaction_where_clause,
                                           join_clause, filter)
        self._filter_query_by_category_ids(params, transaction_where_clause,
                                           join_clause, filter)
        self._filter_query_by_security_types(params, transaction_where_clause,
                                             join_clause, filter)
        self._filter_query_by_ltt_only(params, transaction_where_clause,
                                       join_clause, filter)

        if transaction_where_clause:
            transaction_where_clause.insert(0, sql.SQL(""))
        if chart_where_clause:
            chart_where_clause.insert(0, sql.SQL(""))

        rows = self._execute_query(
            params, {
                "transaction_where_clause": transaction_where_clause,
                "chart_where_clause": chart_where_clause,
            }, join_clause, query)

        rows = list(self._filter_chart_by_transaction_count(rows))
        if not rows:
            return []

        if max(row['adjusted_close'] for row in rows) < 1e-3:
            return []

        return rows

    def get_portfolio_chart_previous_period_close(
            self, profile_id, filter: PortfolioChartFilter):
        with open(
                os.path.join(SCRIPT_DIR,
                             "../sql/portfolio_chart_prev_close.sql")) as f:
            query = f.read()

        params = {"profile_id": profile_id}
        transaction_where_clause = []
        chart_where_clause = []
        join_clause = []

        if self._should_return_empty_result(filter):
            return []

        self._filter_query_by_periods(params, chart_where_clause, None, filter)
        self._filter_query_by_institution_ids(params, transaction_where_clause,
                                              join_clause, filter)
        self._filter_query_by_access_token_ids(params,
                                               transaction_where_clause,
                                               join_clause, filter)
        self._filter_query_by_broker_ids(params, transaction_where_clause,
                                         join_clause, filter)
        self._filter_query_by_interest_ids(params, transaction_where_clause,
                                           join_clause, filter)
        self._filter_query_by_category_ids(params, transaction_where_clause,
                                           join_clause, filter)
        self._filter_query_by_security_types(params, transaction_where_clause,
                                             join_clause, filter)
        self._filter_query_by_ltt_only(params, transaction_where_clause,
                                       join_clause, filter)

        if transaction_where_clause:
            transaction_where_clause.insert(0, sql.SQL(""))
        if chart_where_clause:
            chart_where_clause.insert(0, sql.SQL(""))

        data = self._execute_query(
            params, {
                "transaction_where_clause": transaction_where_clause,
                "chart_where_clause": chart_where_clause,
            }, join_clause, query)

        if not data:
            return {
                'prev_close_1d': None,
                'prev_close_1w': None,
                'prev_close_1m': None,
                'prev_close_3m': None,
                'prev_close_1y': None,
                'prev_close_5y': None,
            }

        return data[0]

    def get_portfolio_piechart(self, profile_id, filter: PortfolioChartFilter):
        with open(os.path.join(SCRIPT_DIR,
                               "../sql/portfolio_piechart.sql")) as f:
            query = f.read()

        params = {}
        where_clause = []
        join_clause = []

        if self._should_return_empty_result(filter):
            return []

        self._filter_query_by_profile_id(params, where_clause, join_clause,
                                         profile_id,
                                         "profile_holdings_normalized")
        self._filter_query_by_access_token_ids(params, where_clause,
                                               join_clause, filter)
        self._filter_query_by_broker_ids(params, where_clause, join_clause,
                                         filter)

        rows = self._execute_query(params, {"where_clause": where_clause},
                                   join_clause, query)

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

    def _should_return_empty_result(self, filter: PortfolioChartFilter):
        if filter.periods is not None and not len(filter.periods):
            return True
        if filter.institution_ids is not None and not len(
                filter.institution_ids):
            return True
        if filter.access_token_ids is not None and not len(
                filter.access_token_ids):
            return True
        if filter.broker_ids is not None and not len(filter.broker_ids):
            return True
        if filter.interest_ids is not None and not len(filter.interest_ids):
            return True
        if filter.category_ids is not None and not len(filter.category_ids):
            return True
        if filter.security_types is not None and not len(
                filter.security_types):
            return True

        return False

    def _filter_query_by_periods(self, params, where_clause, join_clause,
                                 filter: PortfolioChartFilter):
        if not filter.periods:
            return

        where_clause.append(sql.SQL("period in %(periods)s"))
        params['periods'] = tuple(filter.periods)

    def _filter_query_by_institution_ids(self, params, where_clause,
                                         join_clause,
                                         filter: PortfolioChartFilter):
        if not filter.institution_ids:
            return

        where_clause.append(sql.SQL("institution_id in %(institution_ids)s"))
        params['institution_ids'] = tuple(filter.institution_ids)

    def _filter_query_by_access_token_ids(self, params, where_clause,
                                          join_clause,
                                          filter: PortfolioChartFilter):
        if not filter.access_token_ids:
            return
        where_clause.append(
            sql.SQL("plaid_access_token_id in %(access_token_ids)s"))
        params['access_token_ids'] = tuple(filter.access_token_ids)

    def _filter_query_by_broker_ids(self, params, where_clause, join_clause,
                                    filter: PortfolioChartFilter):
        if not filter.broker_ids:
            return

        where_clause.append(
            sql.SQL(
                "profile_holdings_normalized.broker_uniq_id in %(broker_ids)s")
        )
        params['broker_ids'] = tuple(filter.broker_ids)

    def _filter_query_by_interest_ids(self, params, where_clause, join_clause,
                                      filter: PortfolioChartFilter):
        if not filter.interest_ids:
            return

        join_clause.append(
            sql.SQL(
                "join ticker_interests on ticker_interests.symbol = portfolio_expanded_transactions.symbol"
            ))
        where_clause.append(sql.SQL("interest_id in %(interest_ids)s"))
        params['interest_ids'] = tuple(filter.interest_ids)

    def _filter_query_by_category_ids(self, params, where_clause, join_clause,
                                      filter: PortfolioChartFilter):
        if not filter.category_ids:
            return

        join_clause.append(
            sql.SQL(
                "join ticker_categories on ticker_categories.symbol = portfolio_expanded_transactions.symbol"
            ))
        where_clause.append(sql.SQL("category_id in %(category_ids)s"))
        params['category_ids'] = tuple(filter.category_ids)

    def _filter_query_by_security_types(self, params, where_clause,
                                        join_clause,
                                        filter: PortfolioChartFilter):
        if not filter.security_types:
            return

        where_clause.append(
            sql.SQL(
                "portfolio_expanded_transactions.security_type in %(security_types)s"
            ))
        params['security_types'] = tuple(filter.security_types)

    def _filter_query_by_ltt_only(self, params, where_clause, join_clause,
                                  filter: PortfolioChartFilter):
        if not filter.ltt_only:
            return
        join_clause.append(
            sql.SQL("join portfolio_holding_details using (holding_id_v2)"))
        where_clause.append(
            sql.SQL("portfolio_holding_details.ltt_quantity_total > 0"))

    def _execute_query(self, params, where_clauses: dict, join_clause, query):
        format_params = {
            k: sql.SQL(' and ').join(i)
            for k, i in where_clauses.items()
        }
        format_params["join_clause"] = sql.SQL("\n").join(join_clause)

        query = sql.SQL(query).format(**format_params)

        with self.db_conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(query, params)
            return cursor.fetchall()
