from portfolio.service import PortfolioService
from portfolio.models import PortfolioChartFilter
from common.hasura_function import HasuraAction
from service.logging import get_logger

logger = get_logger(__name__)


class GetPortfolioHoldings(HasuraAction):

    def __init__(self):
        super().__init__("get_portfolio_holdings", "profile_id")

        self.service = PortfolioService()

    def apply(self, db_conn, input_params, headers):
        profile_id = input_params["profile_id"]
        try:
            holdings = self.service.get_holdings(db_conn, profile_id)
        except Exception as e:
            logger.error(e, exc_info=True)
            holdings = []

        return [i.normalize() for i in holdings]


class GetPortfolioTransactions(HasuraAction):

    def __init__(self):
        super().__init__("get_portfolio_transactions", "profile_id")

        self.service = PortfolioService()

    def apply(self, db_conn, input_params, headers):
        profile_id = input_params["profile_id"]
        count = input_params.get("count", 500)
        offset = input_params.get("offset", 0)

        try:
            transactions = self.service.get_transactions(db_conn,
                                                         profile_id,
                                                         count=count,
                                                         offset=offset)
        except Exception as e:
            logger.error(e, exc_info=True)
            transactions = []

        return [i.normalize() for i in transactions]


class GetPortfolioChart(HasuraAction):

    def __init__(self):
        super().__init__("get_portfolio_chart", "profile_id")

        self.service = PortfolioService()

    def apply(self, db_conn, input_params, headers):
        profile_id = input_params["profile_id"]

        filter = PortfolioChartFilter()
        filter.periods = input_params.get("periods")
        filter.account_ids = input_params.get("account_ids")
        filter.institution_ids = input_params.get("institution_ids")
        filter.interest_ids = input_params.get("interest_ids")
        filter.category_ids = input_params.get("category_ids")
        filter.security_types = input_params.get("security_types")
        filter.ltt_only = input_params.get("ltt_only")

        chart = self.service.get_portfolio_chart(db_conn, profile_id, filter)
        for row in chart:
            row['datetime'] = row['datetime'].isoformat()

        return chart
