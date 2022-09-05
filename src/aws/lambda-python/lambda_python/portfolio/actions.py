from portfolio.models import PortfolioChartFilter
from common.context_container import ContextContainer
from common.hasura_function import HasuraAction
from gainy.utils import get_logger

logger = get_logger(__name__)


class GetPortfolioHoldings(HasuraAction):

    def __init__(self):
        super().__init__("get_portfolio_holdings", "profile_id")

    def apply(self, input_params, context_container: ContextContainer):
        service = context_container.portfolio_service
        profile_id = input_params["profile_id"]
        try:
            holdings = service.get_holdings(profile_id)
        except Exception as e:
            logger.exception(e)
            holdings = []

        return [i.normalize() for i in holdings]


class GetPortfolioTransactions(HasuraAction):

    def __init__(self):
        super().__init__("get_portfolio_transactions", "profile_id")

    def apply(self, input_params, context_container: ContextContainer):
        service = context_container.portfolio_service
        profile_id = input_params["profile_id"]
        count = input_params.get("count", 500)
        offset = input_params.get("offset", 0)

        try:
            transactions = service.get_transactions(profile_id,
                                                    count=count,
                                                    offset=offset)
        except Exception as e:
            logger.exception(e)
            transactions = []

        return [i.normalize() for i in transactions]


class GetPortfolioChart(HasuraAction):

    def __init__(self):
        super().__init__("get_portfolio_chart", "profile_id")

    def get_allowed_profile_ids(self, input_params):
        profile_id = self.get_profile_id(input_params)

        if profile_id == 1:
            return None

        return profile_id

    def apply(self, input_params, context_container: ContextContainer):
        profile_id = input_params["profile_id"]

        filter = PortfolioChartFilter()
        filter.periods = input_params.get("periods")
        filter.access_token_ids = input_params.get("access_token_ids")
        filter.account_ids = input_params.get("account_ids")
        filter.institution_ids = input_params.get("institution_ids")
        filter.interest_ids = input_params.get("interest_ids")
        filter.category_ids = input_params.get("category_ids")
        filter.security_types = input_params.get("security_types")
        filter.ltt_only = input_params.get("ltt_only")

        service = context_container.portfolio_chart_service
        chart = service.get_portfolio_chart(profile_id, filter)
        for row in chart:
            row['datetime'] = row['datetime'].isoformat()

        return chart


class GetPortfolioChartPreviousPeriodClose(HasuraAction):

    def __init__(self):
        super().__init__("get_portfolio_chart_previous_period_close",
                         "profile_id")

    def get_allowed_profile_ids(self, input_params):
        profile_id = self.get_profile_id(input_params)

        if profile_id == 1:
            return None

        return profile_id

    def apply(self, input_params, context_container: ContextContainer):
        profile_id = input_params["profile_id"]

        filter = PortfolioChartFilter()
        filter.periods = input_params.get("periods")
        filter.access_token_ids = input_params.get("access_token_ids")
        filter.account_ids = input_params.get("account_ids")
        filter.institution_ids = input_params.get("institution_ids")
        filter.interest_ids = input_params.get("interest_ids")
        filter.category_ids = input_params.get("category_ids")
        filter.security_types = input_params.get("security_types")
        filter.ltt_only = input_params.get("ltt_only")

        service = context_container.portfolio_chart_service
        return service.get_portfolio_chart_previous_period_close(
            profile_id, filter)


class GetPortfolioPieChart(HasuraAction):

    def __init__(self):
        super().__init__("get_portfolio_piechart", "profile_id")

    def get_allowed_profile_ids(self, input_params):
        profile_id = self.get_profile_id(input_params)

        if profile_id == 1:
            return None

        return profile_id

    def apply(self, input_params, context_container: ContextContainer):
        profile_id = input_params["profile_id"]

        filter = PortfolioChartFilter()
        filter.access_token_ids = input_params.get("access_token_ids")

        service = context_container.portfolio_chart_service
        return service.get_portfolio_piechart(profile_id, filter)
