from portfolio.models import PortfolioChartFilter
from common.context_container import ContextContainer
from common.hasura_function import HasuraAction


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

        _filter = PortfolioChartFilter()
        _filter.access_token_ids = input_params.get("access_token_ids")
        _filter.broker_ids = input_params.get("broker_ids")
        _filter.interest_ids = input_params.get("interest_ids")
        _filter.category_ids = input_params.get("category_ids")

        service = context_container.portfolio_chart_service
        return service.get_portfolio_piechart(profile_id, _filter)
