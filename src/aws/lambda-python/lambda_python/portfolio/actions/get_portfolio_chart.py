from portfolio.models import PortfolioChartFilter
from common.context_container import ContextContainer
from common.hasura_function import HasuraAction


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

        _filter = PortfolioChartFilter()
        _filter.periods = input_params.get("periods")
        _filter.access_token_ids = input_params.get("access_token_ids")
        _filter.broker_ids = input_params.get("broker_ids")
        _filter.institution_ids = input_params.get("institution_ids")
        _filter.interest_ids = input_params.get("interest_ids")
        _filter.category_ids = input_params.get("category_ids")
        _filter.security_types = input_params.get("security_types")
        _filter.ltt_only = input_params.get("ltt_only")

        service = context_container.portfolio_chart_service
        chart = service.get_portfolio_chart(profile_id, _filter)
        for row in chart:
            row['datetime'] = row['datetime'].isoformat()

        return chart
