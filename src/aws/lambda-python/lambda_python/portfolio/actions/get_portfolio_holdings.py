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
