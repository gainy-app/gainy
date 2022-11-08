from common.context_container import ContextContainer
from common.hasura_function import HasuraAction
from gainy.utils import get_logger

logger = get_logger(__name__)


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
