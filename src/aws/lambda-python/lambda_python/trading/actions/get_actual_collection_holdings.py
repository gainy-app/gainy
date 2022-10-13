from decimal import Decimal
from common.context_container import ContextContainer
from common.hasura_function import HasuraAction
from gainy.utils import get_logger

logger = get_logger(__name__)


class TradingGetActualCollectionHoldings(HasuraAction):

    def __init__(self):
        super().__init__("trading_get_actual_collection_holdings",
                         "profile_id")

    def apply(self, input_params, context_container: ContextContainer):
        profile_id = input_params['profile_id']
        collection_id = input_params['collection_id']

        trading_service = context_container.trading_service
        actual_collection_holdings = trading_service.get_actual_collection_holdings(
            profile_id, collection_id)

        return [i.to_dict() for i in actual_collection_holdings]
