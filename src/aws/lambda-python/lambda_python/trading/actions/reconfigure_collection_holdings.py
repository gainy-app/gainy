from decimal import Decimal
from common.context_container import ContextContainer
from common.hasura_function import HasuraAction
from gainy.exceptions import BadRequestException
from gainy.utils import get_logger
from trading.exceptions import InsufficientFundsException

logger = get_logger(__name__)


class TradingReconfigureCollectionHoldings(HasuraAction):

    def __init__(self):
        super().__init__("trading_reconfigure_collection_holdings",
                         "profile_id")

    def apply(self, input_params, context_container: ContextContainer):
        profile_id = input_params['profile_id']
        collection_id = input_params['collection_id']
        weights = input_params['weights']
        target_amount_delta = Decimal(input_params['target_amount_delta'])

        if not weights:
            weights = context_container.trading_repository.get_collection_actual_weights(
                collection_id)
        weights = {i["symbol"]: Decimal(i["weight"]) for i in weights}

        trading_service = context_container.trading_service

        try:
            trading_collection_version = trading_service.reconfigure_collection_holdings(
                profile_id, collection_id, weights, target_amount_delta)
        except InsufficientFundsException as e:
            raise BadRequestException(str(e))

        return {'trading_collection_version_id': trading_collection_version.id}
