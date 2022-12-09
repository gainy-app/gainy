from decimal import Decimal
from common.context_container import ContextContainer
from common.hasura_function import HasuraAction
from gainy.trading.models import TradingOrderSource
from gainy.utils import get_logger

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

        trading_account_id = context_container.trading_repository.get_trading_account(
            profile_id).id

        trading_service = context_container.trading_service

        trading_collection_version = trading_service.create_collection_version(
            profile_id,
            TradingOrderSource.MANUAL,
            collection_id,
            trading_account_id,
            weights=weights,
            target_amount_delta=target_amount_delta)

        return {'trading_collection_version_id': trading_collection_version.id}
