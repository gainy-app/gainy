from common.context_container import ContextContainer
from common.hasura_function import HasuraAction
from exceptions import ForbiddenException
from gainy.trading.models import TradingCollectionVersion
from gainy.utils import get_logger

logger = get_logger(__name__)


class TradingCancelPendingOrder(HasuraAction):

    def __init__(self):
        super().__init__("trading_cancel_pending_order",
                         "profile_id")

    def apply(self, input_params, context_container: ContextContainer):
        profile_id = input_params['profile_id']
        trading_collection_version_id = input_params['trading_collection_version_id']

        trading_collection_version:TradingCollectionVersion = context_container.get_repository().find_one(TradingCollectionVersion, {"id": trading_collection_version_id})
        if trading_collection_version.profile_id != profile_id:
            raise ForbiddenException()

        context_container.trading_service.cancel_pending_order(trading_collection_version)

        return {'trading_collection_version_id': trading_collection_version_id}
