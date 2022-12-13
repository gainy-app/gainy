from common.context_container import ContextContainer
from common.hasura_function import HasuraAction
from exceptions import ForbiddenException
from gainy.exceptions import BadRequestException
from gainy.trading.models import TradingCollectionVersion, TradingOrder
from gainy.utils import get_logger
from trading.exceptions import WrongTradingOrderStatusException

logger = get_logger(__name__)


def _cancel_trading_collection_version(context_container: ContextContainer,
                                       profile_id: int,
                                       trading_collection_version_id: int):
    trading_collection_version: TradingCollectionVersion = context_container.get_repository(
    ).find_one(TradingCollectionVersion, {"id": trading_collection_version_id})
    if trading_collection_version.profile_id != profile_id:
        raise ForbiddenException()

    context_container.trading_service.cancel_pending_collection_version(
        trading_collection_version)

    return {'trading_collection_version_id': trading_collection_version_id}


def _cancel_trading_order(context_container: ContextContainer, profile_id: int,
                          trading_order_id: int):
    trading_order: TradingOrder = context_container.get_repository().find_one(
        TradingOrder, {"id": trading_order_id})
    if trading_order.profile_id != profile_id:
        raise ForbiddenException()

    context_container.trading_service.cancel_pending_order(trading_order)

    return {'trading_order_id': trading_order_id}


class TradingCancelPendingOrder(HasuraAction):

    def __init__(self):
        super().__init__("trading_cancel_pending_order", "profile_id")

    def apply(self, input_params, context_container: ContextContainer):
        profile_id = input_params['profile_id']
        trading_collection_version_id = input_params.get(
            'trading_collection_version_id')
        trading_order_id = input_params.get('trading_order_id')

        wrong_input_exception = BadRequestException(
            'You must specify exactly one of trading_collection_version_id and trading_order_id.'
        )
        if trading_collection_version_id and trading_order_id:
            raise wrong_input_exception

        try:
            if trading_collection_version_id:
                return _cancel_trading_collection_version(
                    context_container, profile_id,
                    trading_collection_version_id)

            if trading_order_id:
                return _cancel_trading_order(context_container, profile_id,
                                             trading_order_id)

        except WrongTradingOrderStatusException:
            raise BadRequestException(
                'Can not cancel an order not in a pending state.')

        raise wrong_input_exception
