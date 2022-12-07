from decimal import Decimal
from common.context_container import ContextContainer
from common.hasura_function import HasuraAction
from gainy.exceptions import BadRequestException
from gainy.trading.exceptions import InsufficientFundsException
from gainy.trading.models import TradingOrderSource
from gainy.utils import get_logger

logger = get_logger(__name__)


class TradingCreateStockOrder(HasuraAction):

    def __init__(self):
        super().__init__("create_stock_order", "profile_id")

    def apply(self, input_params, context_container: ContextContainer):
        profile_id = input_params['profile_id']
        symbol = input_params['symbol']
        target_amount_delta = Decimal(input_params['target_amount_delta'])

        trading_account_id = context_container.trading_repository.get_trading_account(
            profile_id).id

        trading_service = context_container.trading_service

        try:
            trading_order = trading_service.create_stock_order(
                profile_id,
                TradingOrderSource.MANUAL,
                symbol,
                trading_account_id,
                target_amount_delta=target_amount_delta)
        except InsufficientFundsException as e:
            raise BadRequestException(str(e))

        return {'trading_order_id': trading_order.id}
