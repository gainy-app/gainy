from decimal import Decimal
from common.context_container import ContextContainer
from common.hasura_function import HasuraAction
from gainy.exceptions import BadRequestException, InsufficientFundsHttpException
from gainy.trading.exceptions import InsufficientFundsException, TradingPausedException, \
    InsufficientHoldingValueException
from gainy.trading.models import TradingOrderSource
from gainy.utils import get_logger

logger = get_logger(__name__)


class TradingCreateStockOrder(HasuraAction):

    def __init__(self):
        super().__init__("trading_create_stock_order", "profile_id")

    def apply(self, input_params, context_container: ContextContainer):
        profile_id = input_params['profile_id']
        symbol = input_params['symbol']
        target_amount_delta = input_params.get('target_amount_delta')
        target_amount_delta = Decimal(
            target_amount_delta) if target_amount_delta else None
        target_amount_delta_relative = input_params.get(
            'target_amount_delta_relative')
        target_amount_delta_relative = Decimal(
            target_amount_delta_relative
        ) if target_amount_delta_relative else None

        if target_amount_delta_relative:
            if target_amount_delta:
                raise BadRequestException(
                    'Only one of target_amount_delta and target_amount_delta_relative must be specified.'
                )
            if target_amount_delta_relative < -1 or target_amount_delta_relative >= 0:
                raise BadRequestException(
                    'target_amount_delta_relative must be within [-1, 0).')

        trading_account_id = context_container.trading_repository.get_trading_account(
            profile_id).id

        trading_service = context_container.trading_service

        try:
            trading_order = trading_service.create_stock_order(
                profile_id,
                TradingOrderSource.MANUAL,
                symbol,
                trading_account_id,
                target_amount_delta=target_amount_delta,
                target_amount_delta_relative=target_amount_delta_relative)
        except (InsufficientFundsException,
                InsufficientHoldingValueException) as e:
            raise InsufficientFundsHttpException() from e
        except TradingPausedException as e:
            raise BadRequestException(e.message) from e

        if target_amount_delta_relative:
            holding_amount = context_container.trading_repository.get_ticker_holding_value(
                profile_id, symbol)
            trading_order.target_amount_delta = target_amount_delta_relative * holding_amount
            context_container.trading_repository.persist(trading_order)

        return {'trading_order_id': trading_order.id}
