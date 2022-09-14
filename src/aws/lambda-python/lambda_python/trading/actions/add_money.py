from common.context_container import ContextContainer
from common.hasura_function import HasuraAction
from gainy.utils import get_logger

logger = get_logger(__name__)


class TradingAddMoney(HasuraAction):

    def __init__(self):
        super().__init__("trading_add_money", "profile_id")

    def apply(self, input_params, context_container: ContextContainer):
        profile_id = input_params["profile_id"]
        trading_account_id = input_params["trading_account_id"]
        amount = input_params["amount"]

        trading_service = context_container.trading_service
        trading_service.debug_add_money(trading_account_id, amount)

        return {"ok": True}
