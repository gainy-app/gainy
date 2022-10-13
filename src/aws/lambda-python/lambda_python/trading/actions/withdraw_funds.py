from common.context_container import ContextContainer
from trading.actions.money_flow import MoneyFlowAction
from gainy.utils import get_logger

logger = get_logger(__name__)


class TradingWithdrawFunds(MoneyFlowAction):

    def __init__(self):
        super().__init__("trading_withdraw_funds", "profile_id")

    def apply(self, input_params, context_container: ContextContainer):
        (profile_id, amount, trading_account,
         funding_account) = self.process_input(input_params, context_container)

        trading_service = context_container.trading_service
        money_flow = trading_service.create_money_flow(profile_id, -amount,
                                                       trading_account,
                                                       funding_account)

        return {'trading_money_flow_id': money_flow.id}
