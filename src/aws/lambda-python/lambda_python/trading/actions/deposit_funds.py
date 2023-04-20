from common.context_container import ContextContainer
from gainy.exceptions import BadRequestException
from gainy.trading import MIN_FIRST_DEPOSIT_AMOUNT
from gainy.trading.models import TradingMoneyFlow
from trading.actions.money_flow import MoneyFlowAction
from gainy.utils import get_logger

logger = get_logger(__name__)


class TradingDepositFunds(MoneyFlowAction):

    def __init__(self):
        super().__init__("trading_deposit_funds", "profile_id")

    def apply(self, input_params, context_container: ContextContainer):
        (profile_id, amount, trading_account,
         funding_account) = self.process_input(input_params, context_container)

        trading_service = context_container.trading_service

        self.validate_amount(context_container, profile_id, amount)

        money_flow = trading_service.create_money_flow(profile_id, amount,
                                                       trading_account,
                                                       funding_account)

        context_container.notification_service.on_deposit_initiated(
            profile_id, money_flow.amount)

        return {'trading_money_flow_id': money_flow.id}

    def validate_amount(self, context_container: ContextContainer, profile_id,
                        amount):
        money_flow = context_container.trading_repository.find_one(
            TradingMoneyFlow, {"profile_id": profile_id})

        if money_flow:
            return

        if amount < MIN_FIRST_DEPOSIT_AMOUNT:
            raise BadRequestException(
                f"Minimal amount for the first deposit is ${MIN_FIRST_DEPOSIT_AMOUNT}."
            )
