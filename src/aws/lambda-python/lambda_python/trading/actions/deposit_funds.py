from common.context_container import ContextContainer
from gainy.exceptions import BadRequestException, AccountNeedsReauthException, AccountNeedsReauthHttpException
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

        try:
            money_flow = trading_service.create_money_flow(
                profile_id, amount, trading_account, funding_account)

            context_container.notification_service.on_deposit_initiated(
                profile_id, money_flow.amount)
        except AccountNeedsReauthException:
            raise AccountNeedsReauthHttpException(
                'Request failed, please try again later.')
        except:
            raise BadRequestException(
                'Request failed, please try again later.')

        return {'trading_money_flow_id': money_flow.id}
