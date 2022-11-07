from common.context_container import ContextContainer
from common.hasura_function import HasuraAction
from gainy.exceptions import NotFoundException, BadRequestException


class TradingAddMoney(HasuraAction):

    def __init__(self):
        super().__init__("trading_add_money", "profile_id")

    def apply(self, input_params, context_container: ContextContainer):
        trading_service = context_container.trading_service
        trading_repository = context_container.trading_repository

        if input_params["trading_account_id"]:
            trading_account_id = input_params["trading_account_id"]
        elif input_params["profile_id"]:
            profile_id = input_params["profile_id"]
            trading_account = trading_repository.get_trading_account(
                profile_id)
            if not trading_account:
                raise NotFoundException()
            trading_account_id = trading_account.id
        else:
            raise BadRequestException(
                "Either profile_id or trading_account_id must be specified")

        amount = input_params["amount"]

        trading_service.debug_add_money(trading_account_id, amount)

        return {"ok": True}
