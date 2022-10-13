from decimal import Decimal
from common.context_container import ContextContainer
from gainy.exceptions import NotFoundException, BadRequestException
from common.hasura_function import HasuraAction
from gainy.trading.models import TradingAccount
from trading.models import FundingAccount


class MoneyFlowAction(HasuraAction):

    def process_input(self, input_params, context_container: ContextContainer):
        profile_id = input_params["profile_id"]
        trading_account_id = input_params.get("trading_account_id")
        amount = Decimal(input_params["amount"])
        funding_account_id = input_params["funding_account_id"]

        if amount <= 0:
            raise BadRequestException('Bad amount')

        repository = context_container.get_repository()

        if trading_account_id:
            trading_account = repository.find_one(TradingAccount,
                                                  {"id": trading_account_id})
        else:
            trading_accounts = repository.find_all(TradingAccount,
                                                   {"profile_id": profile_id})
            if len(trading_accounts) > 1:
                raise BadRequestException(
                    'Profile has more than one Trading Account.')

            trading_account = trading_accounts[0] if len(
                trading_accounts) else None

        if not trading_account or trading_account.profile_id != profile_id:
            raise NotFoundException('Trading Account not found')

        funding_account = repository.find_one(FundingAccount,
                                              {"id": funding_account_id})
        if not funding_account or funding_account.profile_id != profile_id:
            raise NotFoundException('Funding Account not found')

        return profile_id, amount, trading_account, funding_account
