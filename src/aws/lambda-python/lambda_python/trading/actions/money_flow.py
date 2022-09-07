from common.context_container import ContextContainer
from common.exceptions import NotFoundException, BadRequestException
from common.hasura_function import HasuraAction
from trading.models import TradingAccount, FundingAccount


class MoneyFlowAction(HasuraAction):

    def process_input(self, input_params, context_container: ContextContainer):
        profile_id = input_params["profile_id"]
        trading_account_id = input_params["trading_account_id"]
        amount_cents = input_params["amount_cents"]
        funding_account_id = input_params["funding_account_id"]

        if amount_cents <= 0:
            raise BadRequestException('Bad amount')

        repository = context_container.get_repository()

        trading_account = repository.find_one(TradingAccount,
                                              {"id": trading_account_id})
        if not trading_account or trading_account.profile_id != profile_id:
            raise NotFoundException('Trading Account not found')

        funding_account = repository.find_one(FundingAccount,
                                              {"id": funding_account_id})
        if not funding_account or funding_account.profile_id != profile_id:
            raise NotFoundException('Funding Account not found')

        return (profile_id, amount_cents, trading_account, funding_account)
