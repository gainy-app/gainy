from common.context_container import ContextContainer
from common.exceptions import NotFoundException
from common.hasura_function import HasuraAction
from managed_portfolio.models import ManagedPortfolioFundingAccount
from gainy.utils import get_logger

logger = get_logger(__name__)


class ManagedPortfolioDeleteFundingAccount(HasuraAction):

    def __init__(self, action_name="managed_portfolio_delete_funding_account"):
        super().__init__(action_name, "profile_id")

    def apply(self, input_params, context_container: ContextContainer):
        funding_account_id = input_params['funding_account_id']
        profile_id = input_params['profile_id']

        repository = context_container.get_repository()
        funding_account = repository.find_one(ManagedPortfolioFundingAccount,
                                              {"id": funding_account_id})
        if not funding_account or funding_account.profile_id != profile_id:
            raise NotFoundException()

        context_container.managed_portfolio_service.delete_funding_account(
            funding_account)

        return {"ok": True}
