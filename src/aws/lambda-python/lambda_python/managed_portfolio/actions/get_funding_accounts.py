from common.context_container import ContextContainer
from common.hasura_function import HasuraAction
from managed_portfolio import ManagedPortfolioService, ManagedPortfolioRepository
from managed_portfolio.models import ManagedPortfolioFundingAccount
from gainy.utils import get_logger

logger = get_logger(__name__)


class ManagedPortfolioGetFundingAccounts(HasuraAction):

    def __init__(self, action_name="managed_portfolio_get_funding_accounts"):
        super().__init__(action_name, "profile_id")

    def apply(self, input_params, context_container: ContextContainer):
        db_conn = context_container.db_conn
        profile_id = input_params["profile_id"]

        repository = context_container.managed_portfolio_repository
        funding_accounts = repository.find_all(ManagedPortfolioFundingAccount,
                                               {"profile_id": profile_id})

        managed_portfolio_service = context_container.managed_portfolio_service
        managed_portfolio_service.update_funding_accounts_balance(
            funding_accounts)

        return [{
            "id": funding_account.id
        } for funding_account in funding_accounts]
