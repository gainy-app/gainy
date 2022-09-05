from common.context_container import ContextContainer
from common.hasura_function import HasuraAction
from trading import TradingService, TradingRepository
from trading.models import FundingAccount
from gainy.utils import get_logger

logger = get_logger(__name__)


class TradingGetFundingAccounts(HasuraAction):

    def __init__(self, action_name="trading_get_funding_accounts"):
        super().__init__(action_name, "profile_id")

    def apply(self, input_params, context_container: ContextContainer):
        db_conn = context_container.db_conn
        profile_id = input_params["profile_id"]

        repository = context_container.trading_repository
        funding_accounts = repository.find_all(FundingAccount,
                                               {"profile_id": profile_id})

        trading_service = context_container.trading_service
        trading_service.update_funding_accounts_balance(funding_accounts)

        return [{
            "id": funding_account.id
        } for funding_account in funding_accounts]
