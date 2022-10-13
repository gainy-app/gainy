from common.context_container import ContextContainer
from common.hasura_function import HasuraAction
from gainy.utils import get_logger

logger = get_logger(__name__)


class TradingGetFundingAccounts(HasuraAction):

    def __init__(self, action_name="trading_get_funding_accounts"):
        super().__init__(action_name, "profile_id")

    def apply(self, input_params, context_container: ContextContainer):
        profile_id = input_params["profile_id"]

        trading_service = context_container.trading_service
        funding_accounts = trading_service.sync_funding_accounts(profile_id)

        return [{
            "id": funding_account.id
        } for funding_account in funding_accounts]
