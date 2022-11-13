from gainy.plaid.common import PURPOSE_TRADING
from common.context_container import ContextContainer
from gainy.exceptions import NotFoundException
from common.hasura_function import HasuraAction
from gainy.plaid.models import PlaidAccessToken
from gainy.utils import get_logger

logger = get_logger(__name__)


class TradingLinkBankAccountWithPlaid(HasuraAction):

    def __init__(self):
        super().__init__("trading_link_bank_account_with_plaid", "profile_id")

    def apply(self, input_params, context_container: ContextContainer):
        trading_service = context_container.trading_service
        profile_id = input_params["profile_id"]
        account_id = input_params["account_id"]
        access_token_id = input_params["access_token_id"]
        account_name = input_params["account_name"]

        access_token = context_container.get_repository().find_one(
            PlaidAccessToken, {"id": access_token_id})

        if not access_token or access_token.purpose != PURPOSE_TRADING or access_token.profile_id != profile_id:
            raise NotFoundException('Token not found')

        funding_account = trading_service.link_bank_account_with_plaid(
            access_token, account_name, account_id)
        return {'id': funding_account.id}
