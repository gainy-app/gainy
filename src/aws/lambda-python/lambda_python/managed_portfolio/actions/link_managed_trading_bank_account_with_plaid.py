from portfolio.plaid.common import PURPOSE_MANAGED_TRADING
from common.context_container import ContextContainer
from common.exceptions import ApiException, NotFoundException
from common.hasura_exception import HasuraActionException
from common.hasura_function import HasuraAction
from managed_portfolio import ManagedPortfolioService
from psycopg2.extras import RealDictCursor
from gainy.utils import get_logger

logger = get_logger(__name__)


class LinkManagedTradingBankAccountWithPlaid(HasuraAction):

    def __init__(self):
        super().__init__("link_managed_trading_bank_account_with_plaid",
                         "profile_id")

    def apply(self, input_params, context_container: ContextContainer):
        managed_portfolio_service = context_container.managed_portfolio_service
        profile_id = input_params["profile_id"]
        account_id = input_params["account_id"]
        access_token_id = input_params["access_token_id"]
        account_name = input_params["account_name"]

        query = "select * from app.profile_plaid_access_tokens where id = %(access_token_id)s and profile_id = %(profile_id)s"
        with context_container.db_conn.cursor(
                cursor_factory=RealDictCursor) as cursor:
            cursor.execute(query, {
                "access_token_id": access_token_id,
                "profile_id": profile_id,
            })
            access_token = cursor.fetchone()

        if not access_token or access_token[
                'purpose'] != PURPOSE_MANAGED_TRADING:
            raise NotFoundException('Token not found')

        try:
            funding_account = managed_portfolio_service.link_bank_account_with_plaid(
                access_token, account_name, account_id)
        except Exception as e:
            return {"error_message": str(e), 'id': None}

        return {"error_message": None, 'id': funding_account.id}
