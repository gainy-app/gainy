import logging
from portfolio.plaid.common import PURPOSE_MANAGED_TRADING
from common.context_container import ContextContainer
from common.exceptions import ApiException
from common.hasura_exception import HasuraActionException
from common.hasura_function import HasuraAction
from managed_portfolio import ManagedPortfolioService
from psycopg2.extras import RealDictCursor

logger = logging.getLogger()


class LinkManagedTradingBankAccountWithPlaid(HasuraAction):

    def __init__(self):
        super().__init__("link_managed_trading_bank_account_with_plaid",
                         "profile_id")
        self.managed_portfolio_service = ManagedPortfolioService()

    def apply(self, input_params, context_container: ContextContainer):
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
            raise Exception('Token not found')

        self.managed_portfolio_service.link_bank_account_with_plaid(
            context_container, access_token, account_name, account_id)
