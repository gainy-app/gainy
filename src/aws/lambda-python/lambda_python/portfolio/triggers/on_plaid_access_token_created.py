from portfolio.plaid.service import SERVICE_PLAID
from gainy.plaid.common import PURPOSE_PORTFOLIO
from common.context_container import ContextContainer
from common.hasura_function import HasuraTrigger


class OnPlaidAccessTokenCreated(HasuraTrigger):

    def __init__(self):
        super().__init__("on_plaid_access_token_created")

    def get_allowed_profile_ids(self, op, data):
        return [data["new"]['profile_id']]

    def apply(self, op, data, context_container: ContextContainer):
        portfolio_service = context_container.portfolio_service
        db_conn = context_container.db_conn
        payload = self._extract_payload(data)

        if "purpose" in payload and payload["purpose"] != PURPOSE_PORTFOLIO:
            return

        access_token = payload
        access_token['service'] = SERVICE_PLAID

        holdings_count = portfolio_service.sync_token_holdings(access_token)
        transactions_count = portfolio_service.sync_token_transactions(
            access_token)
        portfolio_service.sync_institution(access_token)

        return {
            'holdings_count': holdings_count,
            'transactions_count': transactions_count,
        }
