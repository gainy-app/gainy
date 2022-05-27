from portfolio.service import PortfolioService, SERVICE_PLAID
from common.hasura_function import HasuraTrigger


class OnPlaidAccessTokenCreated(HasuraTrigger):

    def __init__(self):
        super().__init__("on_plaid_access_token_created")

        self.portfolio_service = PortfolioService()

    def get_allowed_profile_ids(self, op, data):
        return [data["new"]['profile_id']]

    def apply(self, db_conn, op, data):
        payload = self._extract_payload(data)
        access_token = payload
        access_token['service'] = SERVICE_PLAID

        holdings_count = self.portfolio_service.sync_token_holdings(
            db_conn, access_token)
        transactions_count = self.portfolio_service.sync_token_transactions(
            db_conn, access_token)
        self.portfolio_service.sync_institution(db_conn, access_token)

        return {
            'holdings_count': holdings_count,
            'transactions_count': transactions_count,
        }
