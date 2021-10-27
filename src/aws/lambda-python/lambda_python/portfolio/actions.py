import os
import plaid

from portfolio.service import PortfolioService
from portfolio.repository import PortfolioRepository
from common.hasura_function import HasuraAction


class GetPortfolioHoldings(HasuraAction):
    def __init__(self):
        super().__init__("get_portfolio_holdings", "profile_id")

        self.service = PortfolioService()
        self.portfolio_repository = PortfolioRepository()

    def apply(self, db_conn, input_params):
        profile_id = input_params["profile_id"]

        result = self.service.get_holdings(db_conn, profile_id)

        securities = result['securities']
        accounts = result['accounts']
        holdings = result['holdings']

        # persist securities
        for entity in securities:
            entity.profile_id = profile_id
        self.portfolio_repository.persist(db_conn, securities)
        securities_dict = {
            security.ref_id: security.id
            for security in securities
        }

        # persist accounts
        for entity in accounts:
            entity.profile_id = profile_id
        self.portfolio_repository.persist(db_conn, accounts)
        accounts_dict = {account.ref_id: account.id for account in accounts}

        # persist holdings
        for entity in holdings:
            entity.profile_id = profile_id
            entity.security_id = securities_dict[entity.security_ref_id]
            entity.account_id = accounts_dict[entity.account_ref_id]
        self.portfolio_repository.persist(db_conn, holdings)

        # cleanup
        self.portfolio_repository.remove_other_by_profile_id(db_conn, holdings)
        self.portfolio_repository.remove_other_by_profile_id(db_conn, accounts)
        self.portfolio_repository.remove_other_by_profile_id(
            db_conn, securities)

        return [i.normalize() for i in result['holdings']]
