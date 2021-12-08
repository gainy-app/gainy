from portfolio.service import PortfolioService
from common.hasura_function import HasuraAction


class GetPortfolioHoldings(HasuraAction):
    def __init__(self):
        super().__init__("get_portfolio_holdings", "profile_id")

        self.service = PortfolioService()

    def apply(self, db_conn, input_params):
        profile_id = input_params["profile_id"]

        holdings = self.service.get_holdings(db_conn, profile_id)

        return [i.normalize() for i in holdings]


class GetPortfolioTransactions(HasuraAction):
    def __init__(self):
        super().__init__("get_portfolio_transactions", "profile_id")

        self.service = PortfolioService()

    def apply(self, db_conn, input_params):
        profile_id = input_params["profile_id"]
        count = input_params.get("count", 100)
        offset = input_params.get("offset", 0)

        transactions = self.service.get_transactions(db_conn,
                                                     profile_id,
                                                     count=count,
                                                     offset=offset)

        return [i.normalize() for i in transactions]
