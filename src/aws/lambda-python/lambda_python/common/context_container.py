from functools import cached_property, cache

from portfolio.plaid.service import PlaidService
from portfolio.service import PortfolioService
from portfolio.service.chart import PortfolioChartService
from portfolio.repository import PortfolioRepository
from trading import TradingService, TradingRepository
from trading.drivewealth.provider import DriveWealthProvider
from trading.drivewealth.repository import DriveWealthRepository

from gainy.recommendation.repository import RecommendationRepository
from gainy.data_access.repository import Repository


class ContextContainer():
    db_conn = None

    @cache
    def get_repository(self, cls=None):
        if cls:
            raise Exception('get_repository for a class is not supported')

        return Repository(self.db_conn)

    @cached_property
    def recommendation_repository(self) -> RecommendationRepository:
        return RecommendationRepository(self.db_conn)

    ## portfolio
    @cached_property
    def portfolio_repository(self) -> PortfolioRepository:
        return PortfolioRepository(self.db_conn)

    @cached_property
    def plaid_service(self) -> PlaidService:
        return PlaidService(self.db_conn)

    @cached_property
    def portfolio_service(self) -> PortfolioService:
        return PortfolioService(self.db_conn, self.portfolio_repository,
                                self.plaid_service)

    @cached_property
    def portfolio_chart_service(self) -> PortfolioChartService:
        return PortfolioChartService(self.db_conn)

    ## drivewealth
    @cached_property
    def drivewealth_repository(self):
        return DriveWealthRepository(self.db_conn)

    @cached_property
    def drivewealth_provider(self):
        return DriveWealthProvider(self.drivewealth_repository,
                                   self.plaid_service)

    ## trading
    @cached_property
    def trading_service(self):
        return TradingService(self.db_conn, self.trading_repository,
                              self.drivewealth_provider, self.plaid_service)

    @cached_property
    def trading_repository(self):
        return TradingRepository(self.db_conn)
