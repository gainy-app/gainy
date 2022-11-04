from exceptions import EntityNotFoundException
from gainy.trading.models import TradingCollectionVersion
from trading.drivewealth.models import DriveWealthAutopilotRun
from trading.drivewealth.api import DriveWealthApi
from trading.drivewealth.repository import DriveWealthRepository
from gainy.utils import get_logger
from gainy.trading.drivewealth.models import DriveWealthAccount, DriveWealthFund, DriveWealthPortfolio, \
    DriveWealthPortfolioStatusHolding, CollectionStatus
from gainy.trading.drivewealth.provider import DriveWealthProvider as GainyDriveWealthProvider

logger = get_logger(__name__)


class DriveWealthProviderCollection(GainyDriveWealthProvider):
    repository: DriveWealthRepository = None
    api: DriveWealthApi = None

    def get_actual_collection_data(self, profile_id: int,
                                   collection_id: int) -> CollectionStatus:
        fund = self.get_fund(profile_id, collection_id)
        if not fund:
            raise EntityNotFoundException(DriveWealthFund)

        repository = self.repository
        portfolio = repository.get_profile_portfolio(fund.profile_id)
        if not portfolio:
            raise EntityNotFoundException(DriveWealthPortfolio)

        portfolio_status = self.sync_portfolio_status(portfolio)
        fund_status = portfolio_status.get_fund(fund.ref_id)
        if not fund_status:
            raise EntityNotFoundException(DriveWealthPortfolioStatusHolding)

        return fund_status.get_collection_status()

    def _create_autopilot_run(self, account: DriveWealthAccount,
                              collection_version: TradingCollectionVersion):
        data = self.api.create_autopilot_run([account.ref_id])
        entity = DriveWealthAutopilotRun()
        entity.set_from_response(data)
        entity.account_id = account.ref_id
        self.repository.persist(entity)

        entity.update_trading_collection_version(collection_version)
        self.repository.persist(collection_version)

        return entity

    def _on_money_transfer(self, profile_id):
        repository = self.repository
        portfolio = repository.get_profile_portfolio(profile_id)
        if not portfolio:
            return

        self.sync_portfolio_status(portfolio)
