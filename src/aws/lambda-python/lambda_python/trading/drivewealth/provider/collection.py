from gainy.exceptions import EntityNotFoundException
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

    # TODO deprecated ?
    def get_actual_collection_data(self, profile_id: int,
                                   collection_id: int) -> CollectionStatus:
        fund = self.repository.get_profile_fund(profile_id,
                                                collection_id=collection_id)
        if not fund:
            raise EntityNotFoundException(DriveWealthFund)

        trading_collection_version: TradingCollectionVersion = self.repository.find_one(
            TradingCollectionVersion,
            {"id": fund.trading_collection_version_id})
        if not fund:
            raise EntityNotFoundException(TradingCollectionVersion)
        trading_account_id = trading_collection_version.trading_account_id

        repository = self.repository
        portfolio = repository.get_profile_portfolio(fund.profile_id,
                                                     trading_account_id)
        if not portfolio:
            raise EntityNotFoundException(DriveWealthPortfolio)

        portfolio_status = self.sync_portfolio_status(portfolio)
        fund_status = portfolio_status.get_fund(fund.ref_id)
        if not fund_status:
            raise EntityNotFoundException(DriveWealthPortfolioStatusHolding)

        return fund_status.get_collection_status()
