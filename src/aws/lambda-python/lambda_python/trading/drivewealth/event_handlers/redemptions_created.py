from gainy.trading.drivewealth.models import DriveWealthRedemption
from gainy.utils import get_logger
from trading.drivewealth.abstract_event_handler import AbstractDriveWealthEventHandler

logger = get_logger(__name__)


class RedemptionCreatedEventHandler(AbstractDriveWealthEventHandler):

    def supports(self, event_type: str):
        return event_type == 'redemptions.created'

    def handle(self, event_payload: dict):
        redemption = DriveWealthRedemption()
        redemption.set_from_response(event_payload)
        self.repo.persist(redemption)
        self.provider.handle_redemption_status(redemption)
        self.sync_trading_account_balances(redemption.trading_account_ref_id,
                                           force=True)
