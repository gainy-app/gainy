from gainy.utils import get_logger
from trading.drivewealth.abstract_event_handler import AbstractDriveWealthEventHandler
from trading.drivewealth.models import DriveWealthRedemption

logger = get_logger(__name__)


class RedemptionUpdatedEventHandler(AbstractDriveWealthEventHandler):

    def supports(self, event_type: str):
        return event_type == 'redemptions.updated'

    def handle(self, event_payload: dict):
        ref_id = event_payload["paymentID"]
        redemption = self.repo.find_one(DriveWealthRedemption,
                                        {"ref_id": ref_id})

        if not redemption:
            redemption = DriveWealthRedemption()

        redemption.set_from_response(event_payload)
        self.repo.persist(redemption)
        self.provider.handle_redemption_status(redemption)
