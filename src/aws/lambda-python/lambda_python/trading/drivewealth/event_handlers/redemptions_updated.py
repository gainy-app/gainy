from gainy.utils import get_logger
from trading.drivewealth.abstract_event_handler import AbstractDriveWealthEventHandler
from trading.drivewealth.models import DriveWealthRedemption

logger = get_logger(__name__)


class RedemptionUpdatedEventHandler(AbstractDriveWealthEventHandler):

    def supports(self, event_type: str):
        return event_type == 'redemptions.updated'

    def handle(self, event_payload: dict):
        ref_id = event_payload["paymentID"]
        redemption: DriveWealthRedemption = self.repo.find_one(
            DriveWealthRedemption, {"ref_id": ref_id})

        if not redemption:
            redemption = DriveWealthRedemption()

        redemption.set_from_response(event_payload)
        self.repo.persist(redemption)
        self.provider.handle_redemption_status(redemption)

        if redemption.is_approved() and redemption.fees_total_amount is None:
            # Todo fees in more cases?
            self.provider.sync_redemption(redemption.ref_id)

        self.provider.update_money_flow_from_dw(redemption)
