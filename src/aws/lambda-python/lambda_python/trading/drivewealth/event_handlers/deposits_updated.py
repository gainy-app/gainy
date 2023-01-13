from gainy.utils import get_logger
from trading.drivewealth.abstract_event_handler import AbstractDriveWealthEventHandler
from trading.drivewealth.models import DriveWealthDeposit

logger = get_logger(__name__)


class DepositsUpdatedEventHandler(AbstractDriveWealthEventHandler):

    def supports(self, event_type: str):
        return event_type == "deposits.updated"

    def handle(self, event_payload: dict):
        ref_id = event_payload["paymentID"]
        deposit = self.repo.find_one(DriveWealthDeposit, {"ref_id": ref_id})

        if not deposit:
            deposit = DriveWealthDeposit()

        deposit.set_from_response(event_payload)
        self.repo.persist(deposit)
        self.provider.update_money_flow_from_dw(deposit)
        self.sync_trading_account_balances(deposit.trading_account_ref_id,
                                           force=True)
