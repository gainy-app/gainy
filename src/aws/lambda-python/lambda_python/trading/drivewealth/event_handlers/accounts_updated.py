from gainy.trading.drivewealth.models import DriveWealthAccount
from gainy.utils import get_logger
from trading.drivewealth.abstract_event_handler import AbstractDriveWealthEventHandler

logger = get_logger(__name__)


class AccountsUpdatedEventHandler(AbstractDriveWealthEventHandler):

    def supports(self, event_type: str):
        return event_type == "accounts.updated"

    def handle(self, event_payload: dict):
        ref_id = event_payload["accountID"]

        account = self.repo.find_one(DriveWealthAccount, {"ref_id": ref_id})
        if account:
            data = event_payload['current']
            if "status" in data:
                account.status = data["status"]['name']

            self.repo.persist(account)
        else:
            self.provider.sync_trading_account(account_ref_id=ref_id,
                                               fetch_info=True)
