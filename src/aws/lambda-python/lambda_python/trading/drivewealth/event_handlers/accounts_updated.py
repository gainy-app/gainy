from gainy.utils import get_logger
from trading.drivewealth.abstract_event_handler import AbstractDriveWealthEventHandler

logger = get_logger(__name__)


class AccountsUpdatedEventHandler(AbstractDriveWealthEventHandler):

    def supports(self, event_type: str):
        return event_type == "accounts.updated"

    def handle(self, event_payload: dict):
        account_ref_id = event_payload["accountID"]

        self.provider.sync_trading_account(account_ref_id=account_ref_id,
                                           fetch_info=True)
