from gainy.trading.drivewealth.models import DriveWealthAccount
from gainy.trading.models import TradingAccount
from gainy.utils import get_logger
from trading.drivewealth.abstract_event_handler import AbstractDriveWealthEventHandler

logger = get_logger(__name__)


class AccountsUpdatedEventHandler(AbstractDriveWealthEventHandler):

    def supports(self, event_type: str):
        return event_type in ["accounts.updated", "accounts.created"]

    def handle(self, event_payload: dict):
        ref_id = event_payload["accountID"]

        account: DriveWealthAccount = self.repo.find_one(
            DriveWealthAccount, {"ref_id": ref_id})
        if account:
            data = event_payload.get('current', {})
            if "status" in data:
                account.status = data["status"]['name']
                self.repo.persist(account)
        else:
            self.provider.sync_trading_account(account_ref_id=ref_id,
                                               fetch_info=True)

        self.ensure_portfolio(account)

    def ensure_portfolio(self, account: DriveWealthAccount):
        if not account.trading_account_id:
            return

        trading_account: TradingAccount = self.repo.find_one(
            TradingAccount, {"id": account.trading_account_id})
        if not trading_account:
            return

        self.provider.ensure_portfolio(trading_account.profile_id,
                                       trading_account.id)
