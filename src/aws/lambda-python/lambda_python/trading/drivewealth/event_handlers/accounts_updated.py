from typing import Optional

from gainy.trading.drivewealth.exceptions import TradingAccountNotOpenException
from gainy.trading.drivewealth.models import DriveWealthAccount, DriveWealthUser
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
            was_open = account.is_open()
            data = event_payload.get('current', {})
            if "status" in data:
                old_status = account.status
                account.status = data["status"]['name']
                self.provider.handle_account_status_change(account, old_status)
                self.repo.persist(account)
        else:
            was_open = False
            account = self.provider.sync_trading_account(account_ref_id=ref_id,
                                                         fetch_info=True)

        if account:
            self.ensure_portfolio(account)
            self.send_event(account, was_open)

    def send_event(self, account: DriveWealthAccount, was_open: bool):
        if was_open or not account.is_open(
        ) or not account.drivewealth_user_id:
            return

        user: DriveWealthUser = self.repo.find_one(
            DriveWealthUser, {"ref_id": account.drivewealth_user_id})
        if not user or not user.profile_id:
            return

        self.analytics_service.on_dw_brokerage_account_opened(user.profile_id)

    def ensure_portfolio(self, account: DriveWealthAccount):
        if not account.trading_account_id or not account.is_open():
            return

        trading_account: TradingAccount = self.repo.find_one(
            TradingAccount, {"id": account.trading_account_id})
        if not trading_account:
            return

        try:
            self.provider.ensure_portfolio(trading_account.profile_id,
                                           trading_account.id)
        except TradingAccountNotOpenException:
            pass
