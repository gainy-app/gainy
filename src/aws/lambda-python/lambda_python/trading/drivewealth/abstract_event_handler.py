from abc import ABC
from typing import Optional

from gainy.trading.drivewealth.models import DriveWealthAccount
from gainy.trading.models import TradingAccount
from queue_processing.abstract_event_handler import EventHandlerInterface
from trading.drivewealth.provider import DriveWealthProvider
from trading.drivewealth.repository import DriveWealthRepository


class AbstractDriveWealthEventHandler(EventHandlerInterface, ABC):

    def __init__(self, repo: DriveWealthRepository,
                 provider: DriveWealthProvider):
        self.repo = repo
        self.provider = provider

    def sync_trading_account_balances(
            self, trading_account_ref_id: str) -> Optional[TradingAccount]:
        if not trading_account_ref_id:
            return

        account: DriveWealthAccount = self.repo.find_one(
            DriveWealthAccount, {"ref_id": trading_account_ref_id})
        if not account or not account.trading_account_id:
            return

        trading_account = self.repo.find_one(
            TradingAccount, {"id": account.trading_account_id})
        if not trading_account:
            return

        self.provider.sync_balances(trading_account)
        return trading_account
