from gainy.trading.models import TradingMoneyFlowStatus
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

        old_mf_status = deposit.get_money_flow_status()
        old_status = deposit.status
        deposit.set_from_response(event_payload)

        self.provider.handle_money_flow_status_change(deposit, old_status)

        self.repo.persist(deposit)
        money_flow = self.provider.update_money_flow_from_dw(deposit)
        self.sync_trading_account_balances(deposit.trading_account_ref_id,
                                           force=True)

        if money_flow and deposit.get_money_flow_status(
        ) == TradingMoneyFlowStatus.SUCCESS and old_mf_status != TradingMoneyFlowStatus.SUCCESS:
            self.analytics_service.on_dw_deposit_success(money_flow)
