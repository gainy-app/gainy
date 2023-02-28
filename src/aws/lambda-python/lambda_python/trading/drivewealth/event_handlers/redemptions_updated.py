from gainy.billing.models import PaymentTransaction
from gainy.trading.drivewealth.models import DriveWealthRedemption
from gainy.trading.models import TradingMoneyFlowStatus
from gainy.utils import get_logger
from trading.drivewealth.abstract_event_handler import AbstractDriveWealthEventHandler

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

        old_mf_status = redemption.get_money_flow_status()
        old_status = redemption.status
        redemption.set_from_response(event_payload)
        self.provider.handle_money_flow_status_change(redemption, old_status)

        self.repo.persist(redemption)
        self.provider.handle_redemption_status(redemption)

        if redemption.is_approved() and redemption.fees_total_amount is None:
            self.provider.sync_redemption(redemption.ref_id)

        self.provider.update_payment_transaction_from_dw(redemption)

        money_flow = self.provider.update_money_flow_from_dw(redemption)

        trading_account = self.sync_trading_account_balances(
            redemption.trading_account_ref_id, force=True)
        if trading_account:
            self.provider.notify_low_balance(trading_account)

        if money_flow and money_flow.status == TradingMoneyFlowStatus.SUCCESS and old_mf_status != TradingMoneyFlowStatus.SUCCESS:
            self.analytics_service.on_dw_withdraw_success(
                money_flow.profile_id, -money_flow.amount)
