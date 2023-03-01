from gainy.trading.drivewealth.models import DriveWealthRedemption, DriveWealthPortfolio, DW_WEIGHT_THRESHOLD
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

        was_approved = redemption.is_approved()
        old_mf_status = redemption.get_money_flow_status()
        old_status = redemption.status
        redemption.set_from_response(event_payload)
        self.provider.handle_money_flow_status_change(redemption, old_status)

        self.repo.persist(redemption)
        self.provider.handle_redemption_status(redemption)

        if redemption.is_approved() and not was_approved:
            # update cash weight in linked portfolio
            # todo thread-safe
            portfolio: DriveWealthPortfolio = self.repo.find_one(
                DriveWealthPortfolio,
                {"drivewealth_account_id": redemption.trading_account_ref_id})
            if portfolio:
                prev_cash_target_weight = portfolio.cash_target_weight
                portfolio_status = self.provider.sync_portfolio_status(
                    portfolio, force=True)
                portfolio_changed = self.provider.actualize_portfolio(
                    portfolio, portfolio_status)
                if portfolio_changed:
                    portfolio.normalize_weights()
                    self.provider.send_portfolio_to_api(portfolio)

        if redemption.is_approved() and redemption.fees_total_amount is None:
            self.provider.sync_redemption(redemption.ref_id)

        money_flow = self.provider.update_money_flow_from_dw(redemption)

        trading_account = self.sync_trading_account_balances(
            redemption.trading_account_ref_id, force=True)
        if trading_account:
            self.provider.notify_low_balance(trading_account)

        if money_flow and redemption.get_money_flow_status(
        ) == TradingMoneyFlowStatus.SUCCESS and old_mf_status != TradingMoneyFlowStatus.SUCCESS:
            self.analytics_service.on_dw_withdraw_success(
                money_flow.profile_id, -money_flow.amount)
