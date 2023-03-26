import datetime
from gainy.trading.models import TradingOrder, TradingCollectionVersion, TradingMoneyFlow
from gainy.context_container import ContextContainer


def run():
    amount = 10
    profile_id = 714

    order = TradingOrder()
    order.symbol = "AAPL"
    order.id = 1
    order.target_amount_delta = amount
    order.created_at = datetime.datetime.now(tz=datetime.timezone.utc)
    order.profile_id = profile_id

    tcv = TradingCollectionVersion()
    tcv.collection_id = 16
    tcv.id = 1
    tcv.target_amount_delta = amount
    tcv.created_at = datetime.datetime.now(tz=datetime.timezone.utc)
    tcv.profile_id = profile_id

    money_flow = TradingMoneyFlow()
    money_flow.profile_id = profile_id
    money_flow.id = 1
    money_flow.amount = amount

    with ContextContainer() as context_container:
        context_container.analytics_service.on_dw_brokerage_account_opened(
            profile_id)
        context_container.analytics_service.on_kyc_status_rejected(profile_id)
        context_container.analytics_service.on_deposit_success(money_flow)
        context_container.analytics_service.on_withdraw_success(
            profile_id, amount)
        context_container.analytics_service.on_order_executed(order)
        context_container.analytics_service.on_order_executed(tcv)
        order.target_amount_delta *= -1
        context_container.analytics_service.on_order_executed(order)
        tcv.target_amount_delta *= -1
        context_container.analytics_service.on_order_executed(tcv)
        context_container.analytics_service.on_commission_withdrawn(
            profile_id, amount)
