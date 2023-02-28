from gainy.analytics.service import AnalyticsService
from gainy.tests.mocks.repository_mocks import mock_find, mock_persist, mock_record_calls
from gainy.trading.models import TradingMoneyFlow
from trading.drivewealth.event_handlers import DepositsUpdatedEventHandler
from trading.drivewealth.models import DriveWealthDeposit
from trading.drivewealth.provider import DriveWealthProvider
from trading.drivewealth.repository import DriveWealthRepository

message = {
    "paymentID": "GYEK000001-1664534874577-DW51K",
    "statusMessage": "Successful",
    "accountID": "bf98c335-57ad-4337-ae9f-ed1fcfb447af.1662377145557",
}


def test_exists(monkeypatch):
    old_status = 'Pending'
    new_status = message["statusMessage"]

    deposit = DriveWealthDeposit()
    deposit.status = old_status

    money_flow = TradingMoneyFlow()

    provider = DriveWealthProvider(None, None, None, None, None)

    def mock_update_money_flow_from_dw(_deposit):
        assert deposit == _deposit
        deposit.status = new_status
        return money_flow

    monkeypatch.setattr(provider, 'update_money_flow_from_dw',
                        mock_update_money_flow_from_dw)
    handle_money_flow_status_change_calls = []
    monkeypatch.setattr(
        provider, 'handle_money_flow_status_change',
        mock_record_calls(handle_money_flow_status_change_calls))

    repository = DriveWealthRepository(None)
    monkeypatch.setattr(
        repository, 'find_one',
        mock_find([(DriveWealthDeposit, {
            "ref_id": message["paymentID"]
        }, deposit)]))
    persisted_objects = {}
    monkeypatch.setattr(repository, 'persist', mock_persist(persisted_objects))

    analytics_service = AnalyticsService(None, None, None)
    on_dw_deposit_success_calls = []
    monkeypatch.setattr(analytics_service, 'on_dw_deposit_success',
                        mock_record_calls(on_dw_deposit_success_calls))

    event_handler = DepositsUpdatedEventHandler(repository, provider, None,
                                                analytics_service)
    sync_trading_account_balances_calls = []
    monkeypatch.setattr(event_handler, 'sync_trading_account_balances',
                        mock_record_calls(sync_trading_account_balances_calls))

    event_handler.handle(message)

    assert DriveWealthDeposit in persisted_objects
    assert deposit in persisted_objects[DriveWealthDeposit]
    assert message["accountID"] in [
        args[0] for args, kwargs in sync_trading_account_balances_calls
    ]
    assert deposit.ref_id == message["paymentID"]
    assert deposit.trading_account_ref_id == message["accountID"]
    assert deposit.status == new_status
    assert (deposit, old_status) in [
        args for args, kwargs in handle_money_flow_status_change_calls
    ]
    assert ((money_flow, ), {}) in on_dw_deposit_success_calls


def test_not_exists(monkeypatch):
    provider = DriveWealthProvider(None, None, None, None, None)
    update_money_flow_from_dw_calls = []
    monkeypatch.setattr(provider, 'update_money_flow_from_dw',
                        mock_record_calls(update_money_flow_from_dw_calls))

    repository = DriveWealthRepository(None)
    monkeypatch.setattr(
        repository, 'find_one',
        mock_find([(DriveWealthDeposit, {
            "ref_id": message["paymentID"]
        }, None)]))
    persisted_objects = {}
    monkeypatch.setattr(repository, 'persist', mock_persist(persisted_objects))

    event_handler = DepositsUpdatedEventHandler(repository, provider, None,
                                                None)
    sync_trading_account_balances_calls = []
    monkeypatch.setattr(event_handler, 'sync_trading_account_balances',
                        mock_record_calls(sync_trading_account_balances_calls))

    event_handler.handle(message)

    assert DriveWealthDeposit in persisted_objects
    deposit = persisted_objects[DriveWealthDeposit][0]
    assert deposit in [
        args[0] for args, kwargs in update_money_flow_from_dw_calls
    ]
    assert message["accountID"] in [
        args[0] for args, kwargs in sync_trading_account_balances_calls
    ]
