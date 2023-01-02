from gainy.tests.mocks.repository_mocks import mock_find, mock_persist, mock_record_calls
from trading.drivewealth.event_handlers import DepositsUpdatedEventHandler
from trading.drivewealth.models import DriveWealthDeposit
from trading.drivewealth.provider import DriveWealthProvider
from trading.drivewealth.repository import DriveWealthRepository

message = {
    "paymentID": "GYEK000001-1664534874577-DW51K",
    "statusMessage": "Approved",
    "accountID": "bf98c335-57ad-4337-ae9f-ed1fcfb447af.1662377145557",
}


def test_exists(monkeypatch):
    deposit = DriveWealthDeposit()

    provider = DriveWealthProvider(None, None, None, None, None)
    update_money_flow_from_dw_calls = []
    monkeypatch.setattr(provider, 'update_money_flow_from_dw',
                        mock_record_calls(update_money_flow_from_dw_calls))

    repository = DriveWealthRepository(None)
    monkeypatch.setattr(
        repository, 'find_one',
        mock_find([(DriveWealthDeposit, {
            "ref_id": message["paymentID"]
        }, deposit)]))
    persisted_objects = {}
    monkeypatch.setattr(repository, 'persist', mock_persist(persisted_objects))

    event_handler = DepositsUpdatedEventHandler(repository, provider)
    sync_trading_account_balances_calls = []
    monkeypatch.setattr(event_handler, 'sync_trading_account_balances',
                        mock_record_calls(sync_trading_account_balances_calls))

    event_handler.handle(message)

    assert DriveWealthDeposit in persisted_objects
    assert deposit in persisted_objects[DriveWealthDeposit]
    assert deposit in [
        args[0] for args, kwargs in update_money_flow_from_dw_calls
    ]
    assert message["accountID"] in [
        args[0] for args, kwargs in sync_trading_account_balances_calls
    ]
    assert deposit.ref_id == message["paymentID"]
    assert deposit.trading_account_ref_id == message["accountID"]
    assert deposit.status == message["statusMessage"]


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

    event_handler = DepositsUpdatedEventHandler(repository, provider)
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
