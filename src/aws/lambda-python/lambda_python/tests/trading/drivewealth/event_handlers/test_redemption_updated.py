from gainy.tests.mocks.repository_mocks import mock_find, mock_persist, mock_record_calls
from trading.drivewealth.event_handlers import RedemptionUpdatedEventHandler
from trading.drivewealth.models import DriveWealthRedemption, DriveWealthRedemptionStatus
from trading.drivewealth.provider import DriveWealthProvider
from trading.drivewealth.repository import DriveWealthRepository

message = {
    "paymentID": "GYEK000001-1666639501262-RY7T6",
    "statusMessage": DriveWealthRedemptionStatus.RIA_Pending,
    "accountID": "bf98c335-57ad-4337-ae9f-ed1fcfb447af.1662377145557",
}


def test_exists(monkeypatch):
    provider = DriveWealthProvider(None, None, None, None)
    handle_redemption_status_calls = []
    monkeypatch.setattr(provider, 'handle_redemption_status',
                        mock_record_calls(handle_redemption_status_calls))
    update_money_flow_from_dw_calls = []
    monkeypatch.setattr(provider, 'update_money_flow_from_dw',
                        mock_record_calls(update_money_flow_from_dw_calls))
    sync_redemption_calls = []
    monkeypatch.setattr(provider, 'sync_redemption',
                        mock_record_calls(sync_redemption_calls))

    redemption = DriveWealthRedemption()

    repository = DriveWealthRepository(None)
    monkeypatch.setattr(
        repository, 'find_one',
        mock_find([(DriveWealthRedemption, {
            "ref_id": message["paymentID"]
        }, redemption)]))
    persisted_objects = {}
    monkeypatch.setattr(repository, 'persist', mock_persist(persisted_objects))

    event_handler = RedemptionUpdatedEventHandler(repository, provider)

    event_handler.handle(message)

    assert DriveWealthRedemption in persisted_objects
    assert redemption in persisted_objects[DriveWealthRedemption]
    assert redemption in [
        args[0] for args, kwards in handle_redemption_status_calls
    ]
    assert redemption in [
        args[0] for args, kwargs in update_money_flow_from_dw_calls
    ]
    assert redemption.ref_id == message["paymentID"]
    assert redemption.trading_account_ref_id == message["accountID"]
    assert redemption.status == message["statusMessage"]


def test_not_exists(monkeypatch):
    provider = DriveWealthProvider(None, None, None, None)
    handle_redemption_status_calls = []
    monkeypatch.setattr(provider, 'handle_redemption_status',
                        mock_record_calls(handle_redemption_status_calls))
    update_money_flow_from_dw_calls = []
    monkeypatch.setattr(provider, 'update_money_flow_from_dw',
                        mock_record_calls(update_money_flow_from_dw_calls))
    sync_redemption_calls = []
    monkeypatch.setattr(provider, 'sync_redemption',
                        mock_record_calls(sync_redemption_calls))

    repository = DriveWealthRepository(None)
    monkeypatch.setattr(
        repository, 'find_one',
        mock_find([(DriveWealthRedemption, {
            "ref_id": message["paymentID"]
        }, None)]))
    persisted_objects = {}
    monkeypatch.setattr(repository, 'persist', mock_persist(persisted_objects))

    event_handler = RedemptionUpdatedEventHandler(repository, provider)

    event_handler.handle(message)

    assert DriveWealthRedemption in persisted_objects
    redemption = persisted_objects[DriveWealthRedemption][0]
    assert redemption in [
        args[0] for args, kwards in handle_redemption_status_calls
    ]
    assert redemption in [
        args[0] for args, kwargs in update_money_flow_from_dw_calls
    ]
    assert redemption.ref_id == message["paymentID"]
    assert redemption.trading_account_ref_id == message["accountID"]
    assert redemption.status == message["statusMessage"]
