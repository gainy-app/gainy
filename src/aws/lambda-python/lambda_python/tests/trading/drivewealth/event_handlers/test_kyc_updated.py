from trading.drivewealth.event_handlers.accounts_updated import AccountsUpdatedEventHandler
from trading.drivewealth.event_handlers.deposits_updated import DepositsUpdatedEventHandler
from trading.drivewealth.event_handlers.kyc_updated import KycUpdatedEventHandler
from trading.drivewealth.provider import DriveWealthProvider


def test(monkeypatch):
    user_id = "user_id"
    sync_kyc_called = False

    provider = DriveWealthProvider(None, None, None)

    def mock_sync_kyc(_user_id):
        assert _user_id == user_id

        nonlocal sync_kyc_called
        sync_kyc_called = True

    monkeypatch.setattr(provider, 'sync_kyc', mock_sync_kyc)

    event_handler = KycUpdatedEventHandler(None, provider)

    message = {
        "userID": user_id,
    }
    event_handler.handle(message)

    assert sync_kyc_called
