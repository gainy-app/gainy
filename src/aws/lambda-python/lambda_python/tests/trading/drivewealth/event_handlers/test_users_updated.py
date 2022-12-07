from trading.drivewealth.event_handlers import UsersUpdatedEventHandler
from trading.drivewealth.provider import DriveWealthProvider


def test(monkeypatch):
    user_id = "user_id"
    sync_user_called = False

    provider = DriveWealthProvider(None, None, None, None, None)

    def mock_sync_user(_user_id):
        assert _user_id == user_id

        nonlocal sync_user_called
        sync_user_called = True

    monkeypatch.setattr(provider, 'sync_user', mock_sync_user)

    event_handler = UsersUpdatedEventHandler(None, provider)

    message = {
        "userID": user_id,
    }
    event_handler.handle(message)

    assert sync_user_called
