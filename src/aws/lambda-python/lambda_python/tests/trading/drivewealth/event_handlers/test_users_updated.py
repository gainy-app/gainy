from gainy.tests.mocks.repository_mocks import mock_record_calls
from gainy.trading.drivewealth.models import DriveWealthUser
from trading.drivewealth.event_handlers import UsersUpdatedEventHandler
from trading.drivewealth.provider import DriveWealthProvider


def test(monkeypatch):
    user_id = "user_id"

    user = DriveWealthUser()

    provider = DriveWealthProvider(None, None, None, None, None)

    def mock_sync_user(_user_id):
        assert _user_id == user_id
        return user

    monkeypatch.setattr(provider, 'sync_user', mock_sync_user)
    ensure_account_created_calls = []
    monkeypatch.setattr(provider, 'ensure_account_created',
                        mock_record_calls(ensure_account_created_calls))

    event_handler = UsersUpdatedEventHandler(None, provider, None, None)

    message = {
        "userID": user_id,
    }
    event_handler.handle(message)

    assert (user, ) in [args for args, kwargs in ensure_account_created_calls]
