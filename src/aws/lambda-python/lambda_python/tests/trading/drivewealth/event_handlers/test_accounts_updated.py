from gainy.tests.mocks.repository_mocks import mock_find, mock_persist
from gainy.trading.drivewealth.models import DriveWealthAccount
from trading.drivewealth.event_handlers.accounts_updated import AccountsUpdatedEventHandler
from trading.drivewealth.provider import DriveWealthProvider
from trading.drivewealth.repository import DriveWealthRepository


def test_exists(monkeypatch):
    account_id = "account_id"

    account = DriveWealthAccount()

    repository = DriveWealthRepository(None)
    monkeypatch.setattr(
        repository, 'find_one',
        mock_find([(DriveWealthAccount, {
            "ref_id": account_id
        }, account)]))
    persisted_objects = {}
    monkeypatch.setattr(repository, 'persist', mock_persist(persisted_objects))

    event_handler = AccountsUpdatedEventHandler(repository, None)

    message = {
        "accountID": account_id,
        "current": {},
    }
    event_handler.handle(message)

    assert DriveWealthAccount in persisted_objects
    assert account in persisted_objects[DriveWealthAccount]


def test_not_exists(monkeypatch):
    account_id = "account_id"
    sync_trading_account_called = False

    repository = DriveWealthRepository(None)
    monkeypatch.setattr(
        repository, 'find_one',
        mock_find([(DriveWealthAccount, {
            "ref_id": account_id
        }, None)]))

    provider = DriveWealthProvider(None, None, None)

    def mock_sync_trading_account(account_ref_id, fetch_info):
        assert fetch_info
        assert account_ref_id == account_id

        nonlocal sync_trading_account_called
        sync_trading_account_called = True

    monkeypatch.setattr(provider, 'sync_trading_account',
                        mock_sync_trading_account)

    event_handler = AccountsUpdatedEventHandler(repository, provider)

    message = {
        "accountID": account_id,
    }
    event_handler.handle(message)

    assert sync_trading_account_called
