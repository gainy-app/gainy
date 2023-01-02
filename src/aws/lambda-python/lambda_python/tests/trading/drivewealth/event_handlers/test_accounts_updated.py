from gainy.tests.mocks.repository_mocks import mock_find, mock_persist, mock_record_calls
from gainy.trading.drivewealth.models import DriveWealthAccount
from gainy.trading.models import TradingAccount
from trading.drivewealth.event_handlers.accounts_updated import AccountsUpdatedEventHandler
from trading.drivewealth.provider import DriveWealthProvider
from trading.drivewealth.repository import DriveWealthRepository


def test_exists(monkeypatch):
    account_id = "account_id"
    status_name = "status_name"

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
    ensure_portfolio_calls = []
    monkeypatch.setattr(event_handler, 'ensure_portfolio',
                        mock_record_calls(ensure_portfolio_calls))

    message = {
        "accountID": account_id,
        "current": {
            "status": {
                'name': status_name
            },
        },
    }
    event_handler.handle(message)

    assert DriveWealthAccount in persisted_objects
    assert account in persisted_objects[DriveWealthAccount]
    assert (account, ) in [args for args, kwargs in ensure_portfolio_calls]
    assert account.status == status_name


def test_not_exists(monkeypatch):
    account_id = "account_id"
    account = DriveWealthAccount()

    repository = DriveWealthRepository(None)
    monkeypatch.setattr(
        repository, 'find_one',
        mock_find([(DriveWealthAccount, {
            "ref_id": account_id
        }, None)]))

    provider = DriveWealthProvider(None, None, None, None, None)

    def mock_sync_trading_account(account_ref_id, fetch_info):
        assert fetch_info
        assert account_ref_id == account_id
        return account

    monkeypatch.setattr(provider, 'sync_trading_account',
                        mock_sync_trading_account)

    event_handler = AccountsUpdatedEventHandler(repository, provider)
    ensure_portfolio_calls = []
    monkeypatch.setattr(event_handler, 'ensure_portfolio',
                        mock_record_calls(ensure_portfolio_calls))

    message = {
        "accountID": account_id,
    }
    event_handler.handle(message)

    assert (account, ) in [args for args, kwargs in ensure_portfolio_calls]


def test_ensure_portfolio(monkeypatch):
    trading_account_id = 1
    profile_id = 2

    account = DriveWealthAccount()
    account.trading_account_id = trading_account_id

    trading_account = TradingAccount()
    trading_account.profile_id = profile_id
    trading_account.id = trading_account_id

    repository = DriveWealthRepository(None)
    monkeypatch.setattr(
        repository, 'find_one',
        mock_find([(TradingAccount, {
            "id": trading_account_id
        }, trading_account)]))

    provider = DriveWealthProvider(None, None, None, None, None)
    ensure_portfolio_calls = []
    monkeypatch.setattr(provider, 'ensure_portfolio',
                        mock_record_calls(ensure_portfolio_calls))

    event_handler = AccountsUpdatedEventHandler(repository, provider)
    event_handler.ensure_portfolio(account)

    assert (profile_id, trading_account_id) in [
        args for args, kwargs in ensure_portfolio_calls
    ]
