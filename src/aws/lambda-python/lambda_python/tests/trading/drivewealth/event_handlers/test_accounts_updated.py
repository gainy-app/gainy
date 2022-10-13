from trading.drivewealth.event_handlers.accounts_updated import AccountsUpdatedEventHandler
from trading.drivewealth.provider import DriveWealthProvider


def test(monkeypatch):
    account_id = "account_id"
    sync_trading_account_called = False

    provider = DriveWealthProvider(None, None, None)

    def mock_sync_trading_account(account_ref_id, fetch_info):
        assert fetch_info
        assert account_ref_id == account_id

        nonlocal sync_trading_account_called
        sync_trading_account_called = True

    monkeypatch.setattr(provider, 'sync_trading_account',
                        mock_sync_trading_account)

    event_handler = AccountsUpdatedEventHandler(None, provider)

    message = {
        "accountID": account_id,
    }
    event_handler.handle(message)

    assert sync_trading_account_called
