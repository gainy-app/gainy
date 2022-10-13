from trading.drivewealth.event_handlers.accounts_updated import AccountsUpdatedEventHandler
from trading.drivewealth.event_handlers.deposits_updated import DepositsUpdatedEventHandler
from trading.drivewealth.provider import DriveWealthProvider


def test(monkeypatch):
    payment_id = "payment_id"
    sync_trading_account_called = False

    provider = DriveWealthProvider(None, None, None)

    def mock_sync_deposit(deposit_ref_id, fetch_info):
        assert fetch_info
        assert deposit_ref_id == payment_id

        nonlocal sync_trading_account_called
        sync_trading_account_called = True

    monkeypatch.setattr(provider, 'sync_deposit', mock_sync_deposit)

    event_handler = DepositsUpdatedEventHandler(None, provider)

    message = {
        "paymentID": payment_id,
    }
    event_handler.handle(message)

    assert sync_trading_account_called
