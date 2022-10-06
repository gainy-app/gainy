from trading.drivewealth.event_handlers import InstrumentUpdatedEventHandler
from trading.drivewealth.provider import DriveWealthProvider


def test(monkeypatch):
    instrument_id = "instrument_id"
    sync_instrument_called = False

    provider = DriveWealthProvider(None, None, None)

    def mock_sync_instrument(ref_id):
        assert ref_id == instrument_id

        nonlocal sync_instrument_called
        sync_instrument_called = True

    monkeypatch.setattr(provider, 'sync_instrument', mock_sync_instrument)

    event_handler = InstrumentUpdatedEventHandler(None, provider)

    message = {
        "instrumentID": instrument_id,
    }
    event_handler.handle(message)

    assert sync_instrument_called
