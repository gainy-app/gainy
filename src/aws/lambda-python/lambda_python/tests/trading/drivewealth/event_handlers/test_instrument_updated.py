from gainy.tests.mocks.repository_mocks import mock_find, mock_persist
from gainy.trading.drivewealth.models import DriveWealthInstrument
from trading.drivewealth.event_handlers import InstrumentUpdatedEventHandler
from trading.drivewealth.provider import DriveWealthProvider
from trading.drivewealth.repository import DriveWealthRepository


def test_exists(monkeypatch):
    instrument_id = "instrument_id"

    instrument = DriveWealthInstrument()

    repository = DriveWealthRepository(None)
    monkeypatch.setattr(
        repository, 'find_one',
        mock_find([(DriveWealthInstrument, {
            "ref_id": instrument_id
        }, instrument)]))
    persisted_objects = {}
    monkeypatch.setattr(repository, 'persist', mock_persist(persisted_objects))

    event_handler = InstrumentUpdatedEventHandler(repository, None)

    message = {
        "instrumentID": instrument_id,
        "current": {},
    }
    event_handler.handle(message)

    assert DriveWealthInstrument in persisted_objects
    assert instrument in persisted_objects[DriveWealthInstrument]


def test_not_exists(monkeypatch):
    instrument_id = "instrument_id"
    sync_instrument_called = False

    repository = DriveWealthRepository(None)
    monkeypatch.setattr(
        repository, 'find_one',
        mock_find([(DriveWealthInstrument, {
            "ref_id": instrument_id
        }, None)]))
    provider = DriveWealthProvider(None, None, None)

    def mock_sync_instrument(ref_id):
        assert ref_id == instrument_id

        nonlocal sync_instrument_called
        sync_instrument_called = True

    monkeypatch.setattr(provider, 'sync_instrument', mock_sync_instrument)

    event_handler = InstrumentUpdatedEventHandler(repository, provider)

    message = {
        "instrumentID": instrument_id,
    }
    event_handler.handle(message)

    assert sync_instrument_called
