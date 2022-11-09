from gainy.tests.mocks.repository_mocks import mock_persist
from trading.drivewealth.event_handlers import OrderCreatedEventHandler
from trading.drivewealth.models import DriveWealthOrder
from trading.drivewealth.repository import DriveWealthRepository


def test(monkeypatch):
    repository = DriveWealthRepository(None)
    persisted_objects = {}
    monkeypatch.setattr(repository, 'persist', mock_persist(persisted_objects))

    event_handler = OrderCreatedEventHandler(repository, None)

    message = {
        "id": "JK.7f6bbe0d-9341-437a-abf2-f028c2e7eb02",
        "status": "NEW",
        "accountID": "7b746acb-0afa-42c3-9c94-1bc8c16ce7b2.1661277115494",
        "symbol": "GOOG",
    }
    event_handler.handle(message)

    assert DriveWealthOrder in persisted_objects
