import json

from queue_processing.models import QueueMessage
from trading.drivewealth.event_handlers import UsersUpdatedEventHandler
from trading.drivewealth.queue_message_handler import DriveWealthQueueMessageHandler


def test(monkeypatch):
    event_type = "event_type"
    event_payload = "event_payload"
    handle_called = False

    event_handler = UsersUpdatedEventHandler(None, None)

    def mock_handle(_event_payload):
        nonlocal handle_called
        assert _event_payload == event_payload
        handle_called = True

    monkeypatch.setattr(event_handler, 'handle', mock_handle)

    message_handler = DriveWealthQueueMessageHandler(None, None)

    def mock_get_handler(_event_type):
        assert _event_type == event_type
        return event_handler

    monkeypatch.setattr(message_handler, '_get_handler', mock_get_handler)

    message = QueueMessage(None)
    message.body = json.dumps({
        "id": "id",
        "type": event_type,
        "payload": event_payload
    })
    message_handler.handle(message)

    assert handle_called
