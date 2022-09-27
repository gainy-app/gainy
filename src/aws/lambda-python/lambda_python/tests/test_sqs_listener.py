from common.context_container import ContextContainer
from gainy.data_access.repository import Repository
from gainy.tests.mocks.repository_mocks import mock_persist
from queue_processing.dispatcher import QueueMessageDispatcher
from queue_processing.models import QueueMessage
from sqs_listener import handle


def test_drivewealth(monkeypatch):
    record = {
        "messageId": "messageId",
        "eventSourceARN": "eventSourceARN",
        "body": '"body"',
    }
    event = {"Records": [record]}

    handle_called = False
    queue_message_dispatcher = QueueMessageDispatcher([])

    def mock_handle(message: QueueMessage):
        assert message.ref_id == "messageId"
        assert message.source_ref_id == "eventSourceARN"
        assert message.body == "body"

        nonlocal handle_called
        handle_called = True

    monkeypatch.setattr(queue_message_dispatcher, "handle", mock_handle)

    repository = Repository(None)
    monkeypatch.setattr(repository, "persist", mock_persist)
    monkeypatch.setattr(ContextContainer, "queue_message_dispatcher",
                        queue_message_dispatcher)

    def mock_get_repository(*args):
        return repository

    monkeypatch.setattr(ContextContainer, "get_repository",
                        mock_get_repository)

    handle(event, {})

    assert handle_called
