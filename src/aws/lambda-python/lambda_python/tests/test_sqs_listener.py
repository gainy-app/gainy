from common.context_container import ContextContainer
from gainy.data_access.repository import Repository
from gainy.tests.mocks.repository_mocks import mock_persist, mock_record_calls, mock_noop
from queue_processing.dispatcher import QueueMessageDispatcher
from queue_processing.locking_function import HandleMessage
from sqs_listener import handle


def test(monkeypatch):
    record = {
        "messageId": "messageId",
        "eventSourceARN": "eventSourceARN",
        "body": '"body"',
    }
    event = {"Records": [record]}

    queue_message_dispatcher = QueueMessageDispatcher([])

    repository = Repository(None)
    monkeypatch.setattr(repository, "persist", mock_persist)
    monkeypatch.setattr(repository, "commit", mock_noop)
    monkeypatch.setattr(ContextContainer, "queue_message_dispatcher",
                        queue_message_dispatcher)

    def mock_get_repository(*args):
        return repository

    monkeypatch.setattr(ContextContainer, "get_repository",
                        mock_get_repository)

    execute_calls = []
    monkeypatch.setattr(HandleMessage, "execute",
                        mock_record_calls(execute_calls))

    handle(event, {})

    assert len(execute_calls) == 1
