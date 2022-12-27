from gainy.data_access.repository import Repository
from gainy.tests.mocks.repository_mocks import mock_persist, mock_record_calls
from queue_processing.dispatcher import QueueMessageDispatcher
from queue_processing.locking_function import HandleMessage
from queue_processing.models import QueueMessage


def test(monkeypatch):
    dispatcher = QueueMessageDispatcher([])
    handle_calls = []
    monkeypatch.setattr(dispatcher, "handle", mock_record_calls(handle_calls))

    message = QueueMessage()
    message.handled = False

    repository = Repository(None)
    persisted_objects = {}
    monkeypatch.setattr(repository, "persist", mock_persist(persisted_objects))

    func = HandleMessage(repository, dispatcher, message)
    func._do(message)

    assert message in [args[0] for args, kwargs in handle_calls]
    assert message in persisted_objects[QueueMessage]
