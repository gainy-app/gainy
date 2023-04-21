from collections import namedtuple

from common.context_container import ContextContainer
from gainy.data_access.repository import Repository
from gainy.queue_processing.dispatcher import QueueMessageDispatcher
from gainy.queue_processing.models import QueueMessage
from gainy.services.aws_lambda import AWSLambda
from gainy.tests.mocks.repository_mocks import mock_persist, mock_record_calls, mock_noop, mock_find
from queue_processing.locking_function import HandleMessage
from sqs_listener import handle, listen


def test_listen(monkeypatch):
    record = {
        "messageId": "messageId",
        "eventSourceARN": "eventSourceARN",
        "body": '"body"',
    }
    event = {"Records": [record]}

    repository = Repository(None)
    persisted_objects = {}
    monkeypatch.setattr(repository, "persist", mock_persist(persisted_objects))
    monkeypatch.setattr(repository, "commit", mock_noop)

    monkeypatch.setattr(ContextContainer, "get_repository",
                        lambda *args: repository)

    invoke_calls = []
    monkeypatch.setattr(AWSLambda, "invoke", mock_record_calls(invoke_calls))

    listen(
        event,
        namedtuple('Point', [
            'invoked_function_arn', 'log_stream_name', 'log_group_name',
            'aws_request_id', 'memory_limit_in_mb'
        ])(None, None, None, None, None))

    assert len(invoke_calls) == 1
    assert len(persisted_objects[QueueMessage]) == 1


def test_handle(monkeypatch):
    message_id = 1
    event = {"message_ids": [message_id]}
    message = QueueMessage()

    queue_message_dispatcher = QueueMessageDispatcher([])

    repository = Repository(None)
    monkeypatch.setattr(
        repository, "find_one",
        mock_find([(QueueMessage, {
            "id": message_id
        }, message)]))
    monkeypatch.setattr(repository, "commit", mock_noop)

    monkeypatch.setattr(ContextContainer, "queue_message_dispatcher",
                        queue_message_dispatcher)
    monkeypatch.setattr(ContextContainer, "get_repository",
                        lambda *args: repository)

    execute_calls = []
    monkeypatch.setattr(HandleMessage, "execute",
                        mock_record_calls(execute_calls))

    handle(
        event,
        namedtuple('Point', [
            'invoked_function_arn', 'log_stream_name', 'log_group_name',
            'aws_request_id', 'memory_limit_in_mb'
        ])(None, None, None, None, None))

    assert len(execute_calls) == 1
