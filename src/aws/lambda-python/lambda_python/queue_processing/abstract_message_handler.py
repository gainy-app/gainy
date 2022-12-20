import inspect
import os
from abc import ABC
from typing import List, Iterable

from queue_processing.abstract_event_handler import EventHandlerInterface
from queue_processing.exceptions import UnsupportedMessageException
from queue_processing.interfaces import QueueMessageHandlerInterface

AWS_EVENTS_SQS_ARN = os.getenv("AWS_EVENTS_SQS_ARN")


class AbstractMessageHandler(QueueMessageHandlerInterface, ABC):
    handlers: List[EventHandlerInterface]

    def _iterate_module_classes(
            self, module) -> Iterable[EventHandlerInterface.__class__]:
        for cls in module.__dict__.values():
            if not isinstance(cls, type): continue
            if not issubclass(cls, EventHandlerInterface): continue
            if inspect.isabstract(cls): continue

            yield cls

    def _get_handler(self, event_type: str):
        for handler in self.handlers:
            if handler.supports(event_type):
                return handler

        raise UnsupportedMessageException(
            f'Unsupported event type {event_type}')