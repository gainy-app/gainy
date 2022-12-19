import os
from typing import List

import queue_processing.event_handlers
from queue_processing.abstract_event_handler import EventHandlerInterface
from queue_processing.abstract_message_handler import AbstractMessageHandler
from queue_processing.event_handlers.abstract_aws_event_handler import AbstractAwsEventHandler
from queue_processing.models import QueueMessage

AWS_EVENTS_SQS_ARN = os.getenv("AWS_EVENTS_SQS_ARN")


class AwsMessageHandler(AbstractMessageHandler):
    handlers: List[EventHandlerInterface]

    def __init__(self):
        self.handlers = [
            cls() for cls in self._iterate_module_classes(
                queue_processing.event_handlers)
            if issubclass(cls, AbstractAwsEventHandler)
        ]

    def supports(self, message: QueueMessage) -> bool:
        return message.source_ref_id == AWS_EVENTS_SQS_ARN

    def handle(self, message: QueueMessage):
        body = message.body

        message.source_event_ref_id = body["id"]
        event_type = body["detail-type"]
        event_payload = body["detail"]

        self._get_handler(event_type).handle(event_payload)
        message.handled = True
