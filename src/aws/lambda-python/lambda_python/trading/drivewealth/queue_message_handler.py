import os
from typing import List

import trading.drivewealth.event_handlers
from queue_processing.exceptions import UnsupportedMessageException
from queue_processing.interfaces import QueueMessageHandlerInterface
from queue_processing.models import QueueMessage
from trading.drivewealth.event_handler_interface import AbstractDriveWealthEventHandler
from trading.drivewealth.provider import DriveWealthProvider
from trading.drivewealth.repository import DriveWealthRepository

DRIVEWEALTH_SQS_ARN = os.getenv("DRIVEWEALTH_SQS_ARN")


class DriveWealthQueueMessageHandler(QueueMessageHandlerInterface):
    handlers: List[AbstractDriveWealthEventHandler]

    def __init__(self, repo: DriveWealthRepository,
                 provider: DriveWealthProvider):
        self.handlers = [
            cls(repo, provider)
            for cls in trading.drivewealth.event_handlers.__dict__.values()
            if isinstance(cls, type)
            and issubclass(cls, AbstractDriveWealthEventHandler)
        ]

    def supports(self, message: QueueMessage) -> bool:
        return message.source_ref_id == DRIVEWEALTH_SQS_ARN

    def handle(self, message: QueueMessage):
        body = message.body

        message.source_event_ref_id = body["id"]
        event_type = body["type"]
        event_payload = body["payload"]

        self._get_handler(event_type).handle(event_payload)
        message.handled = True

    def _get_handler(self, event_type: str):
        for handler in self.handlers:
            if handler.supports(event_type):
                return handler

        raise UnsupportedMessageException(
            f'Unsupported event type {event_type}')
