import os
from typing import List

import trading.drivewealth.event_handlers
from gainy.analytics.service import AnalyticsService
from queue_processing.abstract_message_handler import AbstractMessageHandler
from queue_processing.models import QueueMessage
from gainy.services.notification import NotificationService
from trading.drivewealth.abstract_event_handler import AbstractDriveWealthEventHandler
from trading.drivewealth.provider import DriveWealthProvider
from trading.drivewealth.repository import DriveWealthRepository
from trading.repository import TradingRepository

DRIVEWEALTH_SQS_ARN = os.getenv("DRIVEWEALTH_SQS_ARN")


class DriveWealthQueueMessageHandler(AbstractMessageHandler):
    handlers: List[AbstractDriveWealthEventHandler]

    def __init__(self, repo: DriveWealthRepository,
                 provider: DriveWealthProvider,
                 trading_repository: TradingRepository,
                 analytics_service: AnalyticsService,
                 notification_service: NotificationService):
        self.handlers = [
            cls(repo, provider, trading_repository, analytics_service,
                notification_service) for cls in self._iterate_module_classes(
                    trading.drivewealth.event_handlers)
            if issubclass(cls, AbstractDriveWealthEventHandler)
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
