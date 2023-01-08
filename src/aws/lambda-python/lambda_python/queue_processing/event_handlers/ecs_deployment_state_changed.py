import datetime

import dateutil.parser

from gainy.utils import get_logger
from queue_processing.event_handlers.abstract_aws_event_handler import AbstractAwsEventHandler

logger = get_logger(__name__)


class ECSDeploymentStateChangeEventHandler(AbstractAwsEventHandler):

    def supports(self, event_type: str):
        return event_type == "ECS Deployment State Change"

    def handle(self, event_payload: dict):
        logger_extra = {
            "event_payload": event_payload,
        }
        try:
            event_name = event_payload["eventName"]
            updated_at = event_payload.get("updatedAt")

            updated_at_ago = datetime.datetime.now(
                tz=datetime.timezone.utc) - dateutil.parser.parse(updated_at)
            logger_extra["updated_at_ago"] = updated_at_ago
            if updated_at_ago > datetime.timedelta(minutes=15):
                return
        finally:
            logger.info("ECSDeploymentStateChangeEventHandler",
                        extra=logger_extra)
