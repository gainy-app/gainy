import datetime
import os

import dateutil.parser

from gainy.utils import get_logger
from queue_processing.event_handlers.abstract_aws_event_handler import AbstractAwsEventHandler
from services.aws_ecs import ECS

SLACK_BOT_TOKEN = os.getenv('SLACK_BOT_TOKEN')
SLACK_NOTIFICATIONS_CHANNEL = os.getenv('SLACK_NOTIFICATIONS_CHANNEL',
                                        "#build-release")

logger = get_logger(__name__)


class ECSTaskStateChangeEventHandler(AbstractAwsEventHandler):

    def supports(self, event_type: str):
        return event_type == "ECS Task State Change"

    def handle(self, event_payload: dict):
        logger_extra = {
            "event_payload": event_payload,
        }
        try:
            desired_status = event_payload["desiredStatus"]
            last_status = event_payload["lastStatus"]
            started_at = event_payload.get("startedAt")
            updated_at = event_payload.get("updatedAt")

            updated_at_ago = datetime.datetime.now(
                tz=datetime.timezone.utc) - dateutil.parser.parse(updated_at)
            logger_extra["updated_at_ago"] = updated_at_ago
            if updated_at_ago > datetime.timedelta(minutes=30):
                return

            ecs = ECS()
            task_def = ecs.describe_task_definition(
                event_payload["taskDefinitionArn"])

            logger_extra["task_def"] = task_def
            logger.info("ECSTaskStateChangeEventHandler", extra=logger_extra)

            tags = {t["key"]: t["value"] for t in task_def.get("tags", [])}
            env = tags.get("environment")
            branch = tags.get("source_code_branch")
            branch_name = tags.get("source_code_branch_name")

            logger_extra["tags"] = tags
            logger_extra["env"] = env
            logger_extra["branch"] = branch
            logger_extra["branch_name"] = branch_name

            if not env or not (branch_name or branch):
                return

            if last_status == "RUNNING":
                message = f":large_green_circle: Branch {branch_name or branch} is running on {env}."
            elif last_status == "STOPPED":
                message = f":large_green_circle: Branch {branch_name or branch} is stopped on {env}."
            elif started_at is not None and desired_status == "RUNNING" and last_status != "RUNNING":
                message = f":red_circle: Branch {branch_name or branch} is unstable on {env} (desired_status: {desired_status}, last_status: {last_status})."
            else:
                return

            logger_extra["message"] = message
            self._send_message(message)
        finally:
            logger.info("ECSTaskStateChangeEventHandler", extra=logger_extra)

    def _send_message(self, message):
        if not SLACK_BOT_TOKEN:
            return

        from slack_sdk import WebClient
        from slack_sdk.errors import SlackApiError

        client = WebClient(token=SLACK_BOT_TOKEN)

        try:
            response = client.chat_postMessage(
                channel=SLACK_NOTIFICATIONS_CHANNEL, text=message)
            logger.info("ECSTaskStateChangeEventHandler _send_message",
                        extra={"response": response})
        except SlackApiError as e:
            logger.exception(e)
