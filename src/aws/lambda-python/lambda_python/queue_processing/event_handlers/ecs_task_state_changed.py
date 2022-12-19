import os

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
        logger.info("ECSTaskStateChangeEventHandler", event_payload)
        desired_status = event_payload["desiredStatus"]
        last_status = event_payload["lastStatus"]
        started_at = event_payload.get("startedAt")

        ecs = ECS()
        task_def = ecs.describe_task_definition(
            event_payload["taskDefinitionArn"])
        logger.info("task_def", task_def)
        tags = {t["key"]: t["value"] for t in task_def.get("tags", [])}
        env = tags.get("environment")
        branch = tags.get("source_code_branch")
        branch_name = tags.get("source_code_branch_name")

        if not env or not (branch_name or branch):
            return

        if last_status == "RUNNING":
            message = f":green_apple: Branch {branch_name or branch} is running on {env}."
        elif last_status == "STOPPED":
            message = f":green_apple: Branch {branch_name or branch} is stopped on {env}."
        elif started_at is not None and desired_status == "RUNNING" and last_status != "RUNNING":
            message = f":tangerine: Branch {branch_name or branch} is unstable on {env} (desired_status: {desired_status}, last_status: {last_status})."
        else:
            return

        self._send_message(message)

    def _send_message(self, message):
        if not SLACK_BOT_TOKEN:
            return

        from slack_sdk import WebClient
        from slack_sdk.errors import SlackApiError

        client = WebClient(token=SLACK_BOT_TOKEN)

        try:
            client.chat_postMessage(channel=SLACK_NOTIFICATIONS_CHANNEL,
                                    text=message)
        except SlackApiError as e:
            logger.exception(e)
