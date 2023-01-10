import base64
import datetime

import dateutil.parser

from gainy.utils import get_logger, env
from queue_processing.event_handlers.abstract_aws_event_handler import AbstractAwsEventHandler
from services.aws_ecs import ECS
from services.slack import Slack

ENV = env()

logger = get_logger(__name__)


class ECSTaskStateChangeEventHandler(AbstractAwsEventHandler):

    def supports(self, event_type: str):
        return event_type == "ECS Task State Change"

    def handle(self, body: dict):
        event_payload = body["detail"]
        logger_extra = {
            "body": body,
        }
        try:
            desired_status = event_payload["desiredStatus"]
            last_status = event_payload["lastStatus"]
            started_at = event_payload.get("startedAt")
            updated_at = event_payload.get("updatedAt")

            updated_at_ago = datetime.datetime.now(
                tz=datetime.timezone.utc) - dateutil.parser.parse(updated_at)
            logger_extra["updated_at_ago"] = updated_at_ago
            if updated_at_ago > datetime.timedelta(minutes=15):
                return

            logger.info("ECSTaskStateChangeEventHandler", extra=logger_extra)

            ecs = ECS()
            task_arn = event_payload["taskArn"]
            task_arn_trimmed = task_arn.split(":")[-1]
            task_def_arn = event_payload["taskDefinitionArn"]
            task_def = ecs.describe_task_definition(task_def_arn)

            logger_extra["task_def"] = task_def
            logger.info("ECSTaskStateChangeEventHandler", extra=logger_extra)

            tags = {t["key"]: t["value"] for t in task_def.get("tags", [])}

            branch = tags.get("source_code_branch")
            branch_name = tags.get("source_code_branch_name")
            try:
                branch_name = base64.b64decode(branch_name).decode('utf-8')
            except:
                pass

            logger_extra["tags"] = tags
            logger_extra["env"] = ENV
            logger_extra["branch"] = branch
            logger_extra["branch_name"] = branch_name

            if last_status == "RUNNING" and (branch_name or branch):
                message = f":large_green_circle: Branch {branch_name or branch} is deployed to *{ENV}* (task `{task_arn_trimmed}`)."
            elif started_at is not None and desired_status == "RUNNING" and last_status != "RUNNING":
                message = f":red_circle: Task `{task_arn_trimmed}` (started at {started_at}) is unstable on *{ENV}* (desired_status: {desired_status}, last_status: {last_status})."
            else:
                return

            logger_extra["message_text"] = message
            response = Slack().send_message(message)
            logger.info("ECSTaskStateChangeEventHandler _send_message",
                        extra={"response": response})
        finally:
            logger.info("ECSTaskStateChangeEventHandler", extra=logger_extra)
