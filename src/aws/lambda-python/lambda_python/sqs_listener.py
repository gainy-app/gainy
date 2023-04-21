from typing import Optional

from psycopg2._psycopg import connection

from common.context_container import ContextContainer
from gainy.queue_processing.exceptions import UnsupportedMessageException
from gainy.queue_processing.models import QueueMessage
from gainy.services.aws_lambda import AWSLambda
from gainy.utils import setup_exception_logger_hook, get_logger, setup_lambda_logging_middleware, PUBLIC_SCHEMA_NAME
from queue_processing.locking_function import HandleMessage

setup_exception_logger_hook()
logger = get_logger(__name__)


def get_handler_function_arn(db_conn: connection) -> Optional[str]:
    query = "select sqs_handler_lambda_arn from deployment.public_schemas where deployed_at is not null order by set_active_at desc nulls last limit 1"

    with db_conn.cursor() as cursor:
        cursor.execute(query)
        row = cursor.fetchone()

    handler_function_arn = row[0] if row else None

    logger.info('handler_lambda_arn',
                extra={
                    "public_schema_name": PUBLIC_SCHEMA_NAME,
                    "handler_lambda_arn": handler_function_arn,
                })

    return handler_function_arn


def listen(event, context):
    setup_lambda_logging_middleware(context)
    logger.info('sqs_listener', extra={"event": event})

    with ContextContainer() as context_container:
        adapter = context_container.sqs_adapter
        handler_function_arn = get_handler_function_arn(
            context_container.db_conn)

        message_ids = []
        for record in event["Records"]:
            logger_extra = {"record": record}

            try:
                message = adapter.get_message(record)
                message_ids.append(message.id)
            except Exception as e:
                logger.exception(e, extra=logger_extra)

    AWSLambda().invoke(handler_function_arn, {"message_ids": message_ids},
                       sync=False)


def handle(event, context):
    setup_lambda_logging_middleware(context)
    logger.info('sqs_handler', extra={"event": event})

    with ContextContainer() as context_container:
        dispatcher = context_container.queue_message_dispatcher
        repo = context_container.get_repository()

        for message_id in event["message_ids"]:
            logger_extra = {"message_id": message_id}
            message: QueueMessage = repo.find_one(QueueMessage,
                                                  {"id": message_id})

            try:
                func = HandleMessage(repo, dispatcher, message)
                func.execute()
                repo.commit()
            except UnsupportedMessageException as e:
                logger.warning(e, extra=logger_extra)
            except Exception as e:
                repo.rollback()
                logger.exception(e, extra=logger_extra)
