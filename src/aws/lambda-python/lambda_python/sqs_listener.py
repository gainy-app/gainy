from psycopg2._psycopg import connection
from psycopg2.extras import RealDictCursor

from common.context_container import ContextContainer
from gainy.queue_processing.exceptions import UnsupportedMessageException
from gainy.utils import setup_exception_logger_hook, get_logger, setup_lambda_logging_middleware, PUBLIC_SCHEMA_NAME
from queue_processing.locking_function import HandleMessage

setup_exception_logger_hook()
logger = get_logger(__name__)


def check_can_run(db_conn: connection, function_arn: str) -> bool:
    query = "select deployed_at, sqs_listener_lambda_arn from deployment.public_schemas where schema_name = %(public_schema_name)s"
    params = {
        "public_schema_name": PUBLIC_SCHEMA_NAME,
    }

    with db_conn.cursor(cursor_factory=RealDictCursor) as cursor:
        cursor.execute(query, params)
        row = cursor.fetchone()

    if row is None:
        return False

    logger.info('check_can_run',
                extra={
                    "public_schema_name": PUBLIC_SCHEMA_NAME,
                    "function_arn": function_arn,
                    "row": dict(row),
                })

    if not row["deployed_at"]:
        return False
    if row["sqs_listener_lambda_arn"] != function_arn:
        return False

    return True


def handle(event, context):
    setup_lambda_logging_middleware(context)
    function_arn = context.invoked_function_arn

    with ContextContainer() as context_container:
        if not check_can_run(context_container.db_conn, function_arn):
            return

        adapter = context_container.sqs_adapter
        dispatcher = context_container.queue_message_dispatcher
        repo = context_container.get_repository()

        logger.info('sqs_listener',
                    extra={
                        "event": event,
                        "function_arn": function_arn,
                    })
        for record in event["Records"]:
            logger_extra = {"record": record}

            try:
                message = adapter.get_message(record)

                func = HandleMessage(repo, dispatcher, message)
                func.execute()
            except UnsupportedMessageException as e:
                logger.warning(e, extra=logger_extra)
            except Exception as e:
                logger.exception(e, extra=logger_extra)
