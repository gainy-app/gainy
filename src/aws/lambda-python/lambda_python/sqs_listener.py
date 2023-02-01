from common.context_container import ContextContainer
from gainy.utils import setup_exception_logger_hook, get_logger, setup_lambda_logging_middleware
from queue_processing.exceptions import UnsupportedMessageException
from queue_processing.locking_function import HandleMessage

setup_exception_logger_hook()


def handle(event, context):
    setup_lambda_logging_middleware(context)
    logger = get_logger(__name__)
    logger.info('sqs_listener', extra={"event": event})

    with ContextContainer() as context_container:
        adapter = context_container.sqs_adapter
        dispatcher = context_container.queue_message_dispatcher
        repo = context_container.get_repository()

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
