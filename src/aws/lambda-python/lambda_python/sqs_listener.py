from common.context_container import ContextContainer
from gainy.utils import setup_exception_logger_hook, get_logger
from queue_processing.exceptions import UnsupportedMessageException

logger = get_logger(__name__)

setup_exception_logger_hook()


def handle(event, context):
    logger.info('New message', extra={"event": event, "context": context})

    with ContextContainer() as context_container:
        adapter = context_container.sqs_adapter
        dispatcher = context_container.queue_message_dispatcher
        repo = context_container.get_repository()

        for record in event["Records"]:
            logger_extra = {"record": record}

            try:
                message = adapter.get_message(record)
                dispatcher.handle(message)
                repo.persist(message)
            except UnsupportedMessageException as e:
                logger.warning(e, extra=logger_extra)
            except Exception as e:
                logger.exception(e, extra=logger_extra)
