from gainy.utils import setup_exception_logger_hook
from gainy.utils import get_logger

logger = get_logger(__name__)

setup_exception_logger_hook()


def handle(event, context):
    return logger.info('new event', extra={"event": event, "context": context})
