from common.context_container import ContextContainer
from common.hasura_function import HasuraAction

from gainy.data_access.operators import OperatorIn
from gainy.utils import get_logger
from queue_processing.exceptions import UnsupportedMessageException
from queue_processing.models import QueueMessage

logger = get_logger(__name__)


class ReHandleQueueMessages(HasuraAction):

    def __init__(self, action_name="rehandle_queue_messages"):
        super().__init__(action_name, None)

    def apply(self, input_params, context_container: ContextContainer):
        ids = input_params["ids"]
        _filter = {"id": OperatorIn(ids), "handled": False}

        dispatcher = context_container.queue_message_dispatcher
        repo = context_container.get_repository()
        success = 0
        unsupported = 0
        error = 0
        for message in repo.iterate_all(QueueMessage, _filter):
            message: QueueMessage
            logger_extra = {"queue_message_id": message.id}

            try:
                dispatcher.handle(message)
                repo.persist(message)
                success += 1
            except UnsupportedMessageException as e:
                unsupported += 1
                logger.warning(e, extra=logger_extra)
            except Exception as e:
                error += 1
                logger.exception(e, extra=logger_extra)

        return {
            "success": success,
            "unsupported": unsupported,
            "error": error,
        }
