from gainy.data_access.pessimistic_lock import AbstractPessimisticLockingFunction
from gainy.data_access.repository import Repository
from gainy.utils import get_logger
from queue_processing.dispatcher import QueueMessageDispatcher
from queue_processing.models import QueueMessage

logger = get_logger(__name__)


class HandleMessage(AbstractPessimisticLockingFunction):
    repo: Repository
    dispatcher: QueueMessageDispatcher

    def __init__(self, repo: Repository, dispatcher: QueueMessageDispatcher,
                 message: QueueMessage):
        super().__init__(repo)
        self.dispatcher = dispatcher
        self.message = message

    def execute(self, max_tries: int = 2):
        return super().execute(max_tries)

    def load_version(self) -> QueueMessage:
        return self.repo.refresh(self.message)

    def _do(self, message: QueueMessage):
        if message.handled:
            return

        self.dispatcher.handle(message)
        self.repo.persist(message)
