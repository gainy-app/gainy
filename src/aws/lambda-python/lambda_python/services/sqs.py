from gainy.data_access.repository import Repository
from queue_processing.models import QueueMessage


class SqsAdapter:
    repo: Repository

    def __init__(self, repo: Repository):
        self.repo = repo

    def get_message(self, record: dict) -> QueueMessage:
        message = QueueMessage()
        message.ref_id = record["messageId"]
        message.source_ref_id = record["eventSourceARN"]
        message.body = record["body"]
        message.data = record
        self.repo.persist(message)
        return message
