import datetime
import json
from typing import Optional

from gainy.data_access.models import BaseModel, classproperty


class QueueMessage(BaseModel):
    ref_id: str = None
    source_ref_id: str = None
    source_event_ref_id: Optional[str] = None
    body = None
    data = None
    handled: bool = None
    created_at: datetime.datetime = None
    updated_at: datetime.datetime = None

    key_fields = ["ref_id"]

    db_excluded_fields = ["created_at", "updated_at"]
    non_persistent_fields = ["id", "created_at", "updated_at"]

    def __init__(self, row: dict = None):
        self.handled = False

        super().__init__(row)

    @classproperty
    def schema_name(self) -> str:
        return "app"

    @classproperty
    def table_name(self) -> str:
        return "queue_messages"

    def to_dict(self) -> dict:
        return {
            **super().to_dict(),
            "data": json.dumps(self.data),
        }

    @property
    def body_json(self):
        return json.loads(self.body)
