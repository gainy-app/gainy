from gainy.utils import get_logger
from trading.drivewealth.abstract_event_handler import AbstractDriveWealthEventHandler

logger = get_logger(__name__)


class UsersUpdatedEventHandler(AbstractDriveWealthEventHandler):

    def supports(self, event_type: str):
        return event_type == "users.updated"

    def handle(self, event_payload: dict):
        user_id = event_payload["userID"]
        self.provider.sync_user(user_id)