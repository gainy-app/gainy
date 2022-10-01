from gainy.utils import get_logger
from trading.drivewealth.event_handler_interface import AbstractDriveWealthEventHandler
from trading.drivewealth.provider import DriveWealthProvider
from trading.drivewealth.repository import DriveWealthRepository

logger = get_logger(__name__)


class UsersUpdatedEventHandler(AbstractDriveWealthEventHandler):

    def __init__(self, repo: DriveWealthRepository,
                 service: DriveWealthProvider):
        super().__init__(repo, service)

    def supports(self, event_type: str):
        return event_type == "users.updated"

    def handle(self, event_payload: dict):
        user_id = event_payload["userID"]
        self.provider.sync_user(user_id)
