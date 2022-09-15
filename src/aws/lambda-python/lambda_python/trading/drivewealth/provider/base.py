from trading.drivewealth.models import DriveWealthUser
from trading.drivewealth.repository import DriveWealthRepository
from gainy.utils import get_logger

logger = get_logger(__name__)


class DriveWealthProviderBase:
    drivewealth_repository: DriveWealthRepository = None

    def __init__(self, drivewealth_repository: DriveWealthRepository):
        self.drivewealth_repository = drivewealth_repository

    def _get_user(self, profile_id) -> DriveWealthUser:
        repository = self.drivewealth_repository
        user = repository.get_user(profile_id)
        if user is None:
            raise Exception("KYC form has not been sent")
        return user
