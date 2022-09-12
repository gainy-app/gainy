import dateutil

from gainy.data_access.pessimistic_lock import AbstractPessimisticLockingFunction
from gainy.utils import get_logger
from trading.drivewealth.models import DriveWealthAuthToken
from trading.drivewealth.repository import DriveWealthRepository

logger = get_logger(__name__)


class UpdateDriveWealthAuthToken(AbstractPessimisticLockingFunction):
    repo: DriveWealthRepository
    api = None

    def __init__(self, repo: DriveWealthRepository, api):
        super().__init__(repo)
        self.api = api

    def execute(self, max_tries: int = 3) -> DriveWealthAuthToken:
        return super().execute(max_tries)

    def load_version(self) -> DriveWealthAuthToken:
        entity = self.repo.get_latest_auth_token()

        if not entity:
            entity = DriveWealthAuthToken()

        return entity

    def _do(self, token: DriveWealthAuthToken):
        if not token.is_expired():
            return token

        data = self.api.get_auth_token()

        token.auth_token = data["authToken"]
        token.expires_at = dateutil.parser.parse(data['expiresAt'])
        token.data = data
        self.repo.persist(token)
        return token
