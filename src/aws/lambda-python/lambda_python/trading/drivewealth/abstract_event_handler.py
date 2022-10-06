from abc import abstractmethod, ABC

from trading.drivewealth.provider import DriveWealthProvider
from trading.drivewealth.repository import DriveWealthRepository


class AbstractDriveWealthEventHandler(ABC):

    def __init__(self, repo: DriveWealthRepository,
                 provider: DriveWealthProvider):
        self.repo = repo
        self.provider = provider

    @abstractmethod
    def supports(self, event_type: str):
        pass

    @abstractmethod
    def handle(self, event_payload: dict):
        pass
