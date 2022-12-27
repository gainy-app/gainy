from abc import ABC

from queue_processing.abstract_event_handler import EventHandlerInterface
from trading.drivewealth.provider import DriveWealthProvider
from trading.drivewealth.repository import DriveWealthRepository


class AbstractDriveWealthEventHandler(EventHandlerInterface, ABC):

    def __init__(self, repo: DriveWealthRepository,
                 provider: DriveWealthProvider):
        self.repo = repo
        self.provider = provider
