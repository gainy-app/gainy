from abc import ABC

from queue_processing.abstract_event_handler import EventHandlerInterface


class AbstractAwsEventHandler(EventHandlerInterface, ABC):

    def __init__(self):
        pass
