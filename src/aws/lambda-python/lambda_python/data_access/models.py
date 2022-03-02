from abc import ABC, abstractmethod
from typing import List, Any, Dict
from data_access.db_lock import ResourceType

class classproperty(property):

    def __get__(self, cls, owner):
        return classmethod(self.fget).__get__(None, owner)()


class ResourceVersion(ABC):

    @property
    @abstractmethod
    def resource_type(self) -> ResourceType:
        pass

    @property
    @abstractmethod
    def resource_id(self) -> int:
        pass

    @property
    @abstractmethod
    def resource_version(self):
        pass

    @abstractmethod
    def update_version(self):
        pass
