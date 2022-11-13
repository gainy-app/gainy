from abc import ABC, abstractmethod

from verification.models import VerificationCode


class VerificationClient(ABC):

    @abstractmethod
    def send(self, entity: VerificationCode):
        pass

    @abstractmethod
    def check_user_input(self,
                         entity: VerificationCode,
                         user_input: str = None):
        pass

    @abstractmethod
    def supports(self, entity: VerificationCode) -> bool:
        pass

    @abstractmethod
    def validate_address(self, entity: VerificationCode):
        pass
