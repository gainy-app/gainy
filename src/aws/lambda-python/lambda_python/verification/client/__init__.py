from abc import ABC

from verification.models import VerificationCode


class VerificationClient(ABC):

    def send(self, entity: VerificationCode):
        pass

    def check_user_input(self,
                         entity: VerificationCode,
                         user_input: str = None):
        pass

    def supports(self, entity: VerificationCode) -> bool:
        pass

    def validate_address(self, entity: VerificationCode):
        pass
