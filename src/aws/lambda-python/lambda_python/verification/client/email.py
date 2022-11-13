from services.twilio import TwilioClient, TWILIO_VERIFICATION_CHANNEL_EMAIL
from verification.client import VerificationClient
from verification.exceptions import WrongCodeException
from verification.models import VerificationCode, VerificationCodeChannel
from validate_email import validate_email


class EmailVerificationClient(VerificationClient):

    def __init__(self, twilio_client: TwilioClient):
        self.twilio_client = twilio_client

    def validate_address(self, entity: VerificationCode):
        if not validate_email(email_address=entity.address, check_smtp=False):
            raise Exception('Invalid email address.')

    def send(self, entity: VerificationCode):
        if not self.twilio_client.verification_create(
                entity.address, TWILIO_VERIFICATION_CHANNEL_EMAIL):
            raise Exception('Failed to send verification code.')

    def check_user_input(self,
                         entity: VerificationCode,
                         user_input: str = None):
        if not self.twilio_client.verification_check(entity.address,
                                                     user_input):
            raise WrongCodeException

    def supports(self, entity: VerificationCode) -> bool:
        return entity.channel == VerificationCodeChannel.EMAIL
