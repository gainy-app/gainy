from services.twilio import TwilioClient, TWILIO_VERIFICATION_CHANNEL_SMS
from verification.client import VerificationClient
from verification.exceptions import WrongCodeException
from verification.models import VerificationCode, VerificationCodeChannel


class SmsVerificationClient(VerificationClient):

    def __init__(self, twilio_client: TwilioClient):
        self.twilio_client = twilio_client

    def validate_address(self, entity: VerificationCode):
        if not self.twilio_client.validate_phone_number(entity.address):
            raise Exception('Invalid phone number.')

    def send(self, entity: VerificationCode):
        if not self.twilio_client.verification_create(
                entity.address, TWILIO_VERIFICATION_CHANNEL_SMS):
            raise Exception('Failed to send verification code.')

    def check_user_input(self,
                         entity: VerificationCode,
                         user_input: str = None):
        if not self.twilio_client.verification_check(entity.address,
                                                     user_input):
            raise WrongCodeException

    def supports(self, entity: VerificationCode) -> bool:
        return entity.channel == VerificationCodeChannel.SMS
