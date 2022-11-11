from services.twilio import TwilioClient, TWILIO_VERIFICATION_CHANNEL_EMAIL
from verification.client import VerificationClient
from verification.models import VerificationCode, VerificationCodeChannel


class EmailVerificationClient(VerificationClient):

    def __init__(self, twilio_client: TwilioClient):
        self.twilio_client = twilio_client

    def send(self, entity: VerificationCode):
        if not self.twilio_client.verification_create(
                entity.address, TWILIO_VERIFICATION_CHANNEL_EMAIL):
            raise Exception('Failed to send verification code.')

    def supports(self, entity: VerificationCode) -> bool:
        return entity.channel == VerificationCodeChannel.EMAIL
