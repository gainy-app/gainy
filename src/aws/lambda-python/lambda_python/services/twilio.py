from functools import cached_property
import os
from twilio.rest import Client
from gainy.utils import get_logger

logger = get_logger(__name__)

TWILIO_ACCOUNT_SID = os.getenv('TWILIO_ACCOUNT_SID')
TWILIO_AUTH_TOKEN = os.getenv('TWILIO_AUTH_TOKEN')
TWILIO_SERVICE_ID = os.getenv('TWILIO_SERVICE_ID')

TWILIO_VERIFICATION_CHANNEL_SMS = 'sms'
TWILIO_VERIFICATION_CHANNEL_EMAIL = 'email'


class TwilioClient:

    def verification_create(self, address, channel: str):
        try:
            response = self._verification_service.verifications.create(
                to=address, channel=channel)
            logger.info('verification_create', extra={"response": response})
            return response.status == 'pending'
        except Exception as e:
            logger.exception('verification_create', extra={"exception": e})
            raise e

    def verification_check(self, address, code: str) -> bool:
        try:
            response = self._verification_service.verification_checks.create(
                to=address, code=code)
            logger.info('verification_check', extra={"response": response})
            return response.status == 'approved'
        except Exception as e:
            logger.exception('verification_check', extra={"exception": e})
            raise e

    def validate_phone_number(self, phone_number) -> bool:
        try:
            response = self._lookup_service.phone_numbers(phone_number).fetch()
            logger.info('validate_phone_number', extra={"response": response})
            return response.valid
        except Exception as e:
            logger.exception('validate_phone_number', extra={"exception": e})
            raise e

    @cached_property
    def _verification_service(self):
        return self._client.verify.v2.services(TWILIO_SERVICE_ID)

    @cached_property
    def _lookup_service(self):
        return self._client.lookups.v2

    @cached_property
    def _client(self):
        return Client(TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN)
