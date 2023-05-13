import os

from abc import ABC

from exceptions import ValidationException
from gainy.data_access.operators import OperatorNotNull
from gainy.utils import get_logger
from services.gmaps import GoogleMaps, Address
from trading.repository import TradingRepository
from verification.models import VerificationCodeChannel, VerificationCode

logger = get_logger(__name__)

GOOGLE_PLACES_API_KEY = os.getenv('GOOGLE_PLACES_API_KEY')
ALLOWED_COUNTRY_CODES = ['US']


class KycFormValidator(ABC):

    def __init__(self, repository: TradingRepository,
                 google_maps_service: GoogleMaps):
        self.repository = repository
        self.google_maps_service = google_maps_service

    def validate_address(self, street1, street2, city, province, postal_code,
                         country) -> Address:

        logging_extra = {
            "street1": street1,
            "street2": street2,
            "city": city,
            "province": province,
            "postal_code": postal_code,
            "country": country,
        }

        address_parts = [
            street1, street2, city, province, postal_code, country
        ]
        query = ", ".join(filter(lambda x: x, address_parts))

        try:
            result = list(
                self.google_maps_service.suggest_addresses(
                    query,
                    limit=1,
                    allowed_country_codes=ALLOWED_COUNTRY_CODES))

            if not result:
                raise ValidationException("Failed to verify address.")

            return result[0]
        finally:
            logger.info("validate_address", extra=logging_extra)

    def validate_verification(self, profile_id: int,
                              channel: VerificationCodeChannel, address: str):
        entity = self.repository.find_one(
            VerificationCode, {
                "profile_id": profile_id,
                "channel": channel,
                "address": address,
                "verified_at": OperatorNotNull(),
            })

        if entity:
            return

        raise ValidationException(
            "%s communication address %s is not verified." %
            (channel.name, address))
