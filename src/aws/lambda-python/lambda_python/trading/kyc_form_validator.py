import os

from abc import ABC

from exceptions import ValidationException
from gainy.data_access.operators import OperatorLt, OperatorNotNull
from gainy.utils import get_logger, env, ENV_PRODUCTION
from trading.repository import TradingRepository
from verification.models import VerificationCodeChannel, VerificationCode

logger = get_logger(__name__)

GOOGLE_PLACES_API_KEY = os.getenv('GOOGLE_PLACES_API_KEY')
ALLOWED_COUNTRY_CODES = ['US']


class KycFormValidator(ABC):

    def __init__(self, repository: TradingRepository):
        self.repository = repository

    @staticmethod
    def validate_address(street1, street2, city, province, postal_code,
                         country):
        # https://developers.google.com/maps/documentation/places/web-service/supported_types
        import googlemaps

        logging_extra = {
            "street1": street1,
            "street2": street2,
            "city": city,
            "province": province,
            "postal_code": postal_code,
            "country": country,
        }

        try:
            gmaps = googlemaps.Client(key=GOOGLE_PLACES_API_KEY)

            address_parts = [
                street1, street2, city, province, postal_code, country
            ]
            address = ", ".join(filter(lambda x: x, address_parts))

            place_search = gmaps.places(query=address)
            logging_extra["place_search"] = place_search
            if not place_search['results']:
                raise ValidationException("Failed to verify address.")

            place_types = set(place_search['results'][0]["types"])
            if {"post_office", "post_box"}.intersection(place_types):
                raise ValidationException(
                    "Address must not be a post office address.")

            place_id = place_search['results'][0]["place_id"]
            place = gmaps.place(place_id)
            logging_extra["place"] = place

            address_components = place['result']['address_components']
            country_components = list(
                filter(lambda x: 'country' in x['types'], address_components))
            if not country_components:
                raise ValidationException("Failed to verify country.")

            country_code = country_components[0]['short_name']
            logging_extra["country_code"] = country_code
            if env(
            ) == ENV_PRODUCTION and country_code not in ALLOWED_COUNTRY_CODES:
                raise ValidationException(
                    'Currently the following countries are supported: ' +
                    ', '.join(ALLOWED_COUNTRY_CODES))

            suggested_postal_code = list(
                filter(lambda x: 'postal_code' in x['types'],
                       address_components))
            suggested_postal_code = suggested_postal_code[0][
                "short_name"] if suggested_postal_code else None
            suggested_province = list(
                filter(lambda x: 'administrative_area_level_1' in x['types'],
                       address_components))
            suggested_province = suggested_province[0][
                "short_name"] if suggested_province else None
            suggested_locality = list(
                filter(lambda x: 'locality' in x['types'], address_components))
            suggested_locality = suggested_locality[0][
                "short_name"] if suggested_locality else None
            suggested_street_number = list(
                filter(lambda x: 'street_number' in x['types'],
                       address_components))
            suggested_street_number = suggested_street_number[0][
                "short_name"] if suggested_street_number else None
            suggested_route = list(
                filter(lambda x: 'route' in x['types'], address_components))
            suggested_route = suggested_route[0][
                "short_name"] if suggested_route else None
            suggested_street1 = " ".join(
                filter(lambda x: x,
                       [suggested_street_number, suggested_route]))

            return {
                "street1": suggested_street1,
                "street2": street2,
                "city": suggested_locality,
                "province": suggested_province,
                "postal_code": suggested_postal_code,
                "country": country,
            }
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
