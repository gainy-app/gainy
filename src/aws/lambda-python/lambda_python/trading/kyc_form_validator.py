import os

from abc import ABC

from exceptions import ValidationException
from gainy.utils import get_logger

logger = get_logger(__name__)

GOOGLE_PLACES_API_KEY = os.getenv('GOOGLE_PLACES_API_KEY')


class KycFormValidator(ABC):

    @staticmethod
    def validate_not_po(street1, street2, city, province, postal_code,
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
            geocode_result = gmaps.geocode(address)
            logging_extra["geocode_result"] = geocode_result

            if not geocode_result:
                raise ValidationException("Could not decode address.")

            place_types = set(geocode_result[0]["types"])
            if {"post_office", "post_box"}.intersection(place_types):
                raise ValidationException(
                    "Address must not be a post office address.")

            # place_id = geocode_result[0]["place_id"]
            # place_details = gmaps.place(place_id, fields=["type"])
            #
            # logging_extra["place_details"] = place_details
            #
            # place_search = gmaps.places(query=address, type="post_office")
            #
            # logging_extra["place_search"] = place_search
        finally:
            logger.info("validate_not_po", extra=logging_extra)
