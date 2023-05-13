from typing import Iterable

import os

import googlemaps

from gainy.data_access.repository import Repository
from gainy.exceptions import EntityNotFoundException
from gainy.utils import get_logger
from services.cache import CachingLoader, Cache

GOOGLE_PLACES_API_KEY = os.getenv('GOOGLE_PLACES_API_KEY')
logger = get_logger(__name__)


class Address:

    def __init__(self,
                 formatted_address: str = None,
                 street1: str = None,
                 city: str = None,
                 province: str = None,
                 postal_code: str = None,
                 country: str = None):
        self.formatted_address = formatted_address
        self.street1 = street1
        self.city = city
        self.province = province
        self.postal_code = postal_code
        self.country = country


class GoogleMaps():

    def __init__(self, repository: Repository, cache: Cache):
        self.repository = repository
        self.cache = cache
        self.client = googlemaps.Client(key=GOOGLE_PLACES_API_KEY)

    def places(self, query):
        return self.client.places(query=query)

    def place(self, place_id):
        caching_loader = CachingLoader(self.cache,
                                       self.client.place,
                                       ttl_seconds=60 * 60)

        return caching_loader.get(place_id)

    def suggest_addresses(
            self,
            query: str,
            limit: int = None,
            post_office_allowed: bool = False,
            allowed_country_codes: list[str] = None) -> Iterable[Address]:
        logging_extra = {
            "query": query,
        }

        try:
            place_search = self.places(query=query)
            logging_extra["place_search"] = place_search
            if not place_search['results']:
                return []

            cnt = 0
            for i in place_search["results"]:
                if not post_office_allowed and {"post_office", "post_box"
                                                }.intersection(i["types"]):
                    continue

                formatted_address = i["formatted_address"]
                place_id = i["place_id"]
                place = self.place(place_id)
                logging_extra["place"] = place

                address_components = place['result']['address_components']
                country_components = list(
                    filter(lambda x: 'country' in x['types'],
                           address_components))
                if not country_components:
                    continue

                country_code2 = country_components[0]['short_name']
                logging_extra["country_code"] = country_code2
                country_code3 = self._get_country_code3(country_code2)
                logging_extra["country_code3"] = country_code3
                if allowed_country_codes and country_code2 not in allowed_country_codes and country_code3 not in allowed_country_codes:
                    continue

                postal_code = list(
                    filter(lambda x: 'postal_code' in x['types'],
                           address_components))
                postal_code = postal_code[0][
                    "short_name"] if postal_code else None
                province = list(
                    filter(
                        lambda x: 'administrative_area_level_1' in x['types'],
                        address_components))
                province = province[0]["short_name"] if province else None
                locality = list(
                    filter(lambda x: 'locality' in x['types'],
                           address_components))
                locality = locality[0]["short_name"] if locality else None
                street_number = list(
                    filter(lambda x: 'street_number' in x['types'],
                           address_components))
                street_number = street_number[0][
                    "short_name"] if street_number else None
                route = list(
                    filter(lambda x: 'route' in x['types'],
                           address_components))
                route = route[0]["short_name"] if route else None
                street1 = " ".join(filter(lambda x: x, [street_number, route]))

                address = Address(
                    formatted_address=formatted_address,
                    street1=street1,
                    city=locality,
                    province=province,
                    postal_code=postal_code,
                    country=country_code3,
                )
                yield address

                cnt += 1
                if limit is not None and cnt >= limit:
                    return
        finally:
            logger.info("suggest_addresses", extra=logging_extra)

    def _get_country_code3(self, country_code2):
        with self.repository.db_conn.cursor() as cursor:
            query = 'select "alpha-3" from raw_data.gainy_countries where "alpha-2" = %(code2)s'
            params = {"code2": country_code2}
            cursor.execute(query, params)
            row = cursor.fetchone()

        if row:
            return row[0]

        raise EntityNotFoundException('gainy_countries')
