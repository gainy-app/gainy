import os
import json
from service.logging import get_logger
from hubspot import HubSpot
from hubspot.crm.contacts import SimplePublicObjectInput
from hubspot.crm.contacts.exceptions import ApiException

logger = get_logger(__name__)

ENV = os.environ['ENV']
HUBSPOT_API_KEY = os.getenv("HUBSPOT_API_KEY")


class HubspotService:

    def __init__(self):
        self.api_client = None

    def create_contact(self, email, first_name, last_name):
        try:
            simple_public_object_input = SimplePublicObjectInput(
                properties={
                    "email": email,
                    "firstname": first_name,
                    "lastname": last_name,
                    "api": "app",
                })
            api_response = self.__get_client().crm.contacts.basic_api.create(
                simple_public_object_input=simple_public_object_input)
            logger.info("Successfully created hubspot contact %s",
                        json.dumps(api_response.to_dict(), default=str))
        except ApiException as e:
            logger.error("Exception when creating contact: %s", e)

    def __get_client(self):
        if self.api_client is None:
            self.api_client = HubSpot(api_key=HUBSPOT_API_KEY)

        return self.api_client
