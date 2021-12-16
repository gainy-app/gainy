import os
import logging
from hubspot import HubSpot
from hubspot.crm.contacts import SimplePublicObjectInput
from hubspot.crm.contacts.exceptions import ApiException

logger = logging.getLogger()
logger.setLevel(logging.INFO)

ENV = os.environ['ENV']
HUBSPOT_API_KEY = os.getenv("HUBSPOT_API_KEY")

class HubspotService:
    def create_contact(self, email, first_name, last_name):
        try:
            simple_public_object_input = SimplePublicObjectInput(
                properties={
                    "email": email,
                    "firstname": first_name,
                    "lastname": last_name,
                }
            )
            api_response = self.__get_client().crm.contacts.basic_api.create(
                simple_public_object_input=simple_public_object_input
            )
        except ApiException as e:
            logging.error("[%s] Exception when creating contact: %s", __name__, e)

    def __get_client(self):
        if self.api_client is None:
            self.api_client = HubSpot(api_key=HUBSPOT_API_KEY)

        return self.api_client

