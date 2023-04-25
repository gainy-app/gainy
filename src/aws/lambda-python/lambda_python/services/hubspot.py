import os
import json
from gainy.utils import get_logger
from hubspot import HubSpot
from hubspot.crm.contacts import SimplePublicObjectInput
from hubspot.crm.contacts.exceptions import ApiException

logger = get_logger(__name__)

ENV = os.environ['ENV']
HUBSPOT_APP_TOKEN = os.getenv("HUBSPOT_APP_TOKEN")


class HubspotService:

    def __init__(self):
        self.client = HubSpot(access_token=HUBSPOT_APP_TOKEN)

    def create_contact(self, email, first_name, last_name):
        try:
            simple_public_object_input = SimplePublicObjectInput(
                properties={
                    "email": email,
                    "firstname": first_name,
                    "lastname": last_name,
                    "api": "app",
                })
            api_response = self.client.crm.contacts.basic_api.create(
                simple_public_object_input=simple_public_object_input)
            logger.info("Successfully created hubspot contact %s",
                        json.dumps(api_response.to_dict(), default=str))
        except ApiException as e:
            if e.status == 409:
                logger.info("Conflict when creating hubspot contact",
                            extra={"e": e})
                # Contact already exists.
                return
            logger.exception("Exception when creating hubspot contact",
                             extra={"e": e})
