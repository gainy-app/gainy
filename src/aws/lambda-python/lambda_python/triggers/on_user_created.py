import os
import re
import sys
import logging
import datadog
from common.hasura_function import HasuraTrigger
from service.hubspot import HubspotService

logger = logging.getLogger()
logger.setLevel(logging.INFO)

script_directory = os.path.dirname(os.path.realpath(__file__))
sys.path.append(script_directory)

datadog.initialize()


class OnUserCreated(HasuraTrigger):

    def __init__(self, env):
        self.env = env
        self.hubspot_service = HubspotService()
        super().__init__("on_user_created")

    def get_allowed_profile_ids(self, op, data):
        return data['new']['id']

    def apply(self, db_conn, op, data):
        payload = self._extract_payload(data)
        profile_id = payload['id']
        email = payload["email"]

        if re.search(r'@gainy.app$', email) is not None:
            return

        try:
            datadog.api.Event.create(title="User Created",
                                     text="User Created #%d" % (profile_id),
                                     tags=["env:%s" % (self.env)])
        except Exception as e:
            logging.error("[%s] Exception when sending datadog event: %s",
                          __name__, e)

        if self.env == "production":
            try:
                self.hubspot_service.create_contact(email,
                                                    payload["first_name"],
                                                    payload["last_name"])
            except Exception as e:
                logging.error(
                    "[%s] Exception when creating hubspot contact: %s",
                    __name__, e)
