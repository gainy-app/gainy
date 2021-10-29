import os
import sys
import datadog
from common.hasura_function import HasuraTrigger

script_directory = os.path.dirname(os.path.realpath(__file__))
sys.path.append(script_directory)

datadog.initialize()


class OnUserCreated(HasuraTrigger):
    def __init__(self, env):
        self.env = env
        super().__init__("on_user_created")

    def get_profile_id(self, op, data):
        return data['new']['id']

    def apply(self, db_conn, op, data):
        payload = self._extract_payload(data)
        profile_id = payload['id']
        datadog.api.Event.create(title="User Created",
                                 text="User Created #%d" % (profile_id),
                                 tags=["env:%s" % (self.env)])
