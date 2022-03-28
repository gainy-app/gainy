import json


class AccessTokenApiException(Exception):

    def __init__(self, parent_exc: Exception, access_token: dict):
        self.parent_exc = parent_exc
        self.access_token = access_token

    def __str__(self):
        return json.dumps(self.parent_exc.__dict__)


class AccessTokenLoginRequiredException(AccessTokenApiException):
    pass
