from abc import abstractmethod, ABC

from common.hasura_exception import HasuraActionException


class HasuraAction(ABC):
    profile_id = None

    def __init__(self, name, profile_id_param=None):
        super().__init__()
        self.name = name
        self.profile_id_param = profile_id_param

    def get_profile_id(self, input_params):
        if self.profile_id is not None:
            return self.profile_id

        if not self.profile_id_param:
            return None

        profile_id = input_params.get(self.profile_id_param, None)
        if not profile_id:
            raise HasuraActionException(
                400, f"{self.name}: Profile id is not provided")

        return profile_id

    def is_applicable(self, name) -> bool:
        return name == self.name

    @abstractmethod
    def apply(self, db_conn, input_params, headers):
        pass


class HasuraTrigger(ABC):

    def __init__(self, name):
        super().__init__()
        self.names = name if type(name) is list else [name]

    def is_applicable(self, name) -> bool:
        return name in self.names

    @abstractmethod
    def apply(self, db_conn, op, data):
        pass

    @abstractmethod
    def get_profile_id(self, op, data):
        pass

    @staticmethod
    def _extract_payload(data):
        # Update old values with new values to properly handle updates
        if data["old"]:
            payload = data["old"]
        else:
            payload = {}

        if data["new"]:
            payload.update(data["new"])

        return payload
