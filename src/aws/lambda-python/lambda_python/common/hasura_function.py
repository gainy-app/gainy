from abc import abstractmethod, ABC

from common.hasura_exception import HasuraActionException


class HasuraAction(ABC):
    def __init__(self, name, profile_id_param=None):
        super().__init__()
        self.name = name
        self.profile_id_param = profile_id_param

    def get_profile_id(self, input_params):
        if not self.profile_id_param:
            return None

        profile_id = input_params.get(self.profile_id_param, None)
        if not profile_id:
            raise HasuraActionException(
                400, f"{self.name}: Profile id is not provided")

        return profile_id

    @abstractmethod
    def apply(self, db_conn, input_params):
        pass


class HasuraTrigger(ABC):
    def __init__(self, name):
        super().__init__()
        self.name = name

    @abstractmethod
    def apply(self, db_conn, op, data):
        pass

    @abstractmethod
    def get_profile_id(self, op, data):
        pass
