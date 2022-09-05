from common.hasura_exception import HasuraActionException


class ApiException(Exception):
    pass


class NotFoundException(HasuraActionException):

    def __init__(self, message='Not Found.'):
        super().__init__(404, message)
