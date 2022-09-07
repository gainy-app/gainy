from common.hasura_exception import HasuraActionException


class ApiException(Exception):
    pass


class NotFoundException(HasuraActionException):

    def __init__(self, message='Not Found.'):
        super().__init__(404, message)


class BadRequestException(HasuraActionException):

    def __init__(self, message='Bad Request.'):
        super().__init__(400, message)
