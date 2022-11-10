from gainy.exceptions import BadRequestException, HttpException


class EntityNotFoundException(Exception):

    def __init__(self, cls):
        super().__init__(f'Entity {cls} not found.')


class ValidationException(BadRequestException):

    def __init__(self, message='Validation Failed'):
        super().__init__(message)


class ForbiddenException(HttpException):

    def __init__(self, message='Forbidden.'):
        super().__init__(403, message)
