from gainy.exceptions import BadRequestException


class WrongTradingOrderStatusException(Exception):

    def __init__(self, message='Wrong TradingCollectionVersion Status', *args):
        super().__init__(message, *args)


class CannotDeleteFundingAccountException(Exception):

    def __init__(self, message=None, *args):
        self.message = message
        super().__init__(message, *args)


class CannotDeleteFundingAccountHttpException(BadRequestException):
    pass
