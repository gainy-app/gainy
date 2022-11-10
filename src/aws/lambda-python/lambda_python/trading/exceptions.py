class WrongTradingCollectionVersionStatusException(Exception):
    def __init__(self, message='Wrong TradingCollectionVersion Status', *args):
        super().__init__(message, *args)
