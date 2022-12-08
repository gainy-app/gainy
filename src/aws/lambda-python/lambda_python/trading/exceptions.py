class WrongTradingCollectionVersionStatusException(Exception):

    def __init__(self, message='Wrong TradingCollectionVersion Status', *args):
        super().__init__(message, *args)


class SymbolIsNotTradeableException(Exception):

    def __init__(self, symbol):
        super().__init__('Symbol %s is not tradeable.', symbol)
