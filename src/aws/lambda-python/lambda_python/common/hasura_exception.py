class HasuraActionException(Exception):
    def __init__(self, http_code, message):
        super().__init__(message)
        self.http_code = http_code
        self.message = message
