class WrongCodeException(Exception):

    def __init__(self, message='Wrong code.', *args):
        super().__init__(message, *args)


class CodeExpiredException(Exception):

    def __init__(self, message='Code expired.', *args):
        super().__init__(message, *args)


class CooldownException(Exception):

    def __init__(
            self,
            message='You have requested a verification code too soon from the previous one.',
            *args):
        super().__init__(message, *args)


class CodeAlreadyUsedException(Exception):

    def __init__(self,
                 message='Verification code has already been used.',
                 *args):
        super().__init__(message, *args)
