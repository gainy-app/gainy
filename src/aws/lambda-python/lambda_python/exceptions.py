class EntityNotFoundException(Exception):

    def __init__(self, cls):
        super().__init__(f'Entity {cls} not found.')
