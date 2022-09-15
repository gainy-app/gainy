def mock_find_one(options):

    def mock(_cls, _fltr):
        for cls, fltr, entity in options:
            if cls == _cls and fltr == _fltr:
                return entity

        raise Exception('unknown find_one call: %s %s', _cls, _fltr)

    return mock


def mock_persist(persisted_objects: dict = None):

    def mock(_entity):
        if persisted_objects is None:
            return

        if _entity.__class__ not in persisted_objects:
            persisted_objects[_entity.__class__] = []

        persisted_objects[_entity.__class__].append(_entity)

    return mock


def mock_noop(*args, **kwargs):
    pass
