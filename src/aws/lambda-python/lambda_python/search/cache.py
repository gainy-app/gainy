from abc import ABC, abstractmethod
from redis import Redis


class Cache(ABC):

    @abstractmethod
    def set(self, key, value):
        pass

    @abstractmethod
    def get(self, key):
        pass


class RedisCache(Cache):

    def __init__(self, host, port, ttl_seconds: int = 60):
        self.redis = Redis(host=host, port=port)
        self.ttl_seconds = ttl_seconds

    def set(self, key, value):
        self.redis.set(name=key, value=value, ex=self.ttl_seconds)

    def get(self, key):
        return self.redis.get(key)


class CachingLoader:

    def __init__(self, read_through_cache: Cache, loader):
        self.read_through_cache = read_through_cache
        self.loader = loader

    def get(self, key):
        existing_value = self.read_through_cache.get(key)
        if existing_value is None:
            new_value = self.loader(key)
            self.read_through_cache.set(key, new_value)
            return new_value
        else:
            return existing_value
