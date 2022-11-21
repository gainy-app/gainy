from abc import ABC, abstractmethod
from redis import Redis


class Cache(ABC):

    @abstractmethod
    def set(self, key, value, ttl_seconds: int = 60):
        pass

    @abstractmethod
    def get(self, key):
        pass


class RedisCache(Cache):

    def __init__(self, host, port):
        self.redis = Redis(host=host, port=port)

    def set(self, key, value, ttl_seconds: int = 60):
        self.redis.set(name=key, value=value, ex=ttl_seconds)

    def get(self, key):
        return self.redis.get(key)


class LocalCache(Cache):

    def __init__(self):
        self.cache = {}

    def set(self, key, value, ttl_seconds: int = 60):
        self.cache[key] = value

    def get(self, key):
        return self.cache.get(key)


class CachingLoader:

    def __init__(self,
                 read_through_cache: Cache,
                 loader,
                 ttl_seconds: int = 60):
        self.read_through_cache = read_through_cache
        self.loader = loader
        self.ttl_seconds = ttl_seconds

    def get(self, key):
        existing_value = self.read_through_cache.get(key)
        if existing_value is None:
            new_value = self.loader(key)
            self.read_through_cache.set(key,
                                        new_value,
                                        ttl_seconds=self.ttl_seconds)
            return new_value
        else:
            return existing_value
