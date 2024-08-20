import json

from airflow.providers.redis.hooks.redis import RedisHook

from portfolio.constants.connection_enum import Connection
from portfolio.utils.data_lakes.base_data_lake import BaseDataLake


class RedisDataLake(BaseDataLake):
    """
    Redis data lake implementation.
    Uses the RedisHook to retrieve, add, or remove objects from/to a bucket.
    """

    def __init__(self, bucket=None):
        self.redis = RedisHook(Connection.REDIS.value)
        self.redis = self.redis.get_conn()

    def get(self, key):
        """
        Retrieve data by key.

        Args:
            key (str): Object key to retrieve
        Returns:
            any: The data object
        """

        return json.loads(self.redis.get(key))

    def put(self, key, data, timeout=7200):
        """
        Saves the data under the key in Redis.

        Args:
            key (str): Object key to save
            data (str): Data to store in Redis
        """
        self.redis.set(key, data, ex=timeout)

    def delete(self, key):
        """
        Deletes a key or list of keys from Redis.

        Args:
            key (str|list): Object key(s) to be deleted
        """
        self.redis.delete(key)
