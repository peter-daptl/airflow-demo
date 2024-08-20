import logging

from portfolio.utils.data_lakes.redis_data_lake import RedisDataLake


class DataLake:
    """DataLake wrapper class that currently uses the RedisDataLake"""

    def __init__(self, bucket=None):
        self.data_lake = RedisDataLake(bucket=bucket)

    def get(self, key, get_csv=False):
        """
        Retrieves data by key.

        Args:
            key (str): Data object identifier
            get_csv (bool): If True use get_csv function
        Returns:
            any: The data object
        """
        logging.info(f"Retrieving item from data lake with key: {key}")
        if get_csv:
            return self.data_lake.get_csv(key)

        return self.data_lake.get(key)

    def put(self, key, data):
        """
        Saves data under a given key.

        Args:
            key (str): Data object identifier
            data (str): Data to save
        """
        logging.info(f"Saving item to data lake with key: {key}")
        self.data_lake.put(key, data)
        logging.info(f"Saved item to data lake with key: {key}")

    def delete(self, key):
        """
        Deletes a key or list of keys from the data lake.

        Args:
            key (str|list): Data object identifier(s)
        """
        self.data_lake.delete(key)

    def get_all(self):
        """
        Retrieves all data objects from the data lake.

        Returns:
            any: Collection of data objects from the data lake.
        """
        return self.data_lake.get_all()
