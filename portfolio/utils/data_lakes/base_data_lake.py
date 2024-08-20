from abc import ABC, abstractmethod


class BaseDataLake(ABC):
    """Abstract data lake class"""

    @abstractmethod
    def get(self, key):
        """
        Abstract method to retrieve data by key.

        Args:
            key (str): Data object identifier
        Returns:
            any: The data object
        """

    @abstractmethod
    def put(self, key, data):
        """
        Abstract method to save data under a given key.

        Args:
            key (str): Data object identifier
            data (str): Data to save
        """

    @abstractmethod
    def delete(self, key):
        """
        Abstract method to delete a key or list of keys from the data lake.

        Args:
            key (str|list): Data object identifier(s)
        """
