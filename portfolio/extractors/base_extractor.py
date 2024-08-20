from abc import ABC, abstractmethod


class BaseExtractor(ABC):
    def __init__(self, *args, **kwargs):
        super().__init__()

    @abstractmethod
    def extract(self):
        """
        Abstract method to extract data from a data source.

        Returns:
            List/Dict: List or Dict of extracted objects
        """
