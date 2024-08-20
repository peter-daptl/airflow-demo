from abc import ABC, abstractmethod


class BaseTransformer(ABC):
    def __init__(self, *args, **kwargs):
        super().__init__()

    @abstractmethod
    def transform(self, extracted_key):
        """
        Abstract method to extract data from a data source.

        Returns:
            List/Dict: List or Dict of extracted objects
        """
