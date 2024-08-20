class FactoryException(Exception):
    """Exception for when a Factory cannot find the correct subclass"""


class RecordValidationException(Exception):
    """Exception for when a Record is not initialized correctly"""


class HTTPDataLoadException(Exception):
    """Exception for when an HTML page cannot be loaded successfully"""
