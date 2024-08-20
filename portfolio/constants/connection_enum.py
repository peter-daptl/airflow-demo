from enum import Enum


class Connection(Enum):
    """
    Connection enum corresponding to airflow connections
    """

    CYCLING_DB = "cycling_db"
    REDIS = "redis"
