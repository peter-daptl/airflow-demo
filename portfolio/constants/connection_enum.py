from enum import Enum


class Connection(Enum):
    """
    Connection enum corresponding to airflow connections
    """

    CYCLING_DB = "cycling_db"
    KAFKA_BROKER = "kafka_broker"
    REDIS = "redis"
