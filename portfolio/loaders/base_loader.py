import logging
from confluent_kafka import Producer
from portfolio.constants.connection_enum import Connection
from portfolio.constants.variable_enum import Variable
from portfolio.utils.config import Config


class BaseLoader:

    def __init__(self):
        kafka_config = Config.get_connection(Connection.KAFKA_BROKER.value).extra_dejson
        self.producer = Producer(kafka_config)
        self.kafka_topic = Config.get_variable(Variable.KAFKA_TOPIC.value)

    def load(self, records):
        """
        Formats and executes data load for record in records.

        Args:
            records (list): List of dicts in "record" format
        """
        # config = {"bootstrap.servers": "redpanda:29092", "acks": "all"}

        for record in records:
            self.producer.produce(
                self.kafka_topic, record, "rank", callback=self.delivery_callback
            )

        self.producer.poll(10000)
        self.producer.flush()

    def delivery_callback(self, err, msg):
        if err:
            logging.error("ERROR: Message failed delivery: {}".format(err))
        else:
            logging.info(
                "Produced event to topic {topic}: key = {key:12} value = {value:12}".format(
                    topic=msg.topic(),
                    key=msg.key().decode("utf-8"),
                    value=msg.value().decode("utf-8"),
                )
            )
