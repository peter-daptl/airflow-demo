import logging
from confluent_kafka import Producer
from portfolio.constants.connection_enum import Connection
from portfolio.constants.variable_enum import Variable
from portfolio.utils.config import Config
from portfolio.loaders.base_loader import BaseLoader
from portfolio.utils.exceptions import KafkaBrokerException


class UCIRankingsLoader(BaseLoader):
    '''
    Kafka version for Demo only.  While overridden from Base, has nothing
    in common with it.  Just not rewriting all logic for this Kafka Demo.
    '''
    def __init__(self):
        self.kafka_config = Config.get_connection(Connection.KAFKA_BROKER.value).extra_dejson
        self.producer = Producer(self.kafka_config)
        self.kafka_topic = Config.get_variable(Variable.KAFKA_TOPIC.value)

    def load(self, records):
        """
        Formats and executes data load for record in records.

        Args:
            records (list): List of dicts in "record" format
        """

        for record in records:
            self.producer.produce(
                self.kafka_topic, record, "rank", callback=self.delivery_callback
            )
        self.producer.poll(1)
        queued_count = self.producer.flush(10)
        if queued_count > 0:
            msg = f"{queued_count} messages were unable to be added to topic."
            logging.error(msg)
            raise KafkaBrokerException(msg)

    def delivery_callback(self, err, msg):
        """
        Callback for the producer to log the message to Kafka.

        Args:
            err (str): The error message
            msg (ProducerRecord): The message to log
        """
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
