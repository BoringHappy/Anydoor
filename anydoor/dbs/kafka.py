from confluent_kafka import Producer, Consumer
from typing import Union
from confluent_kafka.admin import AdminClient


class Kafka:
    @staticmethod
    def get_producer(boot_server) -> Producer:
        return Producer(
            {
                "bootstrap.servers": boot_server,
            }
        )

    @staticmethod
    def get_consumer(boot_server, topic: Union[list, str], conf: dict) -> Consumer:
        consumer = Consumer(
            {
                "bootstrap.servers": boot_server,
                **conf,
            }
        )
        topic = [topic] if isinstance(topic, str) else topic
        consumer.subscribe(topic)
        return consumer

    @staticmethod
    def get_client(boot_server) -> AdminClient:
        return AdminClient(
            {
                "bootstrap.servers": boot_server,
            }
        )
