from time import sleep
from uuid import uuid1

import pytest
from confluent_kafka.admin import NewTopic

from anydoor.dbs import Kafka


@pytest.mark.skip(reason="Call API")
def test_kafka():
    boot_server = "192.168.5.244:9092"
    test_topic_prefix = "_test_topic_"
    test_topic = f"{test_topic_prefix}{uuid1()}"
    test_key = "key"
    test_value = "value"
    producer = Kafka.get_producer(boot_server=boot_server)

    client = Kafka.get_client(boot_server=boot_server)
    create_result = client.create_topics([NewTopic(test_topic)])
    print(create_result)
    sleep(3)
    assert test_topic in list(client.list_topics().topics.keys())
    print(list(client.list_topics().topics.keys()))

    consumer = Kafka.get_consumer(
        boot_server=boot_server,
        topic=test_topic,
        conf={
            "group.id": "pytest",
            "auto.offset.reset": "earliest",
        },
    )
    try:
        for i in range(20):
            msg = consumer.poll(1.0)
            if msg is not None:
                print(msg.key())
                received_key = msg.key().decode("utf-8")
                received_value = msg.value().decode("utf-8")
                assert received_key == test_key
                assert received_value == test_value
                break
            else:
                producer.produce(test_topic, key=test_key, value=test_value)
                producer.poll(1)
                producer.flush()
            sleep(1)
        else:
            raise BufferError("Kafka Test Failure")

    finally:

        def get_test_topic():
            return [
                topic
                for topic in list(client.list_topics().topics.keys())
                if topic.startswith(test_topic_prefix)
            ]

        client.delete_topics(get_test_topic())
        sleep(5)
        assert len(get_test_topic()) == 0


if __name__ == "__main__":
    test_kafka()
if __name__ == "__main__":
    test_kafka()
