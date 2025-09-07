import json
from abc import ABC, abstractmethod
from contextlib import contextmanager
from io import BytesIO
from typing import Any, Dict, List, Optional, Union

import fastavro
import pydantic_avro  # noqa: F401
from confluent_kafka import Consumer, Producer
from confluent_kafka.admin import AdminClient
from confluent_kafka.error import KafkaException
from loguru import logger


class BaseSerializer(ABC):
    """Base class for Kafka message serializers"""

    @abstractmethod
    def serialize(self, data: Any) -> bytes:
        """Serialize data to bytes"""
        pass

    @abstractmethod
    def deserialize(self, data: bytes) -> Any:
        """Deserialize bytes to data"""
        pass


class JSONSerializer(BaseSerializer):
    """JSON serializer for Kafka messages"""

    def __init__(self, encoding: str = "utf-8"):
        self.encoding = encoding

    def serialize(self, data: Any) -> bytes:
        """Serialize data to JSON bytes"""
        try:
            json_str = json.dumps(data, ensure_ascii=False, separators=(",", ":"))
            return json_str.encode(self.encoding)
        except (TypeError, ValueError) as e:
            raise ValueError(f"Failed to serialize data to JSON: {e}")

    def deserialize(self, data: bytes) -> Any:
        """Deserialize JSON bytes to data"""
        try:
            json_str = data.decode(self.encoding)
            return json.loads(json_str)
        except (UnicodeDecodeError, json.JSONDecodeError) as e:
            raise ValueError(f"Failed to deserialize JSON data: {e}")


class StringSerializer(BaseSerializer):
    """String serializer for Kafka messages"""

    def __init__(self, encoding: str = "utf-8"):
        self.encoding = encoding

    def serialize(self, data: str) -> bytes:
        """Serialize string to bytes"""
        if not isinstance(data, str):
            raise ValueError("Data must be a string")
        return data.encode(self.encoding)

    def deserialize(self, data: bytes) -> str:
        """Deserialize bytes to string"""
        try:
            return data.decode(self.encoding)
        except UnicodeDecodeError as e:
            raise ValueError(f"Failed to deserialize string data: {e}")


class BytesSerializer(BaseSerializer):
    """Bytes serializer (pass-through) for Kafka messages"""

    def serialize(self, data: bytes) -> bytes:
        """Pass-through bytes serialization"""
        if not isinstance(data, bytes):
            raise ValueError("Data must be bytes")
        return data

    def deserialize(self, data: bytes) -> bytes:
        """Pass-through bytes deserialization"""
        return data


class AvroSerializer(BaseSerializer):
    """Avro serializer for Kafka messages with schema support"""

    def __init__(
        self, schema: Optional[Dict] = None, schema_registry_url: Optional[str] = None
    ):
        """
        Initialize Avro serializer

        Args:
            schema: Avro schema as dict (for schemaless mode)
            schema_registry_url: URL for Confluent Schema Registry (for schema registry mode)
        """
        self.schema = schema
        self.schema_registry_url = schema_registry_url

        if schema_registry_url:
            try:
                from confluent_kafka.schema_registry import SchemaRegistryClient

                self.schema_registry_client = SchemaRegistryClient(
                    {"url": schema_registry_url}
                )
                self._use_schema_registry = True
            except ImportError:
                raise ImportError(
                    "confluent-kafka[avro] is required for Schema Registry support. "
                    "Install with: pip install confluent-kafka[avro]"
                )
        else:
            self._use_schema_registry = False
            if not schema:
                raise ValueError(
                    "Either schema or schema_registry_url must be provided"
                )

    def serialize(self, data: Any) -> bytes:
        """Serialize data to Avro bytes"""
        try:
            if self._use_schema_registry:
                # Use Schema Registry mode (requires confluent-kafka[avro])
                raise NotImplementedError("Schema Registry mode implementation pending")
            else:
                # Use schemaless mode with fastavro
                if hasattr(data, "avro_schema"):
                    # Handle pydantic-avro models
                    schema = data.avro_schema()
                    data_dict = (
                        data.model_dump()
                        if hasattr(data, "model_dump")
                        else data.dict()
                    )
                    return self._serialize_with_schema(data_dict, schema)
                elif self.schema:
                    # Handle regular data with provided schema
                    return self._serialize_with_schema(data, self.schema)
                else:
                    raise ValueError("No schema available for serialization")

        except Exception as e:
            raise ValueError(f"Failed to serialize data to Avro: {e}")

    def deserialize(self, data: bytes, model_class: Optional[type] = None) -> Any:
        """
        Deserialize Avro bytes to data

        Args:
            data: Avro bytes to deserialize
            model_class: Optional pydantic-avro model class for typed deserialization

        Returns:
            Deserialized data (dict or model instance)
        """
        try:
            if self._use_schema_registry:
                # Use Schema Registry mode
                raise NotImplementedError("Schema Registry mode implementation pending")
            else:
                # Use schemaless mode with fastavro
                if model_class and hasattr(model_class, "avro_schema"):
                    # Deserialize to pydantic-avro model
                    schema = model_class.avro_schema()
                    data_dict = self._deserialize_with_schema(data, schema)
                    return model_class(**data_dict)
                elif self.schema:
                    # Deserialize to dict with provided schema
                    return self._deserialize_with_schema(data, self.schema)
                else:
                    raise ValueError("No schema available for deserialization")

        except Exception as e:
            raise ValueError(f"Failed to deserialize Avro data: {e}")

    def _serialize_with_schema(self, data: Any, schema: Dict) -> bytes:
        """Serialize data with given schema using fastavro"""
        bytes_writer = BytesIO()
        fastavro.schemaless_writer(bytes_writer, schema, data)
        return bytes_writer.getvalue()

    def _deserialize_with_schema(self, data: bytes, schema: Dict) -> Any:
        """Deserialize data with given schema using fastavro"""
        bytes_reader = BytesIO(data)
        return fastavro.schemaless_reader(bytes_reader, schema)


class PydanticAvroSerializer(AvroSerializer):
    """Specialized Avro serializer for pydantic-avro models"""

    def __init__(self, model_class: Optional[type] = None):
        """
        Initialize serializer for pydantic-avro models

        Args:
            model_class: pydantic-avro model class (optional, can be inferred)
        """

        self.model_class = model_class
        # Don't call super().__init__() as we handle schema differently

    def serialize(self, data: Any) -> bytes:
        """Serialize pydantic-avro model to Avro bytes"""
        try:
            if hasattr(data, "avro_schema") and hasattr(data, "model_dump"):
                # This is a pydantic-avro model
                schema = data.avro_schema()
                data_dict = data.model_dump()
                return self._serialize_with_schema(data_dict, schema)
            else:
                raise ValueError("Data must be a pydantic-avro model instance")
        except Exception as e:
            raise ValueError(f"Failed to serialize pydantic-avro model: {e}")

    def deserialize(self, data: bytes, model_class: Optional[type] = None) -> Any:
        """Deserialize Avro bytes to pydantic-avro model"""
        target_class = model_class or self.model_class

        if not target_class:
            raise ValueError(
                "model_class must be provided either in constructor or method call"
            )

        if not hasattr(target_class, "avro_schema"):
            raise ValueError("model_class must be a pydantic-avro model")

        try:
            schema = target_class.avro_schema()
            data_dict = self._deserialize_with_schema(data, schema)
            return target_class(**data_dict)
        except Exception as e:
            raise ValueError(f"Failed to deserialize to pydantic-avro model: {e}")


class KafkaConfig:
    """Kafka configuration management with defaults and validation"""

    DEFAULT_PRODUCER_CONFIG = {
        "acks": "all",
        "retries": 3,
        "batch.size": 16384,
        "linger.ms": 1,
        "buffer.memory": 33554432,
        "compression.type": "snappy",
        "max.in.flight.requests.per.connection": 5,
        "enable.idempotence": True,
    }

    DEFAULT_CONSUMER_CONFIG = {
        "auto.offset.reset": "earliest",
        "enable.auto.commit": True,
        "auto.commit.interval.ms": 1000,
        "session.timeout.ms": 30000,
        "heartbeat.interval.ms": 3000,
        "max.poll.records": 500,
        "fetch.min.bytes": 1,
        "fetch.max.wait.ms": 500,
    }

    @classmethod
    def merge_producer_config(
        cls, bootstrap_servers: str, custom_config: Optional[Dict] = None
    ) -> Dict:
        """Merge default producer config with custom config"""
        config = {"bootstrap.servers": bootstrap_servers}
        config.update(cls.DEFAULT_PRODUCER_CONFIG)
        if custom_config:
            config.update(custom_config)
        return config

    @classmethod
    def merge_consumer_config(
        cls, bootstrap_servers: str, custom_config: Optional[Dict] = None
    ) -> Dict:
        """Merge default consumer config with custom config"""
        config = {"bootstrap.servers": bootstrap_servers}
        config.update(cls.DEFAULT_CONSUMER_CONFIG)
        if custom_config:
            config.update(custom_config)
        return config


class Kafka:
    """Enhanced Kafka client with better configuration, error handling, and resource management"""

    @staticmethod
    def get_producer(
        bootstrap_servers: str,
        config: Optional[Dict] = None,
        validate_connection: bool = True,
    ) -> Producer:
        """
        Create a Kafka producer with enhanced configuration and validation

        Args:
            bootstrap_servers: Comma-separated list of Kafka brokers
            config: Additional producer configuration
            validate_connection: Whether to validate connection on creation

        Returns:
            Producer: Configured Kafka producer

        Raises:
            ValueError: If bootstrap_servers is invalid
            KafkaException: If connection validation fails
        """
        if not bootstrap_servers or not isinstance(bootstrap_servers, str):
            raise ValueError("bootstrap_servers must be a non-empty string")

        producer_config = KafkaConfig.merge_producer_config(bootstrap_servers, config)

        try:
            producer = Producer(producer_config)

            if validate_connection:
                # Test connection by getting cluster metadata
                metadata = producer.list_topics(timeout=5)
                logger.info(
                    f"Connected to Kafka cluster with {len(metadata.brokers)} brokers"
                )

            return producer

        except KafkaException as e:
            logger.error(f"Failed to create Kafka producer: {e}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error creating Kafka producer: {e}")
            raise KafkaException(f"Producer creation failed: {e}")

    @staticmethod
    def get_consumer(
        bootstrap_servers: str,
        topics: Union[List[str], str],
        group_id: str,
        config: Optional[Dict] = None,
        validate_connection: bool = True,
    ) -> Consumer:
        """
        Create a Kafka consumer with enhanced configuration and validation

        Args:
            bootstrap_servers: Comma-separated list of Kafka brokers
            topics: Topic(s) to subscribe to
            group_id: Consumer group ID
            config: Additional consumer configuration
            validate_connection: Whether to validate connection on creation

        Returns:
            Consumer: Configured Kafka consumer

        Raises:
            ValueError: If parameters are invalid
            KafkaException: If connection validation fails
        """
        if not bootstrap_servers or not isinstance(bootstrap_servers, str):
            raise ValueError("bootstrap_servers must be a non-empty string")

        if not group_id or not isinstance(group_id, str):
            raise ValueError("group_id must be a non-empty string")

        if not topics:
            raise ValueError("topics must be provided")

        # Normalize topics to list
        topic_list = [topics] if isinstance(topics, str) else list(topics)
        if not all(isinstance(topic, str) and topic.strip() for topic in topic_list):
            raise ValueError("All topics must be non-empty strings")

        # Merge configuration
        consumer_config = KafkaConfig.merge_consumer_config(bootstrap_servers, config)
        consumer_config["group.id"] = group_id

        try:
            consumer = Consumer(consumer_config)
            consumer.subscribe(topic_list)

            if validate_connection:
                # Test connection by getting cluster metadata
                metadata = consumer.list_topics(timeout=5)
                logger.info(
                    f"Consumer connected to Kafka cluster with {len(metadata.brokers)} brokers"
                )
                logger.info(f"Subscribed to topics: {topic_list}")

            return consumer

        except KafkaException as e:
            logger.error(f"Failed to create Kafka consumer: {e}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error creating Kafka consumer: {e}")
            raise KafkaException(f"Consumer creation failed: {e}")

    @staticmethod
    def get_admin_client(
        bootstrap_servers: str,
        config: Optional[Dict] = None,
        validate_connection: bool = True,
    ) -> AdminClient:
        """
        Create a Kafka admin client with enhanced configuration and validation

        Args:
            bootstrap_servers: Comma-separated list of Kafka brokers
            config: Additional admin client configuration
            validate_connection: Whether to validate connection on creation

        Returns:
            AdminClient: Configured Kafka admin client

        Raises:
            ValueError: If bootstrap_servers is invalid
            KafkaException: If connection validation fails
        """
        if not bootstrap_servers or not isinstance(bootstrap_servers, str):
            raise ValueError("bootstrap_servers must be a non-empty string")

        admin_config = {"bootstrap.servers": bootstrap_servers}
        if config:
            admin_config.update(config)

        try:
            admin_client = AdminClient(admin_config)

            if validate_connection:
                # Test connection by getting cluster metadata
                metadata = admin_client.list_topics(timeout=5)
                logger.info(
                    f"Admin client connected to Kafka cluster with {len(metadata.brokers)} brokers"
                )

            return admin_client

        except KafkaException as e:
            logger.error(f"Failed to create Kafka admin client: {e}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error creating Kafka admin client: {e}")
            raise KafkaException(f"Admin client creation failed: {e}")

    @contextmanager
    def managed_producer(self, bootstrap_servers: str, config: Optional[Dict] = None):
        """Context manager for producer with automatic cleanup"""
        producer = None
        try:
            producer = self.get_producer(bootstrap_servers, config)
            yield producer
        finally:
            if producer:
                producer.flush(timeout=10)
                logger.info("Producer flushed and cleaned up")

    @contextmanager
    def managed_consumer(
        self,
        bootstrap_servers: str,
        topics: Union[List[str], str],
        group_id: str,
        config: Optional[Dict] = None,
    ):
        """Context manager for consumer with automatic cleanup"""
        consumer = None
        try:
            consumer = self.get_consumer(bootstrap_servers, topics, group_id, config)
            yield consumer
        finally:
            if consumer:
                consumer.close()
                logger.info("Consumer closed and cleaned up")


class KafkaClient:
    """
    High-level Kafka client with built-in serialization support

    This client provides automatic serialization/deserialization of messages,
    making it easy to work with structured data in Kafka.

    Example:
        ```python
        # Basic usage with JSON serialization
        client = KafkaClient(
            bootstrap_servers="localhost:9092",
            key_serializer=StringSerializer(),
            value_serializer=JSONSerializer()
        )

        # 自动序列化发送
        client.produce("topic", {"message": "data"}, key="key1")

        # 自动反序列化消费
        for msg in client.consume(["topic"], "group"):
            print(msg['value'])  # 已反序列化的Python对象

        # Advanced usage with Pydantic Avro models
        from pydantic_avro import AvroBase

        class User(AvroBase):
            id: int
            name: str
            email: str
            age: Optional[int] = None

        avro_client = KafkaClient(
            bootstrap_servers="localhost:9092",
            key_serializer=StringSerializer(),
            value_serializer=PydanticAvroSerializer(User)
        )

        # 发送类型安全的模型
        user = User(id=1, name="张三", email="test@example.com", age=25)
        avro_client.produce("users", user, key=f"user_{user.id}")

        # 接收类型安全的模型
        for msg in avro_client.consume(["users"], "user-group"):
            user_obj = msg['value']  # User类型的实例
            print(f"用户: {user_obj.name}, 邮箱: {user_obj.email}")
        ```
    """

    def __init__(
        self,
        bootstrap_servers: str,
        key_serializer: Optional[BaseSerializer] = None,
        value_serializer: Optional[BaseSerializer] = None,
        default_config: Optional[Dict] = None,
    ):
        """
        Initialize Kafka client with serializers

        Args:
            bootstrap_servers: Comma-separated list of Kafka brokers
            key_serializer: Serializer for message keys
            value_serializer: Serializer for message values
            default_config: Default configuration for producers/consumers
        """
        self.bootstrap_servers = bootstrap_servers
        self.key_serializer = key_serializer or StringSerializer()
        self.value_serializer = value_serializer or JSONSerializer()
        self.default_config = default_config or {}

        # Cache for clients
        self._producer = None
        self._admin_client = None

    def get_producer(self, config: Optional[Dict] = None) -> Producer:
        """Get or create a producer instance"""
        if self._producer is None:
            merged_config = {**self.default_config, **(config or {})}
            self._producer = Kafka.get_producer(self.bootstrap_servers, merged_config)
        return self._producer

    def get_admin_client(self, config: Optional[Dict] = None) -> AdminClient:
        """Get or create an admin client instance"""
        if self._admin_client is None:
            merged_config = {**self.default_config, **(config or {})}
            self._admin_client = Kafka.get_admin_client(
                self.bootstrap_servers, merged_config
            )
        return self._admin_client

    def produce(
        self,
        topic: str,
        value: Any,
        key: Optional[Any] = None,
        partition: Optional[int] = None,
        headers: Optional[Dict[str, str]] = None,
        callback: Optional[callable] = None,
    ) -> None:
        """
        Produce a message with automatic serialization

        Args:
            topic: Topic to produce to
            value: Message value (will be serialized)
            key: Message key (will be serialized if provided)
            partition: Specific partition to produce to
            headers: Message headers
            callback: Delivery callback function
        """
        producer = self.get_producer()

        try:
            # Serialize value
            serialized_value = self.value_serializer.serialize(value)

            # Serialize key if provided
            serialized_key = None
            if key is not None:
                serialized_key = self.key_serializer.serialize(key)

            # Convert headers to bytes if provided
            serialized_headers = None
            if headers:
                serialized_headers = {
                    k: v.encode("utf-8") if isinstance(v, str) else v
                    for k, v in headers.items()
                }

            producer.produce(
                topic=topic,
                value=serialized_value,
                key=serialized_key,
                partition=partition,
                headers=serialized_headers,
                callback=callback,
            )

        except Exception as e:
            logger.error(f"Failed to produce message to topic '{topic}': {e}")
            raise

    def consume(
        self,
        topics: Union[List[str], str],
        group_id: str,
        config: Optional[Dict] = None,
        timeout: float = 1.0,
        max_messages: Optional[int] = None,
    ):
        """
        Consume messages with automatic deserialization

        Args:
            topics: Topic(s) to consume from
            group_id: Consumer group ID
            config: Additional consumer configuration
            timeout: Poll timeout in seconds
            max_messages: Maximum number of messages to consume (None for infinite)

        Yields:
            dict: Deserialized message with keys: 'topic', 'partition', 'offset', 'key', 'value', 'headers'
        """
        merged_config = {**self.default_config, **(config or {})}
        consumer = Kafka.get_consumer(
            self.bootstrap_servers, topics, group_id, merged_config
        )

        try:
            messages_consumed = 0
            while max_messages is None or messages_consumed < max_messages:
                msg = consumer.poll(timeout)

                if msg is None:
                    continue

                if msg.error():
                    logger.error(f"Consumer error: {msg.error()}")
                    continue

                try:
                    # Deserialize value
                    value = None
                    if msg.value() is not None:
                        value = self.value_serializer.deserialize(msg.value())

                    # Deserialize key
                    key = None
                    if msg.key() is not None:
                        key = self.key_serializer.deserialize(msg.key())

                    # Convert headers
                    headers = {}
                    if msg.headers():
                        headers = {
                            k: v.decode("utf-8") if isinstance(v, bytes) else v
                            for k, v in msg.headers()
                        }

                    yield {
                        "topic": msg.topic(),
                        "partition": msg.partition(),
                        "offset": msg.offset(),
                        "key": key,
                        "value": value,
                        "headers": headers,
                        "timestamp": msg.timestamp(),
                    }

                    messages_consumed += 1

                except Exception as e:
                    logger.error(f"Failed to deserialize message: {e}")
                    continue

        finally:
            consumer.close()

    def flush(self, timeout: float = 10.0) -> int:
        """Flush producer and return number of messages still in queue"""
        if self._producer:
            return self._producer.flush(timeout)
        return 0

    def close(self):
        """Close all clients and cleanup resources"""
        if self._producer:
            self._producer.flush(10)
            self._producer = None

        if self._admin_client:
            self._admin_client = None

        logger.info("Kafka client closed and cleaned up")
