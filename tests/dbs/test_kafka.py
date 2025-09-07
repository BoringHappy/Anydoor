"""
测试改进后的Kafka库功能
"""

import json
import time
from datetime import datetime
from unittest.mock import Mock, patch

import pytest

from anydoor.dbs.kafka import (
    AvroSerializer,
    BaseSerializer,
    BytesSerializer,
    JSONSerializer,
    Kafka,
    KafkaClient,
    KafkaConfig,
    PydanticAvroSerializer,
    StringSerializer,
)
from tests.decorators import require_env

# Test schemas for Avro serialization
USER_SCHEMA = {
    "type": "record",
    "name": "User",
    "fields": [
        {"name": "id", "type": "int"},
        {"name": "name", "type": "string"},
        {"name": "email", "type": "string"},
        {"name": "age", "type": ["null", "int"], "default": None},
        {"name": "active", "type": "boolean", "default": True},
    ],
}

ORDER_SCHEMA = {
    "type": "record",
    "name": "Order",
    "fields": [
        {"name": "order_id", "type": "string"},
        {"name": "user_id", "type": "int"},
        {"name": "amount", "type": "double"},
        {
            "name": "created_at",
            "type": {"type": "long", "logicalType": "timestamp-millis"},
        },
    ],
}


class TestKafkaConfig:
    """测试Kafka配置管理"""

    def test_default_producer_config(self):
        """测试默认Producer配置"""
        config = KafkaConfig.merge_producer_config("localhost:9092")

        assert config["bootstrap.servers"] == "localhost:9092"
        assert config["acks"] == "all"
        assert config["retries"] == 3
        assert config["compression.type"] == "snappy"
        assert config["enable.idempotence"] is True

    def test_default_consumer_config(self):
        """测试默认Consumer配置"""
        config = KafkaConfig.merge_consumer_config("localhost:9092")

        assert config["bootstrap.servers"] == "localhost:9092"
        assert config["auto.offset.reset"] == "earliest"
        assert config["enable.auto.commit"] is True
        assert config["session.timeout.ms"] == 30000

    def test_custom_config_merge(self):
        """测试自定义配置合并"""
        custom_config = {
            "compression.type": "gzip",
            "batch.size": 65536,
            "custom.setting": "value",
        }

        config = KafkaConfig.merge_producer_config("localhost:9092", custom_config)

        assert config["compression.type"] == "gzip"  # 自定义值覆盖默认值
        assert config["batch.size"] == 65536  # 自定义值覆盖默认值
        assert config["custom.setting"] == "value"  # 新的自定义设置
        assert config["acks"] == "all"  # 默认值保留


class TestSerializers:
    """测试序列化器"""

    def test_json_serializer(self):
        """测试JSON序列化器"""
        serializer = JSONSerializer()

        # 测试序列化
        data = {"key": "value", "number": 42}
        serialized = serializer.serialize(data)

        assert isinstance(serialized, bytes)

        # 测试反序列化
        deserialized = serializer.deserialize(serialized)
        assert deserialized == data

    def test_json_serializer_error_handling(self):
        """测试JSON序列化器错误处理"""
        serializer = JSONSerializer()

        # 测试序列化错误
        with pytest.raises(ValueError, match="Failed to serialize data to JSON"):
            serializer.serialize(lambda x: x)  # lambda无法JSON序列化

        # 测试反序列化错误
        with pytest.raises(ValueError, match="Failed to deserialize JSON data"):
            serializer.deserialize(b"invalid json")

    def test_string_serializer(self):
        """测试字符串序列化器"""
        serializer = StringSerializer()

        # 测试序列化
        data = "Hello, World!"
        serialized = serializer.serialize(data)

        assert isinstance(serialized, bytes)
        assert serialized == b"Hello, World!"

        # 测试反序列化
        deserialized = serializer.deserialize(serialized)
        assert deserialized == data

    def test_string_serializer_error_handling(self):
        """测试字符串序列化器错误处理"""
        serializer = StringSerializer()

        # 测试非字符串输入
        with pytest.raises(ValueError, match="Data must be a string"):
            serializer.serialize(123)

    def test_bytes_serializer(self):
        """测试字节序列化器"""
        serializer = BytesSerializer()

        # 测试序列化（直通）
        data = b"binary data"
        serialized = serializer.serialize(data)

        assert serialized is data  # 直通，同一对象

        # 测试反序列化（直通）
        deserialized = serializer.deserialize(serialized)
        assert deserialized is data

    def test_bytes_serializer_error_handling(self):
        """测试字节序列化器错误处理"""
        serializer = BytesSerializer()

        # 测试非字节输入
        with pytest.raises(ValueError, match="Data must be bytes"):
            serializer.serialize("string")


class TestKafkaValidation:
    """测试Kafka参数验证"""

    def test_producer_validation(self):
        """测试Producer参数验证"""
        # 测试空的bootstrap_servers
        with pytest.raises(
            ValueError, match="bootstrap_servers must be a non-empty string"
        ):
            Kafka.get_producer("", validate_connection=False)

        # 测试None的bootstrap_servers
        with pytest.raises(
            ValueError, match="bootstrap_servers must be a non-empty string"
        ):
            Kafka.get_producer(None, validate_connection=False)

    def test_consumer_validation(self):
        """测试Consumer参数验证"""
        # 测试空的bootstrap_servers
        with pytest.raises(
            ValueError, match="bootstrap_servers must be a non-empty string"
        ):
            Kafka.get_consumer("", ["topic"], "group", validate_connection=False)

        # 测试空的group_id
        with pytest.raises(ValueError, match="group_id must be a non-empty string"):
            Kafka.get_consumer(
                "localhost:9092", ["topic"], "", validate_connection=False
            )

        # 测试空的topics
        with pytest.raises(ValueError, match="topics must be provided"):
            Kafka.get_consumer("localhost:9092", [], "group", validate_connection=False)

        # 测试无效的topics
        with pytest.raises(ValueError, match="All topics must be non-empty strings"):
            Kafka.get_consumer(
                "localhost:9092", ["", "valid"], "group", validate_connection=False
            )

    def test_admin_client_validation(self):
        """测试AdminClient参数验证"""
        # 测试空的bootstrap_servers
        with pytest.raises(
            ValueError, match="bootstrap_servers must be a non-empty string"
        ):
            Kafka.get_admin_client("", validate_connection=False)


class TestKafkaClient:
    """测试高级Kafka客户端"""

    def test_kafka_client_initialization(self):
        """测试KafkaClient初始化"""
        client = KafkaClient(
            bootstrap_servers="localhost:9092",
            key_serializer=StringSerializer(),
            value_serializer=JSONSerializer(),
        )

        assert client.bootstrap_servers == "localhost:9092"
        assert isinstance(client.key_serializer, StringSerializer)
        assert isinstance(client.value_serializer, JSONSerializer)
        assert client.default_config == {}

    def test_kafka_client_default_serializers(self):
        """测试KafkaClient默认序列化器"""
        client = KafkaClient("localhost:9092")

        assert isinstance(client.key_serializer, StringSerializer)
        assert isinstance(client.value_serializer, JSONSerializer)

    @patch("anydoor.dbs.kafka.Kafka.get_producer")
    def test_kafka_client_produce(self, mock_get_producer):
        """测试KafkaClient消息生产"""
        # 模拟producer
        mock_producer = Mock()
        mock_get_producer.return_value = mock_producer

        client = KafkaClient("localhost:9092")

        # 测试消息生产
        test_data = {"message": "test"}
        client.produce(
            topic="test-topic",
            key="test-key",
            value=test_data,
            headers={"content-type": "application/json"},
        )

        # 验证producer.produce被调用
        mock_producer.produce.assert_called_once()
        call_args = mock_producer.produce.call_args

        assert call_args[1]["topic"] == "test-topic"
        assert call_args[1]["key"] == b"test-key"  # 序列化后的key
        assert (
            json.loads(call_args[1]["value"].decode()) == test_data
        )  # 序列化后的value

    def test_kafka_client_close(self):
        """测试KafkaClient资源清理"""
        client = KafkaClient("localhost:9092")

        # 模拟已创建的producer
        mock_producer = Mock()
        client._producer = mock_producer

        # 调用close
        client.close()

        # 验证producer被flush和清理
        mock_producer.flush.assert_called_once_with(10)
        assert client._producer is None


class TestAvroSerializers:
    """测试Avro序列化器"""

    def test_avro_serializer_with_schema(self):
        """测试带schema的Avro序列化器"""
        serializer = AvroSerializer(schema=USER_SCHEMA)

        # 测试数据
        user_data = {
            "id": 123,
            "name": "John Doe",
            "email": "john@example.com",
            "age": 30,
            "active": True,
        }

        # 测试序列化
        serialized = serializer.serialize(user_data)
        assert isinstance(serialized, bytes)
        assert len(serialized) > 0

        # 测试反序列化
        deserialized = serializer.deserialize(serialized)
        assert deserialized["id"] == user_data["id"]
        assert deserialized["name"] == user_data["name"]
        assert deserialized["email"] == user_data["email"]
        assert deserialized["age"] == user_data["age"]
        assert deserialized["active"] == user_data["active"]

    def test_avro_serializer_with_null_values(self):
        """测试Avro序列化器处理null值"""
        serializer = AvroSerializer(schema=USER_SCHEMA)

        # 测试包含null值的数据
        user_data = {
            "id": 456,
            "name": "Jane Doe",
            "email": "jane@example.com",
            "age": None,  # null值
            "active": False,
        }

        # 测试序列化和反序列化
        serialized = serializer.serialize(user_data)
        deserialized = serializer.deserialize(serialized)

        assert deserialized["age"] is None
        assert deserialized["active"] is False

    def test_avro_serializer_with_timestamp(self):
        """测试Avro序列化器处理时间戳"""
        serializer = AvroSerializer(schema=ORDER_SCHEMA)

        # 测试包含时间戳的数据
        timestamp = int(datetime.now().timestamp() * 1000)  # 毫秒时间戳
        order_data = {
            "order_id": "ORD-123",
            "user_id": 456,
            "amount": 99.99,
            "created_at": timestamp,
        }

        # 测试序列化和反序列化
        serialized = serializer.serialize(order_data)
        deserialized = serializer.deserialize(serialized)

        assert deserialized["order_id"] == order_data["order_id"]
        assert deserialized["user_id"] == order_data["user_id"]
        assert abs(deserialized["amount"] - order_data["amount"]) < 0.01

        # fastavro会将timestamp-millis逻辑类型转换为datetime对象
        # 验证时间戳是否正确（允许小的差异）
        if isinstance(deserialized["created_at"], datetime):
            # 转换回毫秒时间戳进行比较
            deserialized_timestamp = int(deserialized["created_at"].timestamp() * 1000)
            assert abs(deserialized_timestamp - timestamp) < 1000  # 允许1秒误差
        else:
            assert deserialized["created_at"] == timestamp

    def test_avro_serializer_error_handling(self):
        """测试Avro序列化器错误处理"""
        serializer = AvroSerializer(schema=USER_SCHEMA)

        # 测试缺少必填字段的数据
        invalid_data = {
            "id": 123,
            # 缺少 name 字段
            "email": "test@example.com",
        }

        with pytest.raises(ValueError, match="Failed to serialize data to Avro"):
            serializer.serialize(invalid_data)

        # 测试反序列化无效数据
        with pytest.raises(ValueError, match="Failed to deserialize Avro data"):
            serializer.deserialize(b"invalid avro data")

    def test_avro_serializer_initialization_errors(self):
        """测试Avro序列化器初始化错误"""
        # 测试没有提供schema或schema_registry_url
        with pytest.raises(
            ValueError, match="Either schema or schema_registry_url must be provided"
        ):
            AvroSerializer()

    def test_pydantic_avro_serializer(self):
        """测试PydanticAvroSerializer（使用pydantic-avro）"""
        from pydantic_avro import AvroBase

        class TestUser(AvroBase):
            id: int
            name: str
            email: str
            age: int = None
            active: bool = True

        serializer = PydanticAvroSerializer(TestUser)

        # 创建测试用户
        user = TestUser(id=789, name="Test User", email="test@example.com", age=25)

        # 测试序列化
        serialized = serializer.serialize(user)
        assert isinstance(serialized, bytes)

        # 测试反序列化
        deserialized = serializer.deserialize(serialized, TestUser)
        assert isinstance(deserialized, TestUser)
        assert deserialized.id == user.id
        assert deserialized.name == user.name
        assert deserialized.email == user.email
        assert deserialized.age == user.age

    def test_pydantic_avro_serializer_errors(self):
        """测试PydanticAvroSerializer错误处理"""
        serializer = PydanticAvroSerializer()

        # 测试序列化非pydantic-avro对象
        with pytest.raises(
            ValueError, match="Data must be a pydantic-avro model instance"
        ):
            serializer.serialize({"not": "a model"})

        # 测试没有提供model_class
        with pytest.raises(ValueError, match="model_class must be provided"):
            serializer.deserialize(b"some data")


class TestCustomSerializer:
    """测试自定义序列化器"""

    def test_custom_serializer_implementation(self):
        """测试自定义序列化器实现"""

        class TestSerializer(BaseSerializer):
            def serialize(self, data):
                return f"TEST:{data}".encode()

            def deserialize(self, data):
                return data.decode().replace("TEST:", "")

        serializer = TestSerializer()

        # 测试序列化
        result = serializer.serialize("hello")
        assert result == b"TEST:hello"

        # 测试反序列化
        original = serializer.deserialize(result)
        assert original == "hello"

    def test_base_serializer_abstract(self):
        """测试BaseSerializer是抽象类"""
        with pytest.raises(TypeError):
            BaseSerializer()  # 不能直接实例化抽象类


# Integration Tests (require actual Kafka instance)
class TestKafkaIntegration:
    """Kafka集成测试 - 需要实际的Kafka实例"""

    @classmethod
    def setup_class(cls):
        """设置测试类"""
        cls.bootstrap_servers = "localhost:9092"
        cls.test_topic = "test-integration-topic"
        cls.test_group = "test-integration-group"

        # 清理可能存在的测试数据
        try:
            admin_client = Kafka.get_admin_client(cls.bootstrap_servers)
            # 删除测试主题（如果存在）
            from confluent_kafka.admin import NewTopic

            admin_client.delete_topics([cls.test_topic], operation_timeout=10)
            time.sleep(2)  # 等待删除完成
        except Exception:
            pass  # 忽略删除错误

    @require_env(
        "UNITTEST_KAFKA_ENABLED",
        reason="Kafka integration tests are disabled. Set UNITTEST_KAFKA_ENABLED=true to enable.",
    )
    def test_producer_consumer_integration(self):
        """测试生产者和消费者集成"""
        # 创建生产者
        producer = Kafka.get_producer(self.bootstrap_servers)

        # 发送测试消息
        test_messages = [
            {"id": 1, "message": "Hello Kafka 1"},
            {"id": 2, "message": "Hello Kafka 2"},
            {"id": 3, "message": "Hello Kafka 3"},
        ]

        for msg in test_messages:
            producer.produce(
                self.test_topic,
                key=f"key-{msg['id']}",
                value=json.dumps(msg).encode("utf-8"),
            )

        # 确保消息发送完成
        producer.flush(10)

        # 创建消费者
        consumer = Kafka.get_consumer(
            self.bootstrap_servers,
            [self.test_topic],
            self.test_group,
            {"auto.offset.reset": "earliest"},
        )

        # 消费消息
        consumed_messages = []
        start_time = time.time()
        timeout = 30  # 30秒超时

        while (
            len(consumed_messages) < len(test_messages)
            and (time.time() - start_time) < timeout
        ):
            msg = consumer.poll(1.0)
            if msg is None:
                continue

            if msg.error():
                pytest.fail(f"Consumer error: {msg.error()}")

            # 解析消息
            key = msg.key().decode("utf-8") if msg.key() else None
            value = json.loads(msg.value().decode("utf-8")) if msg.value() else None

            consumed_messages.append({"key": key, "value": value})

        consumer.close()

        # 验证消息
        assert len(consumed_messages) == len(test_messages)

        # 验证消息内容
        consumed_values = [msg["value"] for msg in consumed_messages]
        for original_msg in test_messages:
            assert original_msg in consumed_values

    @require_env(
        "UNITTEST_KAFKA_ENABLED",
        reason="Kafka integration tests are disabled. Set UNITTEST_KAFKA_ENABLED=true to enable.",
    )
    def test_kafka_client_integration(self):
        """测试KafkaClient集成功能"""
        client = KafkaClient(
            bootstrap_servers=self.bootstrap_servers,
            key_serializer=StringSerializer(),
            value_serializer=JSONSerializer(),
        )

        # 测试数据
        test_data = [
            {
                "key": "user-1",
                "value": {"id": 1, "name": "张三", "email": "zhang@example.com"},
            },
            {
                "key": "user-2",
                "value": {"id": 2, "name": "李四", "email": "li@example.com"},
            },
            {
                "key": "user-3",
                "value": {"id": 3, "name": "王五", "email": "wang@example.com"},
            },
        ]

        # 使用专用topic避免与其他测试冲突
        test_topic = f"{self.test_topic}-client"

        # 生产消息
        for item in test_data:
            client.produce(
                topic=test_topic,
                key=item["key"],
                value=item["value"],
                headers={
                    "content-type": "application/json",
                    "source": "integration-test",
                },
            )

        # 刷新确保消息发送
        client.flush(10)

        # 消费消息
        consumed_data = []
        for msg in client.consume(
            topics=[test_topic],
            group_id=f"{self.test_group}-client",
            config={"auto.offset.reset": "earliest"},
            timeout=2.0,
            max_messages=len(test_data),
        ):
            consumed_data.append(
                {
                    "key": msg["key"],
                    "value": msg["value"],
                    "headers": msg["headers"],
                }
            )

        client.close()

        # 验证消息数量
        assert len(consumed_data) == len(test_data)

        # 验证消息内容
        for original_item in test_data:
            found = False
            for consumed_item in consumed_data:
                if (
                    consumed_item["key"] == original_item["key"]
                    and consumed_item["value"] == original_item["value"]
                ):
                    # 验证headers
                    assert (
                        consumed_item["headers"]["content-type"] == "application/json"
                    )
                    assert consumed_item["headers"]["source"] == "integration-test"
                    found = True
                    break
            assert found, f"Message not found: {original_item}"

    @require_env(
        "UNITTEST_KAFKA_ENABLED",
        reason="Kafka integration tests are disabled. Set UNITTEST_KAFKA_ENABLED=true to enable.",
    )
    def test_avro_serialization_integration(self):
        """测试Avro序列化集成"""
        # 使用用户schema进行测试
        avro_serializer = AvroSerializer(schema=USER_SCHEMA)

        client = KafkaClient(
            bootstrap_servers=self.bootstrap_servers,
            key_serializer=StringSerializer(),
            value_serializer=avro_serializer,
        )

        # 测试用户数据
        test_users = [
            {
                "id": 100,
                "name": "测试用户1",
                "email": "test1@example.com",
                "age": 25,
                "active": True,
            },
            {
                "id": 101,
                "name": "测试用户2",
                "email": "test2@example.com",
                "age": None,  # 测试null值
                "active": False,
            },
        ]

        # 使用专用topic避免与其他测试冲突
        test_topic = f"{self.test_topic}-avro"

        # 生产Avro消息
        for user in test_users:
            client.produce(
                topic=test_topic,
                key=f"user-{user['id']}",
                value=user,
            )

        client.flush(10)

        # 消费Avro消息
        consumed_users = []
        for msg in client.consume(
            topics=[test_topic],
            group_id=f"{self.test_group}-avro",
            config={"auto.offset.reset": "earliest"},
            timeout=2.0,
            max_messages=len(test_users),
        ):
            consumed_users.append(
                {
                    "key": msg["key"],
                    "value": msg["value"],
                }
            )

        client.close()

        # 验证Avro反序列化结果
        assert len(consumed_users) == len(test_users)

        for original_user in test_users:
            found = False
            for consumed in consumed_users:
                if consumed["key"] == f"user-{original_user['id']}":
                    consumed_user = consumed["value"]
                    assert consumed_user["id"] == original_user["id"]
                    assert consumed_user["name"] == original_user["name"]
                    assert consumed_user["email"] == original_user["email"]
                    assert consumed_user["age"] == original_user["age"]
                    assert consumed_user["active"] == original_user["active"]
                    found = True
                    break
            assert found, f"User not found: {original_user}"

    @require_env(
        "UNITTEST_KAFKA_ENABLED",
        reason="Kafka integration tests are disabled. Set UNITTEST_KAFKA_ENABLED=true to enable.",
    )
    def test_pydantic_avro_integration(self):
        """测试pydantic-avro集成"""
        try:
            from pydantic_avro import AvroBase
        except ImportError:
            pytest.skip("pydantic-avro not available")

        # 定义测试模型
        from typing import Optional

        class TestUser(AvroBase):
            id: int
            name: str
            email: str
            age: Optional[int] = None
            active: bool = True

        # 创建客户端
        client = KafkaClient(
            bootstrap_servers=self.bootstrap_servers,
            key_serializer=StringSerializer(),
            value_serializer=PydanticAvroSerializer(TestUser),
        )

        # 创建测试用户
        test_users = [
            TestUser(
                id=200, name="Pydantic用户1", email="pydantic1@example.com", age=30
            ),
            TestUser(
                id=201,
                name="Pydantic用户2",
                email="pydantic2@example.com",
                active=False,
            ),
        ]

        # 使用专用topic避免与其他测试冲突
        test_topic = f"{self.test_topic}-pydantic"

        # 生产消息
        for user in test_users:
            client.produce(
                topic=test_topic,
                key=f"pydantic-user-{user.id}",
                value=user,
            )

        client.flush(10)

        # 消费消息，使用相同的序列化器配置
        consumed_users = []

        # 创建新的客户端用于消费，使用相同的PydanticAvroSerializer配置
        consumer_client = KafkaClient(
            bootstrap_servers=self.bootstrap_servers,
            key_serializer=StringSerializer(),
            value_serializer=PydanticAvroSerializer(TestUser),  # 指定model_class
        )

        for msg in consumer_client.consume(
            topics=[test_topic],
            group_id=f"{self.test_group}-pydantic",
            config={"auto.offset.reset": "earliest"},
            timeout=2.0,
            max_messages=len(test_users),
        ):
            consumed_users.append(
                {
                    "key": msg["key"],
                    "value": msg["value"],  # 这里应该是TestUser实例
                }
            )

        consumer_client.close()
        client.close()

        # 验证数据
        assert len(consumed_users) == len(test_users)

        # 验证消费的用户数据
        for original_user in test_users:
            found = False
            for consumed in consumed_users:
                if consumed["key"] == f"pydantic-user-{original_user.id}":
                    consumed_user = consumed["value"]
                    # 验证这是一个TestUser实例
                    assert isinstance(consumed_user, TestUser)
                    assert consumed_user.id == original_user.id
                    assert consumed_user.name == original_user.name
                    assert consumed_user.email == original_user.email
                    assert consumed_user.age == original_user.age
                    assert consumed_user.active == original_user.active
                    found = True
                    break
            assert found, f"User not found: {original_user}"

    @require_env(
        "UNITTEST_KAFKA_ENABLED",
        reason="Kafka integration tests are disabled. Set UNITTEST_KAFKA_ENABLED=true to enable.",
    )
    def test_error_handling_integration(self):
        """测试错误处理集成"""
        # 测试无效的bootstrap servers
        with pytest.raises(Exception):  # KafkaException或其他连接异常
            Kafka.get_producer("invalid:9999", validate_connection=True)

        # 测试生产者超时
        producer = Kafka.get_producer(self.bootstrap_servers, validate_connection=False)

        # 测试无效主题名（包含无效字符）
        try:
            producer.produce("invalid/topic/name", value=b"test")
            producer.flush(1)  # 短超时来快速失败
        except Exception:
            pass  # 预期会有异常

        # 测试消费者错误处理
        consumer = Kafka.get_consumer(
            self.bootstrap_servers,
            ["non-existent-topic"],
            "error-test-group",
            validate_connection=False,
        )

        # 尝试消费不存在的主题（应该不会立即失败，但也不会收到消息）
        msg = consumer.poll(1.0)
        assert msg is None or msg.error()  # 要么没消息，要么有错误

        consumer.close()


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
