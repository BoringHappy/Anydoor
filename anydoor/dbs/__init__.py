from .clickhouse import Clickhouse
from .postgres import Postgres
from .spark import init_spark
from .sql_runner import SQLRunner
from .sqlite import Sqlite

# Optional: Kafka ecosystem (requires 'kafka' extra)
try:
    from .kafka import (
        AvroSerializer,
        BaseSerializer,
        BytesSerializer,
        JSONSerializer,
        Kafka,
        KafkaClient,
        PydanticAvroSerializer,
        StringSerializer,
    )

    _has_kafka = True
except ImportError:
    _has_kafka = False

# Optional: Delta Lake (requires 'polars' extra which includes deltalake)
try:
    from .delta import Delta

    _has_delta = True
except ImportError:
    _has_delta = False

# Build __all__ dynamically based on available dependencies
__all__ = [
    "Postgres",
    "Clickhouse",
    "Sqlite",
    "SQLRunner",
    "init_spark",
]

if _has_kafka:
    __all__.extend(
        [
            "Kafka",
            "KafkaClient",
            "BaseSerializer",
            "JSONSerializer",
            "StringSerializer",
            "BytesSerializer",
            "AvroSerializer",
            "PydanticAvroSerializer",
        ]
    )

if _has_delta:
    __all__.append("Delta")
