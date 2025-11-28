# -*- coding:utf-8 -*-
"""
ClickHouse database connection and operations
"""

import clickhouse_connect
import pandas as pd
from clickhouse_connect.driver.client import Client
from sqlalchemy import Engine, create_engine

from ..utils import logger
from ..utils.vault import Secret
from .base import BaseDB


class Clickhouse(BaseDB):
    """
    ClickHouse database implementation optimized for analytics workloads.

    ClickHouse is a columnar database designed for OLAP (Online Analytical Processing)
    with excellent performance for time-series and analytical queries.

    Features:
    - Partition support for large datasets
    - MergeTree engine configuration
    - Optimized for time-series data
    - Immutable schema (no modifications after creation)

    Attributes:
        DB_TYPE (str): Set to "clickhouse"
        default_schema (str): Set to "default"
        default_secret_name (str): Set to "clickhouse"
    """

    DB_TYPE = "clickhouse"
    default_schema = "default"
    default_secret_name = "clickhouse"

    def __init__(self, secret: Secret = None, secret_name: str = None, *args, **kwargs):
        """
        Initialize ClickHouse connection.

        Note:
            ClickHouse uses database name as schema, so schema is set to database name.
        """
        # Store secret for clickhouse-connect client
        super().__init__(*args, **kwargs)
        self.schema = self.database
        # Create clickhouse-connect client for native operations
        self._client: Client = None

    @property
    def client(self) -> Client:
        """
        Get or create clickhouse-connect client for native operations.

        Returns:
            Client: ClickHouse native client
        """
        if self._client is None:
            self._client = clickhouse_connect.get_client(
                host=self.secret.host,
                port=self.secret.connect_port,
                username=self.secret.user,
                password=self.secret.password,
                database=self.database,
            )
        return self._client

    @classmethod
    def create_engine(cls, secret: Secret, database, schema, *args, **kwargs) -> Engine:
        """
        Create ClickHouse SQLAlchemy engine.

        Args:
            secret (Secret): Database credentials containing host, port, user, password
            database (str): Database name
            schema (str): Schema name (unused in ClickHouse)
            *args: Additional positional arguments
            **kwargs: Additional engine options

        Returns:
            Engine: Configured SQLAlchemy engine for ClickHouse
        """
        engine = create_engine(
            f"clickhouse+native://{secret.user}:{secret.password}@{secret.host}:{secret.port}/{database}",
            **kwargs,
        )
        return engine

    def execute(self, sql: str, return_pandas=True) -> pd.DataFrame | None:
        if return_pandas and sql.lower().strip().startswith("select"):
            return self.client.query_df(sql)
        else:
            self.client.command(sql)

    def to_sql(
        self,
        df: pd.DataFrame,
        table: str,
        schema: str = None,
    ):
        """
        Store DataFrame in ClickHouse using clickhouse-connect's native insert.

        ClickHouse doesn't support complex schema modifications, so this method
        provides a streamlined data insertion process using the native protocol
        for better performance.

        Args:
            df (pd.DataFrame): Data to store
            table (str): Target table name (converted to lowercase)
            schema (str, optional): Schema name (uses database name if not provided)
        """
        table = table.lower()
        schema = schema or self.schema

        # Use clickhouse-connect's insert_df for native protocol performance
        self.client.insert_df(
            table=table,
            df=df,
            database=schema,
        )

    def ensure_table(
        self,
        table: str,
        schema: str,
        dtype: dict,
        primary_keys: list = None,
        partition_keys: list = None,
        ck_engine: str = None,
    ):
        """
        Create ClickHouse table with partitioning support.

        Creates a ClickHouse table using the MergeTree engine with optional partitioning.
        This is the recommended approach for ClickHouse table creation.

        Args:
            table (str): Table name
            schema (str): Schema name
            dtype (dict): Column type definitions
            primary_keys (list, optional): Primary key columns
            partition_keys (list, optional): Partition key columns for data distribution
            ck_engine (str, optional): ClickHouse engine type (defaults to MergeTree)
        """
        if not self.is_table_exists(schema=schema, table=table):
            sql = f"""
                CREATE TABLE IF NOT EXISTS {schema or self.schema}.{table} 
                ({", ".join([f"`{k}` {v}" for k, v in dtype.items()])} ) 
                ENGINE = {ck_engine or "MergeTree"}() 
                """
            if partition_keys:
                sql += f" PARTITION BY ({','.join(partition_keys)}) "
            sql += f""" PRIMARY KEY ({",".join(primary_keys)}) ORDER BY ({",".join(primary_keys)}) """
            logger.info(f"create Clickhouse Table: {sql}")
            self.execute(sql)

    @classmethod
    def get_df_dtypes(cls, df: pd.DataFrame) -> dict:
        """
        Get ClickHouse-compatible data types for DataFrame columns.

        Args:
            df (pd.DataFrame): DataFrame to analyze

        Returns:
            dict: Column name to ClickHouse type mapping
        """
        return {k: cls.get_dtype(v) for k, v in df.dtypes.to_dict().items()}

    @classmethod
    def get_dtype(cls, dtype: str) -> str:
        """
        Convert pandas dtype to ClickHouse type.

        Maps pandas data types to their ClickHouse equivalents:
        - object -> String
        - int* -> Int*
        - uint* -> UInt*
        - float* -> Float*
        - bool -> UInt8
        - datetime* -> DateTime
        - timedelta -> Int64
        - category -> String

        Args:
            dtype (str): Pandas dtype string

        Returns:
            str: ClickHouse type string

        Raises:
            ValueError: If dtype is not supported
        """
        dtype = str(dtype)
        if dtype == "object":
            return "String"
        elif dtype.startswith("int"):
            return f"Int{dtype[3:]}"
        elif dtype.startswith("uint"):
            return f"UInt{dtype[4:]}"
        elif dtype.startswith("float"):
            return f"Float{dtype[5:]}"
        elif dtype == "bool":
            return "UInt8"
        elif dtype.startswith("datetime"):
            return "DateTime"
        elif dtype == "timedelta":
            return "Int64"
        elif dtype == "category":
            return "String"
        else:
            raise ValueError(f"Invalid dtype '{dtype}'")

    def ensure_primary_key(self, *args, **kwargs):
        raise AttributeError("No primary key change allowed in clickhouse")

    def check_varchar_length(self, *args, **kwargs):
        raise AttributeError("No varchar length check allowed in clickhouse")

    def change_column(self, *args, **kwargs):
        raise AttributeError("No column change allowed in clickhouse")

    def truncate(self, table: str, schema: str):
        self.client.command(f"TRUNCATE TABLE {schema}.{table}")
