from datetime import datetime
from typing import List, Optional
from uuid import uuid1

import pandas as pd
from loguru import logger
from sqlalchemy import Column, Engine, Index, MetaData, Table, inspect
from sqlalchemy.exc import IntegrityError, ProgrammingError
from sqlalchemy.sql import text
from sqlalchemy.types import BIGINT, TEXT, Date, DateTime, Float, String

from ..utils.vault import Secret, Vault


class BaseDB:
    """
    Base database class providing common functionality for all database implementations.

    This class serves as the foundation for database operations, providing:
    - Automatic schema management and table creation
    - Type mapping between pandas DataFrames and database types
    - Conflict resolution strategies for data insertion
    - Audit trail functionality with automatic ID and timestamp columns
    - Connection management through SQLAlchemy engines

    Attributes:
        DB_TYPE (str): Database type identifier (e.g., 'postgres', 'clickhouse')
        default_schema (str): Default schema name for the database
        on_conflicts (list): Available conflict resolution strategies
        default_secret_name (str): Default secret name in Vault for credentials
    """

    DB_TYPE = None
    default_schema = None
    on_conflicts = ["replace", "ignore"]
    default_secret_name = None

    def __init__(
        self,
        database: str,
        secret: Secret = None,
        secret_name: str = None,
        schema: str = None,
        engine: Engine = None,
        create_engine_options: dict = None,
    ):
        """
        Initialize database connection.

        Args:
            database (str): Name of the database to connect to
            secret (Secret, optional): Vault secret object containing credentials
            secret_name (str, optional): Name of the secret in Vault to retrieve
            schema (str, optional): Database schema name (uses default_schema if not provided)
            engine (Engine, optional): Existing SQLAlchemy engine to reuse
            create_engine_options (dict, optional): Additional options for engine creation

        Note:
            Either 'secret' or 'secret_name' must be provided unless 'engine' is specified.
            The secret should contain: host, port, user, password
        """
        self.database = database
        self.schema = schema or self.default_schema
        self.secret = secret or Vault().get(secret_name or self.default_secret_name)

        if isinstance(engine, Engine):
            self.engine = engine
        else:
            create_engine_options = create_engine_options or dict()

            self.engine = self.create_engine(
                secret=self.secret,
                database=self.database,
                schema=self.schema,
                **create_engine_options,
            )

    @classmethod
    def create_engine(
        self, secret: Secret, database, schema, *args, **kwargs
    ) -> Engine: ...

    @classmethod
    def add_audit(cls, df: pd.DataFrame) -> pd.DataFrame:
        """
        Add audit trail columns to DataFrame.

        Adds '_id' (UUID) and '_update_time' (current timestamp) columns to track
        data lineage and modification times.

        Args:
            df (pd.DataFrame): Input DataFrame

        Returns:
            pd.DataFrame: DataFrame with added audit columns
        """
        df["_id"] = df.iloc[:, 0].apply(lambda x: str(uuid1()))
        df["_update_time"] = datetime.now()
        return df

    @classmethod
    def mapping_df_types(cls, df: pd.DataFrame, dtype: dict = None) -> dict:
        """
        Map pandas DataFrame column types to SQLAlchemy types.

        Automatically converts pandas dtypes to appropriate SQLAlchemy types:
        - float types -> Float(53)
        - int types -> BIGINT()
        - datetime types -> DateTime()
        - date types -> Date()
        - string types -> String with calculated length

        Args:
            df (pd.DataFrame): DataFrame to analyze
            dtype (dict, optional): Predefined type mappings

        Returns:
            dict: Column name to SQLAlchemy type mapping
        """
        dtype = dtype or dict()
        for col, col_type in zip(df.columns, df.dtypes):
            if col not in dtype.keys():
                if "float" in str(col_type):
                    dtype[col] = Float(53)
                elif "int" in str(col_type):
                    dtype[col] = BIGINT()
                elif col == "create_time" or "datetime" in str(col_type):
                    dtype[col] = DateTime()
                elif "date" in str(col_type) and "time" not in str(col_type):
                    dtype[col] = Date()
                else:
                    dtype[col] = String(length=df[col].apply(str).apply(len).max() + 10)
        return dtype

    def execute(self, sql: str, return_pandas=True) -> Optional[pd.DataFrame]:
        """
        Execute SQL query against the database.

        Args:
            sql (str): SQL query string
            return_pandas (bool): If True, return pandas DataFrame for SELECT queries

        Returns:
            Optional[pd.DataFrame]: Query results as DataFrame, or None for non-SELECT queries

        Raises:
            ProgrammingError: If query fails due to syntax or permission issues
        """
        if isinstance(sql, str):
            sql = text(sql)
        if "select" in str(sql).lower():
            if return_pandas:
                return pd.read_sql(sql, con=self.engine)
            else:
                with self.engine.connect() as conn:
                    return conn.execute(sql)
        else:
            with self.engine.connect() as conn:
                conn.execute(sql)
                conn.commit()

    def fetch_one(self, sql: str, default=None) -> dict:
        """
        Fetch a single row from database query.

        Args:
            sql (str): SQL query string
            default: Default value to return if no results or table doesn't exist

        Returns:
            dict: First row as dictionary, or default value if no results

        Note:
            Returns default value if table doesn't exist (handles ProgrammingError gracefully)
        """
        try:
            return self.execute(sql).iloc[0].to_dict()
        except ProgrammingError as e:
            if "relation" in str(e) and "does not exist" in str(e):
                logger.warning(f"Table does not exist for query: {sql}")
                return default
            else:
                raise e

    def ensure_table(
        self,
        table: str,
        schema: str,
        dtype: dict,
        primary_keys: list = None,
    ):
        if not self.is_table_exists(schema=schema, table=table):
            pd.DataFrame(columns=dtype.keys()).to_sql(
                table.lower(),
                schema=schema,
                con=self.engine,
                index=False,
                if_exists="append",
                chunksize=1000,
                dtype=dtype,
            )
            logger.info(f"Created: {schema}.{table} ")

        if primary_keys:
            self.ensure_primary_key(
                table=table, schema=schema, primary_keys=primary_keys
            )

    def truncate(self, table: str, schema: str):
        if self.is_table_exists(schema=schema, table=table):
            self.execute(f"truncate table {schema}.{table}")
            logger.info(f"{schema}.{table} truncated")

    def is_table_exists(self, table: str, schema: str = None) -> bool:
        with self.engine.connect() as conn:
            with conn.begin():
                return inspect(conn).has_table(table, schema=schema or self.schema)

    def check_columns(self, df: pd.DataFrame, schema: str, table: str):
        if self.is_table_exists(schema=schema, table=table):
            schema_df = pd.read_sql(
                f"select * from {schema}.{table} limit 1", con=self.engine
            )
            for col, col_type in zip(df.columns, df.dtypes):
                if col not in schema_df.columns:
                    if col.lower() == "create_time" or "datetime" in str(col_type):
                        col_sql_type = DateTime()
                    elif "float" in str(col_type):
                        col_sql_type = Float(53)
                    elif "int" in str(col_type):
                        col_sql_type = BIGINT()
                    else:
                        col_sql_type = String(50)

                    new_column = Column(col, col_sql_type)
                    self.change_column(
                        new_column, schema=schema, table=table, action="ADD"
                    )

    def get_table(self, table: str, schema: str = None):
        return Table(
            table,
            MetaData(schema=schema or self.schema),
            autoload_with=self.engine,
        )

    def ensure_primary_key(self, table: str, schema: str, primary_keys: List[str]):
        if primary_keys:
            constraint = self.get_table(table=table, schema=schema).primary_key
            if not constraint:
                sql = f"""ALTER TABLE "{schema}"."{table}" ADD PRIMARY KEY ("{'","'.join(primary_keys)}")"""
                logger.info(f"[PRIMARY KEY Change]sql：{sql}")
                self.execute(sql)

    def create_index(self, schema, table, name, field):
        mytable = self.get_table(table=table, schema=schema)
        return Index(name, mytable.c.get(field))

    def ensure_index(self, _index: Index):
        if _index:
            indexes = _index.table.indexes
            if _index.name not in [i.name for i in indexes]:
                _index.create(self.engine)

    def check_varchar_length(self, df: pd.DataFrame, schema: str, table: str):
        sql_table = self.get_table(table=table, schema=schema)
        for col in sql_table.columns:
            if col.name not in df.columns or isinstance(col.type, TEXT):
                continue
            elif isinstance(col.type, String):
                df_col_length = df[col.name].apply(str).apply(len).max()
                if df_col_length > (col.type.length or 0):
                    if df_col_length > 2000:
                        col_type = TEXT()
                    else:
                        col_type = String(df_col_length + 10)

                    new_column = Column(col.name, col_type)
                    self.change_column(
                        new_column, schema=schema, table=table, action="ALTER"
                    )
            else:
                ...

    def change_column(
        self, column: Column, schema: str, table: str, action: str = "ALTER"
    ):
        if action not in ["ALTER", "ADD"]:
            raise ValueError('action should be in ["ALTER","ADD"]')
        column_name = column.compile(dialect=self.engine.dialect)
        column_type = column.type.compile(self.engine.dialect)
        alter_sql = (
            f"ALTER TABLE {schema}.{table} {action} COLUMN {column_name} "
            f"{'TYPE' if action == 'ALTER' else ''} {column_type}"
        )
        logger.info(f"[{action} column]: {alter_sql}")
        self.execute(alter_sql)

    def get_conflict_func(self, on_conflict): ...

    def to_sql(
        self,
        df: pd.DataFrame,
        table: str,
        schema: str = None,
        dtype: dict = None,
        primary_keys: List[str] = None,
        on_conflict: str = "replace",
    ):
        """
        Store DataFrame in database with automatic schema management.

        This method handles the complete data storage workflow:
        1. Ensures table exists with proper schema
        2. Checks and adds missing columns
        3. Validates and adjusts column lengths
        4. Handles conflicts during insertion

        Args:
            df (pd.DataFrame): Data to store
            table (str): Target table name (converted to lowercase)
            schema (str, optional): Schema name (uses default_schema if not provided)
            dtype (dict, optional): Column type mappings
            primary_keys (List[str], optional): Primary key columns
            on_conflict (str): Conflict resolution strategy ("replace" or "ignore")

        Raises:
            IntegrityError: If conflict resolution fails
            Exception: For other database errors
        """
        table = table.lower()
        dtypes = self.mapping_df_types(df, dtype)
        schema = schema or self.default_schema

        self.ensure_table(
            table=table, schema=schema, dtype=dtypes, primary_keys=primary_keys
        )
        self.check_columns(df=df, schema=schema, table=table)
        self.check_varchar_length(df=df, schema=schema, table=table)

        to_sql_parameters = {
            "name": table,
            "schema": schema,
            "con": self.engine,
            "index": False,
            "if_exists": "append",
            "chunksize": 1000,
            "dtype": dtypes,
        }

        try:
            df.to_sql(**to_sql_parameters)
        except IntegrityError as err:
            on_conflict_func = self.get_conflict_func(on_conflict)
            if on_conflict_func:
                df.to_sql(**to_sql_parameters, method=on_conflict_func)
            else:
                raise err
        except Exception as err:
            raise err

    def insert(
        self,
        table: str,
        values: List[dict],
        schema: str = None,
    ):
        self.get_table(table=table, schema=schema).insert().values(values)

    def create(
        self,
        table: Table,
    ):
        table.__table__.create(self.engine, checkfirst=True)
