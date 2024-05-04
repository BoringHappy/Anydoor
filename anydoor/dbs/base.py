import pandas as pd
from datetime import datetime
from uuid import uuid1
from anydoor.utils import Secret
from sqlalchemy.types import DateTime, Float, String, Date, BIGINT, TEXT

from sqlalchemy.sql import text
from typing import List, Optional, Union
from types import SimpleNamespace
from sqlalchemy import Engine, inspect, Column, MetaData, Table
from sqlalchemy.exc import IntegrityError


class BaseDB:
    DB_TYPE = None
    default_schema = None
    on_conflicts = ["replace", "ignore"]

    def __init__(
        self,
        database: str,
        secret: SimpleNamespace = None,
        secret_name: str = None,
        schema: str = None,
        engine: Engine = None,
        create_engine_options=dict(),
    ):
        self.database = database
        self.schema = schema or self.default_schema
        self.secret = secret or Secret.get(secret_name)
        self.engine = engine or self.create_engine(
            secret=self.secret,
            database=self.database,
            schema=self.schema,
            **create_engine_options,
        )

    @classmethod
    def create_engine(
        self, secret: SimpleNamespace, database, schema, *args, **kwargs
    ) -> Engine: ...

    @classmethod
    def add_audit(cls, df: pd.DataFrame) -> pd.DataFrame:
        df["_id"] = df.iloc[:, 0].apply(lambda x: str(uuid1()))
        df["_update_time"] = datetime.now()
        return df

    @classmethod
    def mapping_df_types(cls, df: pd.DataFrame, dtype: dict = None) -> dict:
        """pandas to_sql type convert"""
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

    def execute(self, sql: str) -> Optional[pd.DataFrame]:
        if "select" in sql.lower():
            return pd.read_sql(text(sql), con=self.engine)
        else:
            with self.engine.connect() as conn:
                conn.execute(text(sql))
                conn.commit()

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
            print(f"Created: {schema}.{table} ")

        if primary_keys:
            self.ensure_primary_key(
                table=table, schema=schema, primary_keys=primary_keys
            )

    def truncate(self, table: str, schema: str):
        if self.is_table_exists(schema=schema, table=table):
            self.execute(f"truncate table {schema}.{table}")
            print(f"{schema}.{table} truncated")

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
                        col_sql_type = Float(64)
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
                print(f"[PRIMARY KEY Change]sql：{sql}")
                self.execute(sql)

    def check_varchar_length(self, df: pd.DataFrame, schema: str, table: str):
        sql_table = self.get_table(table=table, schema=schema)
        for col in sql_table.columns:
            if isinstance(col.type, String):
                df_col_length = df[col.name].apply(str).apply(len).max()
                if df_col_length > col.type.length:
                    if df_col_length > 2000:
                        col_type = TEXT()
                    else:
                        col_type = String(df_col_length + 10)

                    new_column = Column(col.name, col_type)
                    self.change_column(
                        new_column, schema=schema, table=table, action="ALTER"
                    )

    def change_column(
        self, column: Column, schema: str, table: str, action: str = "ALTER"
    ):
        if action not in ["ALTER", "ADD"]:
            raise ValueError(f'action should be in ["ALTER","ADD"]')
        column_name = column.compile(dialect=self.engine.dialect)
        column_type = column.type.compile(self.engine.dialect)
        alter_sql = (
            f"ALTER TABLE {schema}.{table} {action} COLUMN {column_name} "
            f'{"TYPE" if action=="ALTER" else ""} {column_type}'
        )
        print(f"[{action} column]: {alter_sql}")
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
