# -*- coding:utf-8 -*-
"""
filename : clickhouse.py
create_time : 2021/12/29 19:30
author : Demon Finch
"""

from sqlalchemy import MetaData, create_engine, Table, Engine
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.sql import text
from functools import partial
from .base import BaseDB
from types import SimpleNamespace
import pandas as pd
from anydoor.utils import logger


class Clickhouse(BaseDB):
    DB_TYPE = "clickhouse"
    default_schema = "default"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.schema = self.database

    @classmethod
    def create_engine(
        cls, secret: SimpleNamespace, database, schema, *args, **kwargs
    ) -> Engine:
        engine = create_engine(
            f"clickhouse+native://{secret.user}:{secret.password}@{secret.host}:{secret.port}/{database}",
            **kwargs,
        )
        return engine

    def ensure_table(
        self,
        table: str,
        schema: str,
        dtype: dict,
        primary_keys: list = None,
        partition_keys: list = None,
        ck_engine: str = None,
    ):
        if not self.is_table_exists(schema=schema, table=table):
            sql = f"""
                CREATE TABLE IF NOT EXISTS {schema or self.schema}.{table} 
                ({', '.join([f'`{k}` {v}' for k, v in dtype.items()])} ) 
                ENGINE = {ck_engine or "MergeTree"}() 
                """
            if partition_keys:
                sql += f' PARTITION BY ({",".join(partition_keys)}) '
            sql += f""" PRIMARY KEY ({",".join(primary_keys)}) ORDER BY ({",".join(primary_keys)}) """
            logger.info(f"create Clickhouse Table: {sql}")
            self.execute(sql)

    def to_sql(
        self,
        df: pd.DataFrame,
        table: str,
        schema: str = None,
    ):
        table = table.lower()
        schema = schema or self.schema

        to_sql_parameters = {
            "name": table,
            "schema": schema,
            "con": self.engine,
            "index": False,
            "if_exists": "append",
            "chunksize": 1000,
        }
        df.to_sql(**to_sql_parameters)

    def ensure_primary_key(self, *args, **kwargs): ...
    def check_varchar_length(self, *args, **kwargs): ...
    def change_column(self, *args, **kwargs): ...
