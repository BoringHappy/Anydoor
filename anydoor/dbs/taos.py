# -*- coding:utf-8 -*-
"""
filename : taos.py
create_time : 2024/11/21 11:59
author : Demon Finch
"""

from sqlalchemy import Engine, create_engine
from .base import BaseDB
import pandas as pd
from typing import List
import taosws


class TaosDB(BaseDB):
    DB_TYPE = "tdengine"
    default_schema = "information_schema"
    secret_name = "tdengine"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.schema = self.database

    @classmethod
    def create_engine(cls, secret, **kwargs) -> Engine:
        engine_url = f"{secret.protocol}://{secret.user}:{secret.password}@{secret.host}:{secret.port}"
        print(engine_url)
        engine = taosws.connect(engine_url)
        return engine

    def execute(self, sql):
        self.engine.execute(sql)

    def get_table_cols(self, schema, table):
        """get table columns"""
        return self.execute(f"select * from {schema}.{table} limit 0").columns

    @staticmethod
    def _clean_child_table_name(table_name):
        return table_name.replace(".", "_").lower()

    def batch_insert(self, df: pd.DataFrame, insert_sql: str, chunksize: int = 1000):
        with self.engine.begin() as conn:
            for step in range(0, len(df) + 1, chunksize):
                step_df = df.iloc[step : (step + chunksize)]
                if isinstance(step_df, pd.Series):
                    step_df = pd.DataFrame(step_df).T
                if len(step_df) > 0:
                    sql_query = str(step_df.apply(tuple, axis=1).to_list())[1:-1]
                    conn.execute(insert_sql + sql_query)

    def assert_columns_match(self, _df, schema, table):
        cols = self.get_table_cols(schema, table)
        assert (
            set(cols) == set(_df.columns),
            f"Columns not match {set(cols) - set(_df.columns)} | {set(cols) - set(_df.columns)}",
        )

    def to_sql(
        self,
        df: pd.DataFrame,
        table: str,
        schema: str = None,
        chunksize: int = 1000,
        tag_cols: List[str] = None,
    ):
        assert isinstance(df, pd.DataFrame)
        table = table.lower()
        schema = schema or self.default_schema

        low_cols = {col: col.lower() for col in df.columns}
        _df = df.rename(columns=low_cols)
        self.assert_columns_match(_df=_df, schema=schema, table=table)

        if tag_cols:
            assert isinstance(tag_cols, list)
            tag_cols = sorted(tag_cols)
            _df = _df.sort_values(tag_cols).set_index(tag_cols)

            for idx in set(_df.index):
                idx_items = (
                    [str(idx)]
                    if (isinstance(idx, str) or len(idx)) == 1
                    else [str(a) for a in idx]
                )
                child_table = self._clean_child_table_name(
                    f"_{table}__" + "_".join(idx_items)
                )
                child_table_tags = str(tuple(idx_items)).replace(",)", ")")

                insert_sql = f"INSERT INTO {schema}.{child_table} USING {schema}.{table} TAGS {child_table_tags} {tuple(_df.columns)} VALUES "
                self.batch_insert(_df.loc[idx], insert_sql, chunksize)

        else:
            insert_sql = (
                f"""INSERT INTO {schema}.{table} {tuple(_df.columns)} VALUES """
            )
            self.batch_insert(_df, insert_sql, chunksize)

    def ensure_database(
        self, database: str, duration=1, wal_level=2, extra_params=None
    ):
        """create tdengine database"""
        sql = (
            f"CREATE DATABASE IF NOT EXISTS {database}\n"
            f"KEEP 365000 DURATION {duration} WAL_LEVEL {wal_level} "
        )
        if extra_params:
            sql += extra_params
        self.execute(sql)

    def ensure_table(self, sql: str):
        self.execute(sql)

    def ensure_primary_key(self, *args, **kwargs):
        raise AttributeError(f"No primary key change allowed in {self.DB_TYPE}")

    def change_column(self, *args, **kwargs):
        raise AttributeError(f"No column change allowed in {self.DB_TYPE}")

    def truncate(self, *args, **kwargs):
        raise AttributeError(f"No truncate allowed in {self.DB_TYPE}")

    def create_index(self, *args, **kwargs):
        raise AttributeError(f"No create_index allowed in {self.DB_TYPE}")
