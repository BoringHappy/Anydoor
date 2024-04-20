# -*- coding:utf-8 -*-
"""
filename : database.py
create_time : 2021/12/29 19:30
author : Demon Finch
"""
from typing import List, Optional

import pandas as pd
from sqlalchemy import MetaData, create_engine, Table, Engine
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.exc import IntegrityError
from sqlalchemy.sql import text
from functools import partial
from .base import BaseDB
from types import SimpleNamespace


def pgsql_upsert(table, conn, keys, data_iter, on_conflict):
    constraint_name = conn.execute(
        text(
            "select constraint_name from information_schema.table_constraints "
            f"where constraint_type='PRIMARY KEY' AND TABLE_NAME = '{table.name}' "
            f"AND TABLE_SCHEMA= '{table.schema}' "
        )
    ).fetchall()[0][0]
    insert_table = Table(
        f"{table.name}", MetaData(schema=table.schema), autoload_with=conn
    )
    for data in data_iter:
        data = {k: data[i] for i, k in enumerate(keys)}
        insert_stmt = insert(insert_table).values(**data)
        if on_conflict == "replace":
            upsert_stmt = insert_stmt.on_conflict_do_update(
                constraint=constraint_name, set_=data
            )
        elif on_conflict == "ignore":
            upsert_stmt = insert_stmt.on_conflict_do_nothing(constraint=constraint_name)
        else:
            ...
        conn.execute(upsert_stmt)


def on_conflict_do(on_conflict: str):
    if on_conflict in ("replace", "ignore"):
        return partial(pgsql_upsert, on_conflict=on_conflict)
    else:
        return None


class Postgres(BaseDB):
    default_schema = "public"

    on_conflicts = ["replace", "ignore"]

    @classmethod
    def create_engine(
        cls, secret: SimpleNamespace, database, schema, **kwargs
    ) -> Engine:
        """postgresql sqlalchemy engine"""
        if schema:
            kwargs["connect_args"] = {"options": f"-csearch_path={schema}"}
        engine = create_engine(
            f"postgresql://{secret.user}:{secret.password}@{secret.host}:{secret.port}/{database}",
            client_encoding="utf8",
            **kwargs,
        )
        return engine

    def to_sql(
        self,
        df: pd.DataFrame,
        table: str,
        schema: str = None,
        dtype: dict = None,
        on_conflict: str = "replace",
        primary_keys: List[str] = None,
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
            if on_conflict in self.on_conflicts:
                to_sql_parameters["method"] = on_conflict_do(on_conflict)
                df.to_sql(**to_sql_parameters)
            else:
                raise ValueError(
                    f"on_conflict must be in {self.on_conflicts},current: {on_conflict}"
                )
        except Exception as err:
            raise err

    def check_varchar_length(self, df: pd.DataFrame, schema: str, table: str):
        length_sql = f"""SELECT column_name,character_maximum_length,udt_name 
        FROM information_schema.columns where character_maximum_length is not null 
        and table_schema = '{schema}' and table_name = '{table}' """
        length_df = self.execute(length_sql)
        for _, row in length_df.iterrows():
            if row.column_name in df.columns:
                df_col_length = df[row.column_name].apply(str).apply(len).max()
                if df_col_length > row.character_maximum_length:
                    if df_col_length > 2000:
                        col_type = "text"
                    else:
                        col_type = f"varchar({df_col_length + 10})"
                    alter_sql = f"""ALTER TABLE {schema}.{table} ALTER COLUMN "{row.column_name}" 
                                TYPE {col_type}"""
                    print(f"Change column query：{alter_sql}")
                    self.execute(alter_sql)

    def check_columns(self, df: pd.DataFrame, schema: str, table: str):
        if self.is_table_exists(schema=schema, table=table):
            schema_df = pd.read_sql(
                f"select * from {schema}.{table} limit 1", con=self.engine
            )
            for col, col_type in zip(df.columns, df.dtypes):
                if col not in schema_df.columns:
                    col_sql_type = "varchar(50)"
                    if col == "create_time":
                        col_sql_type = "timestamp(20)"
                    elif "float" in str(col_type):
                        col_sql_type = "float8"
                    elif "int" in str(col_type):
                        col_sql_type = "int8"
                    elif "datetime" in str(col_type):
                        col_sql_type = "timestamp(20)"

                    add_col_sql = f"""ALTER TABLE {schema}.{table} ADD COLUMN "{col}" {col_sql_type}"""
                    print(f"添加字段：{add_col_sql}")
                    self.execute(add_col_sql)

    def ensure_primary_key(self, table: str, schema: str, primary_keys: List[str]):
        if primary_keys:
            constraint = self.execute(
                "select constraint_name from information_schema.table_constraints "
                f"where constraint_type='PRIMARY KEY' AND TABLE_NAME = '{table}' AND TABLE_SCHEMA= '{schema}' ",
            )
            if len(constraint) == 0:
                sql = f"""ALTER TABLE "{schema}"."{table}"  ADD PRIMARY KEY ("{'","'.join(primary_keys)}")"""
                print(f"[PRIMARY KEY]执行sql：{sql}")
                self.execute(sql)

    def is_table_exists(self, table: str, schema: str) -> bool:
        sql = f"""select count(1) from information_schema.tables where table_schema='{schema}' 
        and table_type='BASE TABLE' and table_name='{table}' """
        cnt = self.execute(sql).iloc[0, 0]
        if cnt > 0:
            return True
        else:
            return False

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
            print(f"{schema}.{table} created")

        if primary_keys:
            self.ensure_primary_key(
                table=table, schema=schema, primary_keys=primary_keys
            )

    def truncate(self, table: str, schema: str):
        if self.is_table_exists(schema=schema, table=table):
            self.execute(f"truncate table {schema}.{table}")
            print(f"{schema}.{table} truncated")
