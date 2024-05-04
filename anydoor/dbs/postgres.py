# -*- coding:utf-8 -*-
"""
filename : postgres.py
create_time : 2021/12/29 19:30
author : Demon Finch
"""

from sqlalchemy import MetaData, create_engine, Table, Engine
from sqlalchemy.dialects.postgresql import insert
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
        raise ValueError(f"on_conflict :{on_conflict}")


class Postgres(BaseDB):
    DB_TYPE = "postgres"
    default_schema = "public"

    @classmethod
    def create_engine(
        cls, secret: SimpleNamespace, database, schema, *args, **kwargs
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

    def get_conflict_func(self, on_conflict):
        if on_conflict:
            return on_conflict_do(on_conflict)
