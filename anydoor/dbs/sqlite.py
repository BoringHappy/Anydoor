# -*- coding:utf-8 -*-
"""
filename : sqlite.py
create_time : 2024/04/20 19:30
author : Demon Finch
"""

from sqlalchemy import Engine
from .base import BaseDB
from sqlalchemy import create_engine


class Sqlite(BaseDB):
    DB_TYPE = "sqlite"
    default_schema = "main"

    def __init__(
        self,
        db_path: str,
        schema: str = None,
        engine: Engine = None,
        create_engine_options=dict(),
    ):
        self.schema = schema or self.default_schema
        self.engine = engine or self.create_engine(
            db_path=db_path,
            **create_engine_options,
        )

    @classmethod
    def create_engine(cls, db_path, **kwargs) -> Engine:
        engine = create_engine(
            f"sqlite:///{db_path}",
            **kwargs,
        )
        return engine

    def ensure_primary_key(self, *args, **kwargs):
        raise AttributeError("No primary key change allowed in sqlite")

    def change_column(self, *args, **kwargs):
        raise AttributeError("No column change allowed in sqlite")

    def truncate(self, *args, **kwargs):
        raise AttributeError("No truncate allowed in sqlite")
