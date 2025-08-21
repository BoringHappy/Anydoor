# -*- coding:utf-8 -*-
"""
SQLite database connection and operations
"""

from sqlalchemy import Engine, create_engine

from .base import BaseDB


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
