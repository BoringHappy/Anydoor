from questdb.ingress import Sender
from functools import lru_cache
from ..utils.vault import Vault, Secret
from sqlalchemy import create_engine, text, Engine
import pandas as pd
from typing import Optional
from .base import BaseDB


class QuestDB(BaseDB):
    secret_name = "questdb"
    default_schema = ("qdb",)

    def __init__(self, *args, **kwagrs):
        super().__init__(database=self.default_schema, *args, **kwagrs)

    @classmethod
    def create_engine(cls, secret: Secret, database, schema, *args, **kwargs) -> Engine:
        """postgresql sqlalchemy engine"""
        engine = create_engine(
            f"postgresql://{secret.user}:{secret.password}@{secret.host}:{secret.pg_port}/{cls.default_schema}",
            client_encoding="utf8",
        )
        return engine

    @lru_cache
    def get_conf(self):
        secret = Vault().get(self.secret_name)
        conf = f"http::addr={secret.host}:{secret.port};username={secret.user};password={secret.password};"
        return conf

    def to_sql(self, df, table_name, at, *args, **kwargs):
        conf = self.get_conf()
        with Sender.from_conf(conf) as sender:
            sender.dataframe(df, table_name=table_name, at=at, *args, **kwargs)
            sender.flush()

    def execute(self, sql: str, return_pandas=True) -> Optional[pd.DataFrame]:
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
