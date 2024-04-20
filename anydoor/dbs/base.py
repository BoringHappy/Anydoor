import pandas as pd
from datetime import datetime
from uuid import uuid1
from anydoor.utils import Secret
from sqlalchemy.types import DateTime, Float, String, Date, BIGINT
from sqlalchemy.sql import text
from typing import List, Optional, Union
from types import SimpleNamespace
from sqlalchemy import Engine


class BaseDB:
    default_schema = None

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
        self, secret: SimpleNamespace, database, schema, **kwargs
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

    def to_sql(
        self,
        df: pd.DataFrame,
        table: str,
        schema: str = "public",
        dtype: dict = None,
        on_conflict: str = "replace",
        primary_keys: List[str] = None,
    ): ...
