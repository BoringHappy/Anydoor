# -*- coding:utf-8 -*-
"""
PostgreSQL database connection and operations
"""

from functools import partial

from sqlalchemy import Engine, MetaData, Table, create_engine
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.sql import text

from ..utils.vault import Secret
from .base import BaseDB


def pgsql_upsert(table, conn, keys, data_iter, on_conflict):
    """
    PostgreSQL-specific upsert implementation using ON CONFLICT.

    Handles upsert operations for PostgreSQL by:
    1. Detecting the primary key constraint name
    2. Using PostgreSQL's ON CONFLICT DO UPDATE/NOTHING syntax
    3. Processing data in batches for efficiency

    Args:
        table: SQLAlchemy Table object
        conn: Database connection
        keys: Column names
        data_iter: Iterator of data rows
        on_conflict (str): Conflict resolution strategy ("replace" or "ignore")
    """
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
    """
    Create conflict resolution function for PostgreSQL.

    Args:
        on_conflict (str): Conflict resolution strategy ("replace" or "ignore")

    Returns:
        callable: Partial function configured for the specified conflict strategy

    Raises:
        ValueError: If on_conflict strategy is not supported
    """
    if on_conflict in ("replace", "ignore"):
        return partial(pgsql_upsert, on_conflict=on_conflict)
    else:
        raise ValueError(f"on_conflict :{on_conflict}")


class Postgres(BaseDB):
    """
    PostgreSQL database implementation with full CRUD operations.

    Provides PostgreSQL-specific functionality including:
    - Automatic upsert operations using ON CONFLICT
    - Schema management with proper PostgreSQL types
    - Primary key constraint handling
    - Column type validation and adjustment

    Attributes:
        DB_TYPE (str): Set to "postgres"
        default_schema (str): Set to "public"
        default_secret_name (str): Set to "postgres"
    """

    DB_TYPE = "postgres"
    default_schema = "public"
    default_secret_name = "postgres"

    @classmethod
    def create_engine(cls, secret: Secret, database, schema, *args, **kwargs) -> Engine:
        """
        Create PostgreSQL SQLAlchemy engine.

        Args:
            secret (Secret): Database credentials containing host, port, user, password
            database (str): Database name
            schema (str): Schema name (used to set search_path)
            *args: Additional positional arguments
            **kwargs: Additional engine options

        Returns:
            Engine: Configured SQLAlchemy engine for PostgreSQL

        Note:
            Automatically sets search_path if schema is provided
        """
        if schema:
            kwargs["connect_args"] = {"options": f"-csearch_path={schema}"}
        engine = create_engine(
            f"postgresql://{secret.user}:{secret.password}@{secret.host}:{secret.port}/{database}",
            client_encoding="utf8",
            **kwargs,
        )
        return engine

    def get_conflict_func(self, on_conflict):
        """
        Get conflict resolution function for PostgreSQL.

        Args:
            on_conflict (str): Conflict resolution strategy

        Returns:
            callable: Configured conflict resolution function
        """
        if on_conflict:
            return on_conflict_do(on_conflict)
