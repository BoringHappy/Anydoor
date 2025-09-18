"""
Database-backed caching decorator with thread safety.

This module provides a decorator for caching function results in a database
with automatic expiration, thread safety, and conflict resolution.
"""

import json
import threading
from collections import defaultdict
from datetime import datetime, timedelta
from functools import wraps
from typing import Any, Callable, Optional

from sqlalchemy import Engine, inspect, select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.schema import Column, MetaData, Table
from sqlalchemy.types import TEXT, DateTime, String


# 线程安全的锁管理器
class LockManager:
    """
    Thread-safe lock manager for managing per-key locks.

    Provides a centralized way to manage locks for different cache keys,
    ensuring thread safety in multi-threaded environments.
    """

    def __init__(self):
        """Initialize the lock manager with empty lock dictionary."""
        self._locks = defaultdict(threading.Lock)
        self._lock = threading.Lock()  # 保护锁字典的锁

    def get_lock(self, key):
        """
        Get a lock for the specified key.

        Args:
            key: The key to get a lock for

        Returns:
            threading.Lock: A lock object for the specified key
        """
        with self._lock:
            return self._locks[key]

    def release_lock(self, key):
        """
        Release a lock for the specified key (optional cleanup).

        Args:
            key: The key to release the lock for
        """
        with self._lock:
            if key in self._locks:
                del self._locks[key]


# 全局锁管理器实例
lock_manager = LockManager()


def cache_db(
    engine: Engine,
    schema: str,
    table: str,
    expire_duration: timedelta = timedelta(days=1),
) -> Callable:
    """
    Decorator for caching function results in a database table.

    This decorator provides database-backed caching with the following features:
    - Automatic table creation if it doesn't exist
    - Thread-safe cache access using per-key locks
    - Automatic cache expiration based on record time
    - Conflict resolution using PostgreSQL's ON CONFLICT DO UPDATE

    Args:
        engine (Engine): SQLAlchemy database engine
        schema (str): Database schema name for the cache table
        table (str): Table name for storing cache entries
        expire_duration (timedelta): How long cache entries remain valid (default: 1 day)

    Returns:
        Callable: Decorator function that can be applied to other functions

    Cache Table Schema:
        The cache table has the following columns:
        - func_name (String): Name of the cached function (primary key)
        - args (String): JSON-serialized function arguments (primary key)
        - kwargs (String): JSON-serialized function keyword arguments (primary key)
        - result (TEXT): JSON-serialized function result
        - record_time (DateTime): When the cache entry was created

    Example:
        >>> @cache_db(engine, "cache_schema", "function_cache", timedelta(hours=6))
        ... def expensive_function(x, y):
        ...     return x + y
        >>> result = expensive_function(1, 2)  # Executes function and caches result
        >>> result = expensive_function(1, 2)  # Returns cached result
    """
    metadata = MetaData(schema=schema)
    _table = Table(
        table,
        metadata,
        Column("func_name", String(), primary_key=True),
        Column("args", String(), primary_key=True),
        Column("kwargs", String(), primary_key=True),
        Column("result", TEXT()),
        Column("record_time", DateTime()),
    )

    # 使用线程安全的表创建标志
    table_created = False
    table_creation_lock = threading.Lock()

    def ensure_table_exists():
        nonlocal table_created
        if table_created:
            return

        with table_creation_lock:
            if table_created:
                return

            # 双重检查锁定模式
            with engine.connect() as conn:
                if not inspect(conn).has_table(table, schema=schema):
                    with engine.begin() as trans_conn:
                        _table.create(trans_conn)
            table_created = True

    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            # 确保表存在
            ensure_table_exists()

            # 序列化参数作为缓存键（排序键保证一致性）
            str_args = json.dumps(args, sort_keys=True)
            str_kwargs = json.dumps(kwargs, sort_keys=True)
            cache_key = (func.__name__, str_args, str_kwargs)

            # 获取当前函数+参数的专用锁
            lock = lock_manager.get_lock(cache_key)

            with lock:  # 进程内线程安全
                # 检查缓存是否有效
                cached_result = _get_cached_result(
                    engine, _table, func.__name__, str_args, str_kwargs, expire_duration
                )
                if cached_result is not None:
                    return cached_result

                # 执行函数获取结果
                result = func(*args, **kwargs)
                _update_cache(
                    engine, _table, func.__name__, str_args, str_kwargs, result
                )
                return result

        return wrapper

    return decorator


def _get_cached_result(
    engine: Engine,
    table: Table,
    func_name: str,
    str_args: str,
    str_kwargs: str,
    expire_duration: timedelta,
) -> Optional[Any]:
    """
    Query cache and return valid cached result if available.

    Args:
        engine (Engine): SQLAlchemy database engine
        table (Table): Cache table object
        func_name (str): Name of the function
        str_args (str): JSON-serialized function arguments
        str_kwargs (str): JSON-serialized function keyword arguments
        expire_duration (timedelta): Cache expiration duration

    Returns:
        Optional[Any]: Cached result if valid and not expired, None otherwise
    """
    with engine.connect() as conn:
        row = conn.execute(
            select(table.c.result, table.c.record_time)
            .where(table.c.func_name == func_name)
            .where(table.c.args == str_args)
            .where(table.c.kwargs == str_kwargs)
        ).first()

        if row and row.record_time > datetime.now() - expire_duration:
            return json.loads(row.result)
    return None


def _update_cache(
    engine: Engine,
    table: Table,
    func_name: str,
    str_args: str,
    str_kwargs: str,
    result: Any,
) -> None:
    """
    Update cache with new function result.

    Uses PostgreSQL's ON CONFLICT DO UPDATE to handle concurrent writes
    and ensure cache consistency.

    Args:
        engine (Engine): SQLAlchemy database engine
        table (Table): Cache table object
        func_name (str): Name of the function
        str_args (str): JSON-serialized function arguments
        str_kwargs (str): JSON-serialized function keyword arguments
        result (Any): Function result to cache
    """
    str_result = json.dumps(result)
    record_time = datetime.now()

    with engine.begin() as conn:  # 自动提交事务
        conn.execute(
            insert(table)
            .values(
                func_name=func_name,
                args=str_args,
                kwargs=str_kwargs,
                result=str_result,
                record_time=record_time,
            )
            .on_conflict_do_update(
                index_elements=["func_name", "args", "kwargs"],
                set_=dict(result=str_result, record_time=record_time),
            )
        )
