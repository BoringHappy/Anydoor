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
    def __init__(self):
        self._locks = defaultdict(threading.Lock)
        self._lock = threading.Lock()  # 保护锁字典的锁

    def get_lock(self, key):
        with self._lock:
            return self._locks[key]

    def release_lock(self, key):
        """可选：在不再需要锁时释放"""
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
    """查询缓存并返回有效结果"""
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
    """更新缓存"""
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
