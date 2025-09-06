"""
测试装饰器模块

提供基于环境变量的测试跳过装饰器
"""

import os
from functools import wraps
from typing import List, Optional


def skip_if_env_not_set(
    env_var: str, values: Optional[List[str]] = None, reason: Optional[str] = None
):
    """
    当环境变量不存在或值不在指定列表中时跳过测试

    Args:
        env_var: 环境变量名
        values: 允许的值列表，默认为 ["true", "1", "yes"]
        reason: 跳过原因，如果为None则自动生成

    Examples:
        @skip_if_env_not_set("ENABLE_CLICKHOUSE_TESTS")
        def test_clickhouse():
            pass

        @skip_if_env_not_set("DATABASE_URL", ["postgresql://", "mysql://"])
        def test_database():
            pass
    """
    if values is None:
        values = ["true", "1", "yes"]

    if reason is None:
        reason = f"Test skipped: {env_var} not set to {values}. Set {env_var}=true to enable."

    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            env_value = os.getenv(env_var, "").lower()
            if env_value not in values:
                import pytest

                pytest.skip(reason)
            return func(*args, **kwargs)

        return wrapper

    return decorator


def skip_if_env_set(
    env_var: str, values: Optional[List[str]] = None, reason: Optional[str] = None
):
    """
    当环境变量存在且值在指定列表中时跳过测试

    Args:
        env_var: 环境变量名
        values: 跳过的值列表，默认为 ["true", "1", "yes"]
        reason: 跳过原因，如果为None则自动生成

    Examples:
        @skip_if_env_set("SKIP_INTEGRATION_TESTS")
        def test_integration():
            pass

        @skip_if_env_set("CI", ["true", "1"])
        def test_local_only():
            pass
    """
    if values is None:
        values = ["true", "1", "yes"]

    if reason is None:
        reason = (
            f"Test skipped: {env_var} is set to {values}. Unset {env_var} to enable."
        )

    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            env_value = os.getenv(env_var, "").lower()
            if env_value in values:
                import pytest

                pytest.skip(reason)
            return func(*args, **kwargs)

        return wrapper

    return decorator


def skip_if_missing_env_vars(env_vars: List[str], reason: Optional[str] = None):
    """
    当任一必需的环境变量不存在时跳过测试

    Args:
        env_vars: 必需的环境变量列表
        reason: 跳过原因，如果为None则自动生成

    Examples:
        @skip_if_missing_env_vars(["DATABASE_URL", "REDIS_URL"])
        def test_with_dependencies():
            pass
    """
    if reason is None:
        missing_vars = [var for var in env_vars if not os.getenv(var)]
        reason = f"Test skipped: Missing required environment variables: {missing_vars}"

    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            missing_vars = [var for var in env_vars if not os.getenv(var)]
            if missing_vars:
                import pytest

                pytest.skip(reason)
            return func(*args, **kwargs)

        return wrapper

    return decorator


def skip_if_env_equals(env_var: str, value: str, reason: Optional[str] = None):
    """
    当环境变量等于指定值时跳过测试

    Args:
        env_var: 环境变量名
        value: 跳过的值
        reason: 跳过原因，如果为None则自动生成

    Examples:
        @skip_if_env_equals("NODE_ENV", "production")
        def test_dev_only():
            pass
    """
    if reason is None:
        reason = f"Test skipped: {env_var}={value}. Change {env_var} to run this test."

    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            if os.getenv(env_var) == value:
                import pytest

                pytest.skip(reason)
            return func(*args, **kwargs)

        return wrapper

    return decorator


# 通用装饰器工厂函数
def require_env(
    env_var: str, values: Optional[List[str]] = None, reason: Optional[str] = None
):
    """
    要求环境变量存在且值在指定列表中，否则跳过测试

    这是 skip_if_env_not_set 的别名，提供更直观的命名

    Args:
        env_var: 环境变量名
        values: 允许的值列表，默认为 ["true", "1", "yes"]
        reason: 跳过原因，如果为None则自动生成

    Examples:
        @require_env("CLICKHOUSE_ENABLED")
        def test_clickhouse():
            pass

        @require_env("DATABASE_TYPE", ["postgresql", "mysql"])
        def test_database():
            pass
    """
    return skip_if_env_not_set(env_var, values, reason)


def skip_when_env(
    env_var: str, values: Optional[List[str]] = None, reason: Optional[str] = None
):
    """
    当环境变量存在且值在指定列表中时跳过测试

    这是 skip_if_env_set 的别名，提供更直观的命名

    Args:
        env_var: 环境变量名
        values: 跳过的值列表，默认为 ["true", "1", "yes"]
        reason: 跳过原因，如果为None则自动生成

    Examples:
        @skip_when_env("SKIP_INTEGRATION_TESTS")
        def test_integration():
            pass

        @skip_when_env("CI")
        def test_local_only():
            pass
    """
    return skip_if_env_set(env_var, values, reason)


def require_all_env(env_vars: List[str], reason: Optional[str] = None):
    """
    要求所有环境变量都存在，否则跳过测试

    这是 skip_if_missing_env_vars 的别名，提供更直观的命名

    Args:
        env_vars: 必需的环境变量列表
        reason: 跳过原因，如果为None则自动生成

    Examples:
        @require_all_env(["DATABASE_URL", "REDIS_URL"])
        def test_with_dependencies():
            pass
    """
    return skip_if_missing_env_vars(env_vars, reason)


def skip_when_env_equals(env_var: str, value: str, reason: Optional[str] = None):
    """
    当环境变量等于指定值时跳过测试

    这是 skip_if_env_equals 的别名，提供更直观的命名

    Args:
        env_var: 环境变量名
        value: 跳过的值
        reason: 跳过原因，如果为None则自动生成

    Examples:
        @skip_when_env_equals("NODE_ENV", "production")
        def test_dev_only():
            pass
    """
    return skip_if_env_equals(env_var, value, reason)
