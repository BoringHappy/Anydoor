"""
Environment variable validation utilities.

This module provides utilities for validating that required environment
variables are present before executing functions.
"""

import os
from functools import wraps
from typing import List, Union


class check(object):
    """
    Utility class for environment variable validation.

    Provides decorators and methods to ensure required environment
    variables are present before function execution.
    """

    @classmethod
    def env(cls, envs: Union[str, List[str]]):
        """
        Decorator to validate environment variables before function execution.

        Args:
            envs (Union[str, List[str]]): Environment variable name(s) to check

        Returns:
            Callable: Decorator function

        Raises:
            EnvironmentError: If any required environment variables are missing

        Example:
            >>> @check.env(['DATABASE_URL', 'API_KEY'])
            ... def my_function():
            ...     return "Function executed"
            >>> # Will raise EnvironmentError if DATABASE_URL or API_KEY are missing
        """
        if isinstance(envs, str):
            envs = [envs]
        missing_envs = [env for env in envs if env not in os.environ]
        if missing_envs:
            raise EnvironmentError(
                f"Missing environment variable(s): {', '.join(missing_envs)}"
            )

        def decorator(func):
            @wraps(func)
            def wrapper(*args, **kwargs):
                return func(*args, **kwargs)

            return wrapper

        return decorator
