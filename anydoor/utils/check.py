import os
from functools import wraps
from typing import List, Union


class check(object):

    @classmethod
    def env(cls, envs: Union[str, List[str]]):
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






