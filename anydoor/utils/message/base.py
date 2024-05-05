import os
from types import SimpleNamespace
from .. import Secret, SingletonType


class UserError(Exception): ...


class BaseMsg(metaclass=SingletonType):
    PASSWD_NAME_ENV = None

    def __init__(self, secret_name: str = None, secret: SimpleNamespace = None):
        if (
            secret is None
            and secret_name is None
            and os.environ.get("QYWX_PASSWD_NAME") is None
        ):
            raise ValueError(
                f"secret or secret_name or QYWX_PASSWD_NAME can be none in same time"
            )
        self.secret = (
            secret
            or Secret.get(secret_name, raise_exception=False)
            or Secret.get(os.environ[self.PASSWD_NAME_ENV])
        )

    def send(self, message: str, msgtype: str = "text", raise_exception=False): ...

    @classmethod
    def cls_send(cls, *args, **kwargs):
        return cls().send(*args, **kwargs)
