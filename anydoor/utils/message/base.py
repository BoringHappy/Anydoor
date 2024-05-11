import os
from .. import Vault, SingletonType, Secret


class UserError(Exception): ...


class BaseMsg(metaclass=SingletonType):
    PASSWD_NAME_ENV = None

    def __init__(self, secret_name: str = None, secret: Secret = None):
        if (
            secret is None
            and secret_name is None
            and os.environ.get("QYWX_PASSWD_NAME") is None
        ):
            raise ValueError(
                f"secret or secret_name or QYWX_PASSWD_NAME can be none in same time"
            )
        self.secret_instance = secret
        self.secret_name = secret_name

    @property
    def secret(self):
        return (
            self.secret_instance
            or Vault().get(self.secret_name, raise_exception=False)
            or Vault().get(os.environ[self.PASSWD_NAME_ENV])
        )

    def send(self, message: str, msgtype: str = "text", raise_exception=False): ...

    @classmethod
    def cls_send(cls, *args, **kwargs):
        return cls().send(*args, **kwargs)
