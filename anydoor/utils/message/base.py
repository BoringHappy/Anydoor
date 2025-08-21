import os
from ..vault import Vault, Secret
from ..singleton import SingletonType


class UserError(Exception): ...


class BaseMsg(metaclass=SingletonType):
    PASSWD_NAME_ENV = None
    secret_name: str = None

    def __init__(self, secret: Secret = None):
        self.secret_instance = secret

    @property
    def secret(self):
        return self.secret_instance or Vault().get(
            os.environ.get(self.PASSWD_NAME_ENV, self.secret_name)
        )

    def send(self, message: str, msgtype: str = "text", raise_exception=False): ...

    @classmethod
    def cls_send(cls, *args, **kwargs):
        return cls().send(*args, **kwargs)
