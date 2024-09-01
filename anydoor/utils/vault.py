import hvac
import os
from typing import Dict
from .singleton import SingletonType
from types import SimpleNamespace
from copy import deepcopy


class Secret(SimpleNamespace):
    def __getattr__(self, name: str):
        if name in self.__dict__:
            return getattr(self, name)
        else:
            return None

    def __bool__(self):
        return bool(vars(self))

    def json(self, **kwargs):
        json_str = deepcopy(vars(self))
        json_str.update(kwargs)
        return json_str


class Vault(metaclass=SingletonType):
    """
    VAULT_ADDR
    VAULT_TOKEN
    """

    def __init__(self, url=None, token=None):
        self.client = hvac.Client(url=url, token=token)
        if self.client.sys.is_sealed():
            self.client.sys.submit_unseal_key(key=os.getenv("VAULT_UNSEAL_KEY"))
            assert not self.client.sys.is_sealed()
            
        assert self.client.is_authenticated()
        assert self.client.sys.is_initialized()

    @staticmethod
    def get_mount_point(mount_point):
        return mount_point or os.getenv("VAULT_DEFAULT_MOUNT_POINT", "secret")

    def add(
        self,
        path: str,
        secret: Dict[str, str],
        mount_point: str = None,
    ):
        self.client.secrets.kv.v2.create_or_update_secret(
            path=path, secret=secret, mount_point=self.get_mount_point(mount_point)
        )

    def get(self, path: str, mount_point: str = None, raise_exception=True) -> Secret:
        def raise_exceptions():
            if raise_exception:
                raise Exception(f"Non exists {path}")
            else:
                return Secret()

        if not path:
            raise_exceptions()
        try:
            return Secret(
                **self.client.secrets.kv.read_secret(
                    path=path,
                    mount_point=self.get_mount_point(mount_point),
                    raise_on_deleted_version=False,
                )["data"]["data"]
            )
        except:
            raise_exceptions()

    def delete(self, path: str, mount_point=None):
        return self.client.secrets.kv.delete_metadata_and_all_versions(
            path=path,
            mount_point=self.get_mount_point(mount_point),
        )
