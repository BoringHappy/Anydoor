import os
from .vault import Vault
from functools import lru_cache


@lru_cache
def get_proxy():
    return Vault().get("local_proxy" or os.environ["VAULT_PROXY_PATH"]).json()


def Proxy():
    return {k: v for k, v in get_proxy().items() if k in ["http", "https"]}
