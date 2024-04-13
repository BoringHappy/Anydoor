# -*- coding:utf-8 -*-
"""
filename : secret.py
create_time : 2023/04/16 19:33
author : Demon Finch
"""
import os
import json
from types import SimpleNamespace
from functools import lru_cache
from typing import Dict
from cryptography.fernet import Fernet


class Secret:
    folder = os.environ["SECRETS_FOLDER"]
    fernet_key = os.environ["FERNET_KEY"]

    @classmethod
    def get_secret_path(cls, secret_name):
        return os.path.join(cls.folder, f"{secret_name}.passwd")

    @classmethod
    def encrypt(cls, secret_value: Dict[str, str]):
        return cls.fernet.encrypt(json.dumps(secret_value).encode("utf-8"))

    @classmethod
    def decrypt(cls, secret_value: str):
        return json.loads(cls.fernet.decrypt(secret_value).decode("utf-8"))

    @classmethod
    @lru_cache
    def get(cls, secret_name):
        passwd_path = cls.get_secret_path(secret_name)
        if os.path.exists(passwd_path):
            with open(passwd_path, "rb") as f:
                return SimpleNamespace(**cls.decrypt(f.read()))
        else:
            raise FileNotFoundError(f"Secret {secret_name} not found in {cls.folder}")

    @classmethod
    def add(cls, secret_name, secret_value: Dict[str, str]):
        if not os.path.exists(cls.folder):
            os.makedirs(cls.folder)

        passwd_path = cls.get_secret_path(secret_name)
        with open(passwd_path, "wb") as f:
            f.write(cls.encrypt(secret_value))

    @classmethod
    @property
    @lru_cache
    def fernet(cls):
        with open(cls.fernet_key, "rb") as f:
            return Fernet(f.read())

    @classmethod
    def generate(cls):
        with open(cls.fernet_key, "wb") as f:
            f.write(Fernet.generate_key())


if __name__ == "__main__":
    test_secret = {
        "api_key": "12345",
        "api_secret": "12345",
    }
    Secret.generate()
    Secret.add("test_sec", test_secret)
    result = Secret.get("test_sec")
    print(result)
    assert result.__dict__ == SimpleNamespace(**test_secret).__dict__
