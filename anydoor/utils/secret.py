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
from .log import logger


class SingleSecret(SimpleNamespace): ...


class Secret:

    @staticmethod
    def folder():
        return os.environ["SECRETS_FOLDER"]

    @classmethod
    def get_secret_path(cls, secret_name: str):
        return os.path.join(cls.folder(), f"{secret_name}.passwd")

    @classmethod
    def encrypt(cls, secret_value: Dict[str, str]):
        return cls.fernet().encrypt(json.dumps(secret_value).encode("utf-8"))

    @classmethod
    def decrypt(cls, secret_value: str):
        return json.loads(cls.fernet().decrypt(secret_value).decode("utf-8"))

    @classmethod
    @lru_cache
    def list(cls):
        for file in os.listdir(cls.folder()):
            if file.endswith(".passwd"):
                logger.info(f"{file}")

    @classmethod
    @lru_cache
    def get(cls, secret_name: str, raise_exception=True):
        passwd_path = cls.get_secret_path(secret_name)
        if os.path.exists(passwd_path):
            with open(passwd_path, "rb") as f:
                return SingleSecret(**cls.decrypt(f.read()))
        else:
            if raise_exception:
                raise FileNotFoundError(
                    f"Secret {secret_name} not found in {cls.folder()}"
                )
            else:
                return

    @classmethod
    @lru_cache
    def delete(cls, secret_name: str):
        passwd_path = cls.get_secret_path(secret_name)
        if os.path.exists(passwd_path):
            os.remove(passwd_path)
        else:
            raise FileNotFoundError(passwd_path)

    @classmethod
    def add(
        cls,
        secret_name: str,
        secret_value: Dict[str, str] = None,
        secret_path: str = None,
    ):
        print(secret_name, secret_path)
        if not os.path.exists(cls.folder()):
            os.makedirs(cls.folder())
        if not secret_name:
            raise ValueError(secret_name)
        if secret_value and isinstance(secret_value, str):
            secret_value = json.loads(secret_value)

        if secret_path and isinstance(secret_path, str):
            with open(secret_path, "r") as sf:
                secret_value = json.load(sf)

        passwd_path = cls.get_secret_path(secret_name)
        with open(passwd_path, "wb") as f:
            f.write(cls.encrypt(secret_value))

    @staticmethod
    def fernet():
        return Fernet(os.environ["FERNET_KEY"])

    @staticmethod
    def generate():
        return Fernet.generate_key().decode()
