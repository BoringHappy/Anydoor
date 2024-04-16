# -*- coding:utf-8 -*-
"""
filename : keepass.py
create_time : 2024/04/16 19:33
author : Demon Finch
"""
import os
import json
from types import SimpleNamespace
from functools import lru_cache
from typing import Dict
from cryptography.fernet import Fernet
from .check import check
from pykeepass import PyKeePass


class KeePass:

    def __init__(self, path, password, group="default") -> None:
        path = path or os.environ["KEEPASS_PATH"]
        password = password or os.environ["KEEPASS_PASSWD"]
        if not os.path.exists(path):
            raise FileNotFoundError(path)
        self.kp = PyKeePass(path, password)
        self.group = group

    def add(self): ...
