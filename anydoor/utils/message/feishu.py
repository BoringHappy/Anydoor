# -*- coding:utf-8 -*-
"""
filename : feishu.py
createtime : 2021/6/20 21:46
author : Demon Finch
"""
import requests
from tenacity import retry, stop_after_attempt, wait_exponential
from anydoor.utils import Secret


class msgfs:
    BASE_URL = "https://open.feishu.cn/open-apis/bot/v2/hook/"

    def __init__(self, hook_id: str = None, secret_name: str = None):
        if hook_id is None and secret_name is None:
            raise ValueError(f"hook_id and secret_name can be empty in same time")
        self.hook_id = hook_id or Secret.get(secret_name).hook_id
        self.url = self.BASE_URL + self.hook_id

    @retry(
        reraise=True,
        stop=stop_after_attempt(7),
        wait=wait_exponential(multiplier=1, min=4, max=20),
    )
    def send(self, message: str, msgtype: str = "text", raise_exception=False):
        response = requests.post(
            url=self.url, json={"msg_type": msgtype, "content": {"text": message}}
        )
        if not response.ok and raise_exception:
            response.raise_for_status()
        else:
            return response
