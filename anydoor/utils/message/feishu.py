# -*- coding:utf-8 -*-
"""
filename : feishu.py
createtime : 2021/6/20 21:46
author : Demon Finch
"""
import requests
from tenacity import retry, stop_after_attempt, wait_exponential
from .base import BaseMsg


class msgfs(BaseMsg):
    BASE_URL = "https://open.feishu.cn/open-apis/bot/v2/hook/"
    PASSWD_NAME_ENV = "FEISHU_PASSWD_NAME"

    @retry(
        reraise=True,
        stop=stop_after_attempt(7),
        wait=wait_exponential(multiplier=1, min=4, max=20),
    )
    def send(self, message: str, msgtype: str = "text", raise_exception=False):
        url = self.BASE_URL + self.secret.hook_id
        response = requests.post(
            url=self.url, json={"msg_type": msgtype, "content": {"text": message}}
        )
        if not response.ok and raise_exception:
            response.raise_for_status()
        else:
            return response
