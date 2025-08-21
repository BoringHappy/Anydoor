# -*- coding:utf-8 -*-
"""
Feishu (Lark) message sending utility
"""

import base64
import hashlib
import hmac
from datetime import datetime
from typing import Union

import requests
from loguru import logger

from .base import BaseMsg


def gen_sign(timestamp, secret):
    # 拼接timestamp和secret
    string_to_sign = "{}\n{}".format(timestamp, secret)
    hmac_code = hmac.new(
        string_to_sign.encode("utf-8"), digestmod=hashlib.sha256
    ).digest()

    # 对结果进行base64处理
    sign = base64.b64encode(hmac_code).decode("utf-8")

    return sign


class msgfs(BaseMsg):
    BASE_URL = "https://open.feishu.cn/open-apis/bot/v2/hook/"
    PASSWD_NAME_ENV = "FEISHU_PASSWD_NAME"
    secret_name = "feishu"

    def send(self, message_dict: Union[str, dict]):
        current_timestamp = int(datetime.now().timestamp())
        message_dict.update(
            {
                "timestamp": current_timestamp,
                "sign": gen_sign(current_timestamp, self.secret.sign),
            }
        )
        response = requests.post(
            url=self.secret.url,
            json=message_dict,
        )
        logger.info(response.text)
        if not response.ok:
            response.raise_for_status()
        else:
            return response

    def send_text(self, text):
        self.send({"msg_type": "text", "content": {"text": text}})
        self.send({"msg_type": "text", "content": {"text": text}})
