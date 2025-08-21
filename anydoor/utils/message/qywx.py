# -*- coding:utf-8 -*-
"""
Enterprise WeChat message sending utility
"""

import json
from datetime import datetime

import requests
from requests import Response
from tenacity import retry, stop_after_attempt, wait_exponential

from ...utils import logger
from .base import BaseMsg


class msgqywx(BaseMsg):
    url = "https://qyapi.weixin.qq.com/cgi-bin/gettoken"
    PASSWD_NAME_ENV = "QYWX_PASSWD_NAME"
    secret_name = "qywx"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.secret_json = dict()

    @retry(
        reraise=True,
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=20),
    )
    def get_access_token_from_wx(self, raise_exception):
        response = requests.post(
            self.url,
            params={
                "corpid": self.secret.corp_id,
                "corpsecret": self.secret.corp_secret,
            },
        )
        if response.ok:
            data = json.loads(response.text)
            self.secret_json = {
                "expire_time": datetime.now().timestamp() + 3600,
                "access_token": data["access_token"],
            }

            return self.secret_json["access_token"]
        else:
            if raise_exception:
                response.raise_for_status()
            else:
                return ""

    def get_access_token(self, raise_exception):
        if self.secret_json:
            if self.access_json["expire_time"] > datetime.now().timestamp():
                return self.access_json.access_token
        return self.get_access_token_from_wx(raise_exception)

    @retry(reraise=True, stop=stop_after_attempt(3))
    def send(
        self, message: str, msgtype: str = "text", raise_exception=False
    ) -> Response:
        """
        send_msg发送文本类消息
        :param msgtype: 消息类型，仅支持 text 和 markdown
        :param raise_error: 是否抛出发送错误(response不等于200的情况)，默认为False
        :param message: 消息内容，当前仅支持文本内容
        :param touser: 发送用户，和初始化类时的touser不能同时为None
        :return: 微信返回的response，可以自行处理错误信息，也可不处理
        """
        assert msgtype in ["text", "markdown"], TypeError()
        access_token = self.get_access_token(raise_exception=raise_exception)
        if not access_token:
            logger.warning("Message send fail")
            return

        send_url = f"https://qyapi.weixin.qq.com/cgi-bin/message/send?access_token={access_token}"

        payload = {
            "touser": self.secret.user,
            "agentid": self.secret.agent_id,
            "msgtype": msgtype,
            msgtype: {"content": message},
        }

        response = requests.post(send_url, json=payload)
        if response.ok:
            return response
        else:
            if raise_exception:
                response.raise_for_status()
            else:
                logger.exception(f"{__class__}{response.status_code}")
