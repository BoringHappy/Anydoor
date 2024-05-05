# -*- coding:utf-8 -*-
"""
filename : qywx.py
createtime : 2024/4/20 21:46
author : Demon Finch
"""
import os
import json
import requests
from datetime import datetime
from tenacity import retry, stop_after_attempt, wait_exponential
from .. import Secret
from requests import Response
from .base import BaseMsg
from anydoor.utils import logger


class msgqywx(BaseMsg):
    url = "https://qyapi.weixin.qq.com/cgi-bin/gettoken"
    sec_temp_name = "qywx_access_token"
    PASSWD_NAME_ENV = "QYWX_PASSWD_NAME"

    @retry(
        reraise=True,
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=20),
    )
    def get_access_token_from_wx(self):
        response = requests.post(
            self.url,
            params={
                "corpid": self.secret.corp_id,
                "corpsecret": self.secret.corp_secret,
            },
        )
        if response.ok:
            data = json.loads(response.text)
            secret_json = {
                "expire_time": datetime.now().timestamp() + 3600,
                "access_token": data["access_token"],
            }
            Secret.add(self.sec_temp_name, secret_json)
            return secret_json["access_token"]
        else:
            response.raise_for_status()

    def get_access_token(self):
        access_json = Secret.get(self.sec_temp_name, raise_exception=False)
        if access_json:
            if access_json.expire_time > datetime.now().timestamp():
                return access_json.access_token
        return self.get_access_token_from_wx()

    @retry(
        reraise=True,
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=20),
    )
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

        send_url = f"https://qyapi.weixin.qq.com/cgi-bin/message/send?access_token={self.get_access_token()}"

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
