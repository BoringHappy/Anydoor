# -*- coding:utf-8 -*-
"""
filename : qywx.py
createtime : 2024/4/20 21:46
author : Demon Finch
"""
import os
import json
import requests
from hashlib import md5
from datetime import datetime
from tenacity import retry, stop_after_attempt, wait_exponential
from anydoor.utils import Secret
from anydoor.utils.singleton import SingletonType
from requests import Response


class UserError(Exception): ...


class msgqywx(metaclass=SingletonType):
    url = "https://qyapi.weixin.qq.com/cgi-bin/gettoken"
    sec_temp_name = "qywx_access_token"

    def __init__(self, secret_name: str):
        secret = Secret.get(secret_name)
        self.corpid = secret.corp_id
        self.corpsecret = secret.corp_secret
        self.agentid = secret.agent_id
        self.touser = secret.user
        self.secret_key_md5 = md5(
            (self.corpid + self.corpsecret).encode(encoding="utf-8")
        ).hexdigest()
        self.base_config_folder = f"{os.path.expanduser('~')}/.config/msgqywx"
        if not os.path.exists(self.base_config_folder):
            os.makedirs(self.base_config_folder)
        self.access_conf_file = os.path.join(
            self.base_config_folder, f"{self.secret_key_md5}.conf"
        )

    def access_token_to_jsonfile(self):
        response = requests.post(
            self.url,
            params={
                "corpid": self.corpid,
                "corpsecret": self.corpsecret,
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
            if access_json.get("expire_time", 0) > datetime.now().timestamp():
                return access_json["access_token"]
        return self.access_token_to_jsonfile()

    @retry(
        reraise=True,
        stop=stop_after_attempt(7),
        wait=wait_exponential(multiplier=1, min=4, max=20),
    )
    def send(
        self,
        message,
        msgtype: str = "text",
        raise_error: bool = False,
    ) -> Response:
        """
        send_msg发送文本类消息
        :param msgtype: 消息类型，仅支持 text 和 markdown
        :param raise_error: 是否抛出发送错误(response不等于200的情况)，默认为False
        :param message: 消息内容，当前仅支持文本内容
        :param touser: 发送用户，和初始化类时的touser不能同时为None
        :return: 微信返回的response，可以自行处理错误信息，也可不处理
        """
        if msgtype not in ["text", "markdown"]:
            raise TypeError(
                "Unsupported msgtype, only `text` and `markdown` are acceptable"
            )

        send_url = f"https://qyapi.weixin.qq.com/cgi-bin/message/send?access_token={self.get_access_token()}"

        payload = {
            "touser": self.touser,
            "agentid": self.agentid,
            "msgtype": msgtype,
            msgtype: {"content": message},
        }

        response = requests.post(send_url, json=payload)
        if not response.ok and raise_error:
            response.raise_for_status()
        else:
            return response
