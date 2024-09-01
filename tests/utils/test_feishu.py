from anydoor.utils.message.feishu import msgfs
import pytest


@pytest.mark.skip(reason="Call API")
def test_feishu_text():
    msgfs().send_text("test")


@pytest.mark.skip(reason="Call API")
def test_feishu_card():
    msgfs().send(
        message_dict={
            "msg_type": "interactive",
            "card": {
                "elements": [
                    {
                        "tag": "div",
                        "text": {
                            "content": "**Exception**",
                            "tag": "lark_md",
                        },
                    },
                    {
                        "actions": [
                            {
                                "tag": "button",
                                "text": {
                                    "content": "LINK",
                                    "tag": "lark_md",
                                },
                                "url": "https://www.example.com",
                                "type": "default",
                                "value": {},
                            }
                        ],
                        "tag": "action",
                    },
                ],
                "header": {"title": {"content": "raise_eeee", "tag": "plain_text"}},
            },
        }
    )


@pytest.mark.skip(reason="Call API")
def test_feishu_format_text():
    msgfs().send(
        message_dict={
            "msg_type": "post",
            "content": {
                "msg_type": "interactive",
                "card": {
                    "elements": [
                        {
                            "tag": "div",
                            "text": {
                                "content": "**Exception**",
                                "tag": "lark_md",
                            },
                        },
                        {
                            "actions": [
                                {
                                    "tag": "button",
                                    "text": {"content": "LINK", "tag": "lark_md"},
                                    "url": "http://192.168.5.244:31257/dags/raise_eeee/grid",
                                    "type": "default",
                                    "value": {},
                                }
                            ],
                            "tag": "action",
                        },
                    ],
                    "header": {
                        "title": {
                            "content": "今日旅游推荐",
                            "tag": "plain_text",
                        }
                    },
                },
            },
        }
    )
