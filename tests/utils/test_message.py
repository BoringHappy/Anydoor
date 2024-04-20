from anydoor.utils.message import msgqywx
import pytest


@pytest.mark.skip(reason="Call WX API")
def test_qywx():
    ret = msgqywx("wechat").send("你好")
    assert ret.ok
