from anydoor.utils.message import msgqywx, msgfs
import pytest


@pytest.mark.skip(reason="Call WX API")
def test_qywx():
    ret = msgqywx.cls_send("你好")
    assert ret.ok


@pytest.mark.skip(reason="Call WX API")
def test_feishu():
    ret = msgfs.cls_send("你好")
    assert ret.ok


if __name__ == "__main__":
    test_qywx()
