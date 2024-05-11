from anydoor.utils.message import msgqywx, msgfs
import pytest
from datetime import datetime

@pytest.mark.skip(reason="Call API")
def test_qywx():
    ret = msgqywx.cls_send(f"你好\n{datetime.now()}")
    assert ret.ok


@pytest.mark.skip(reason="Call API")
def test_feishu():
    ret = msgfs.cls_send(f"你好\n{datetime.now()}")
    assert ret.ok


if __name__ == "__main__":
    test_qywx()
